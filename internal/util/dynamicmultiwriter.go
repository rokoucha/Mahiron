package util

import (
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
)

const dynamicMultiWriterBufferSize = 128

type DynamicMultiWriter struct {
	mutex       sync.RWMutex
	pool        sync.Pool
	subscribers []*dynamicMultiWriterSubscriber
}

type dynamicMultiWriterSubscriber struct {
	writer       io.Writer
	ch           chan *dynamicMultiWriterChunk
	done         chan struct{}
	lossless     bool
	onDrop       func(int)
	onQueueDepth func(int64)
	active       sync.WaitGroup
	once         sync.Once
}

type dynamicMultiWriterChunk struct {
	refs int32
	data []byte
	pool *sync.Pool
}

func NewDynamicMultiWriter(writers ...io.Writer) *DynamicMultiWriter {
	d := &DynamicMultiWriter{}
	for _, writer := range writers {
		d.Attach(writer)
	}
	return d
}

func IsExpectedStreamCloseError(err error) bool {
	return errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed)
}

func (d *DynamicMultiWriter) Attach(writer io.Writer) {
	d.attach(writer, DynamicMultiWriterSubscriberOptions{})
}

// DynamicMultiWriterSubscriberOptions controls delivery and observability for
// one attached writer.
type DynamicMultiWriterSubscriberOptions struct {
	Lossless     bool
	OnDrop       func(bytes int)
	OnQueueDepth func(delta int64)
}

// AttachWithOptions attaches a writer with an explicit delivery policy.
// Lossless writers apply backpressure to Write instead of queueing and dropping
// old chunks.
func (d *DynamicMultiWriter) AttachWithOptions(writer io.Writer, options DynamicMultiWriterSubscriberOptions) {
	d.attach(writer, options)
}

func (d *DynamicMultiWriter) attach(writer io.Writer, options DynamicMultiWriterSubscriberOptions) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	sub := &dynamicMultiWriterSubscriber{
		writer:       writer,
		done:         make(chan struct{}),
		lossless:     options.Lossless,
		onDrop:       options.OnDrop,
		onQueueDepth: options.OnQueueDepth,
	}
	if !sub.lossless {
		sub.ch = make(chan *dynamicMultiWriterChunk, dynamicMultiWriterBufferSize)
	}
	d.subscribers = append(d.subscribers, sub)
	if sub.lossless {
		close(sub.done)
		return
	}
	go sub.run(func() {
		d.detachSubscriber(sub, false)
	})
}

func (d *DynamicMultiWriter) Detach(writer io.Writer) {
	d.mutex.Lock()
	var sub *dynamicMultiWriterSubscriber
	for _, candidate := range d.subscribers {
		if candidate.writer == writer {
			sub = candidate
			break
		}
	}
	d.mutex.Unlock()

	if sub != nil {
		d.detachSubscriber(sub, true)
	}
}

func (d *DynamicMultiWriter) detachSubscriber(sub *dynamicMultiWriterSubscriber, wait bool) {
	d.mutex.Lock()
	found := false
	for i, candidate := range d.subscribers {
		if candidate == sub {
			d.subscribers = slices.Delete(d.subscribers, i, i+1)
			found = true
			break
		}
	}
	d.mutex.Unlock()

	if found {
		if sub.lossless {
			if closer, ok := sub.writer.(io.Closer); ok {
				_ = closer.Close()
			}
		}
		sub.active.Wait()
		sub.close()
	}
	if wait {
		<-sub.done
	}
}

func (d *DynamicMultiWriter) Count() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.subscribers)
}

func (d *DynamicMultiWriter) Close() {
	d.mutex.Lock()
	subscribers := d.subscribers
	d.subscribers = nil
	d.mutex.Unlock()

	for _, sub := range subscribers {
		if c, ok := sub.writer.(io.Closer); ok {
			_ = c.Close()
		}
	}
	for _, sub := range subscribers {
		sub.active.Wait()
		sub.close()
	}
}

func (d *DynamicMultiWriter) Write(p []byte) (n int, err error) {
	d.mutex.RLock()
	if len(d.subscribers) == 0 {
		d.mutex.RUnlock()
		return 0, io.ErrClosedPipe
	}
	subscribers := append([]*dynamicMultiWriterSubscriber(nil), d.subscribers...)
	for _, sub := range subscribers {
		sub.active.Add(1)
	}
	d.mutex.RUnlock()
	defer func() {
		for _, sub := range subscribers {
			sub.active.Done()
		}
	}()

	asyncSubscribers := 0
	for _, sub := range subscribers {
		if !sub.lossless {
			asyncSubscribers++
		}
	}
	var chunk *dynamicMultiWriterChunk
	if asyncSubscribers > 0 {
		chunk = d.newChunk(p, asyncSubscribers)
	}
	var failed []*dynamicMultiWriterSubscriber
	var result error
	delivered := 0
	for _, sub := range subscribers {
		if sub.lossless {
			written, writeErr := sub.writer.Write(p)
			if writeErr != nil {
				result = errors.Join(result, writeErr)
				failed = append(failed, sub)
			} else if written != len(p) {
				result = errors.Join(result, io.ErrShortWrite)
				failed = append(failed, sub)
			} else {
				delivered++
			}
			continue
		}
		sub.enqueue(chunk)
		delivered++
	}

	for _, sub := range failed {
		go d.detachSubscriber(sub, false)
	}
	if result != nil && delivered == 0 {
		return 0, result
	}
	return len(p), nil
}

func (d *DynamicMultiWriter) newChunk(p []byte, refs int) *dynamicMultiWriterChunk {
	chunk, _ := d.pool.Get().(*dynamicMultiWriterChunk)
	if chunk == nil {
		chunk = &dynamicMultiWriterChunk{pool: &d.pool}
	}
	if cap(chunk.data) < len(p) {
		chunk.data = make([]byte, len(p))
	} else {
		chunk.data = chunk.data[:len(p)]
	}
	copy(chunk.data, p)
	atomic.StoreInt32(&chunk.refs, int32(refs))
	return chunk
}

func (c *dynamicMultiWriterChunk) release() {
	if atomic.AddInt32(&c.refs, -1) != 0 {
		return
	}
	pool := c.pool
	c.data = c.data[:0]
	pool.Put(c)
}

func (s *dynamicMultiWriterSubscriber) enqueue(chunk *dynamicMultiWriterChunk) {
	select {
	case s.ch <- chunk:
		s.recordQueueDepth(1)
		return
	default:
	}

	select {
	case dropped := <-s.ch:
		s.recordQueueDepth(-1)
		if s.onDrop != nil {
			s.onDrop(len(dropped.data))
		}
		dropped.release()
	default:
	}

	select {
	case s.ch <- chunk:
		s.recordQueueDepth(1)
	default:
		if s.onDrop != nil {
			s.onDrop(len(chunk.data))
		}
		chunk.release()
	}
}

func (s *dynamicMultiWriterSubscriber) run(onError func()) {
	defer close(s.done)
	defer s.drain()
	for chunk := range s.ch {
		s.recordQueueDepth(-1)
		want := len(chunk.data)
		written, err := s.writer.Write(chunk.data)
		chunk.release()
		if err != nil || written != want {
			onError()
			return
		}
	}
}

func (s *dynamicMultiWriterSubscriber) drain() {
	for {
		select {
		case chunk, ok := <-s.ch:
			if !ok {
				return
			}
			s.recordQueueDepth(-1)
			chunk.release()
		default:
			return
		}
	}
}

func (s *dynamicMultiWriterSubscriber) close() {
	s.once.Do(func() {
		if s.ch != nil {
			close(s.ch)
		}
	})
}

func (s *dynamicMultiWriterSubscriber) recordQueueDepth(delta int64) {
	if s.onQueueDepth != nil {
		s.onQueueDepth(delta)
	}
}
