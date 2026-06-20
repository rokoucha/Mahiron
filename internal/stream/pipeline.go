package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/util"
)

type PipelineKind string

const (
	PipelineChannelStream PipelineKind = "channel_stream"
	PipelineServiceStream PipelineKind = "service_stream"
)

type PipelineKey struct {
	ChannelType string
	ChannelID   string
	Kind        PipelineKind
	ServiceID   uint16
	Decode      bool
}

type sourceSubscriber func(context.Context, io.Writer) error

type streamPipeline struct {
	cancel     context.CancelFunc
	done       chan struct{}
	err        error
	hub        *util.DynamicMultiWriter
	key        PipelineKey
	mu         sync.Mutex
	onStop     func()
	processors []Processor
	refs       int
	source     sourceSubscriber
	started    bool
	stopped    bool
}

func newStreamPipeline(key PipelineKey, processors []Processor, source sourceSubscriber, onStop func()) *streamPipeline {
	return &streamPipeline{
		done:       make(chan struct{}),
		hub:        util.NewDynamicMultiWriter(),
		key:        key,
		onStop:     onStop,
		processors: processors,
		source:     source,
	}
}

func (p *streamPipeline) Attach(ctx context.Context, dst io.Writer) error {
	if err := p.attach(dst); err != nil {
		return err
	}
	defer p.detach(dst)

	select {
	case <-ctx.Done():
		return nil
	case <-p.done:
		return p.Err()
	}
}

func (p *streamPipeline) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *streamPipeline) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	cancel := p.cancel
	onStop := p.onStop
	p.hub.Close()
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	p.close(nil)
	if onStop != nil {
		onStop()
	}
}

func (p *streamPipeline) attach(dst io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return errors.New("stream pipeline stopped")
	}
	p.refs++
	p.hub.Attach(dst)
	if err := p.startLocked(); err != nil {
		p.refs--
		p.hub.Detach(dst)
		return err
	}
	return nil
}

func (p *streamPipeline) detach(dst io.Writer) {
	p.mu.Lock()
	if p.refs > 0 {
		p.refs--
	}
	p.hub.Detach(dst)
	refs := p.refs
	var cancel context.CancelFunc
	var onStop func()
	if refs == 0 && !p.stopped {
		p.stopped = true
		cancel = p.cancel
		onStop = p.onStop
		p.hub.Close()
	}
	p.mu.Unlock()

	if refs == 0 {
		if cancel != nil {
			cancel()
		}
		p.close(nil)
		if onStop != nil {
			onStop()
		}
	}
	slog.Debug("stream pipeline subscriber detached", "type", p.key.ChannelType, "channel", p.key.ChannelID, "kind", p.key.Kind, "serviceId", p.key.ServiceID, "decode", p.key.Decode, "refs", refs)
}

func (p *streamPipeline) startLocked() error {
	if p.started {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.started = true
	slog.Debug("starting stream pipeline", "type", p.key.ChannelType, "channel", p.key.ChannelID, "kind", p.key.Kind, "serviceId", p.key.ServiceID, "decode", p.key.Decode, "processors", len(p.processors))
	go p.run(ctx)
	return nil
}

func (p *streamPipeline) run(ctx context.Context) {
	err := p.runProcesses(ctx)
	if util.IsExpectedStreamCloseError(err) {
		err = nil
	}
	if err != nil {
		slog.Warn("stream pipeline finished with error", "type", p.key.ChannelType, "channel", p.key.ChannelID, "kind", p.key.Kind, "serviceId", p.key.ServiceID, "decode", p.key.Decode, "err", err)
	} else {
		slog.Debug("stream pipeline finished", "type", p.key.ChannelType, "channel", p.key.ChannelID, "kind", p.key.Kind, "serviceId", p.key.ServiceID, "decode", p.key.Decode)
	}
	p.close(err)
}

func (p *streamPipeline) runProcesses(ctx context.Context) error {
	if len(p.processors) == 0 {
		return p.source(ctx, p.hub)
	}

	errCh := make(chan error, len(p.processors)+1)
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			if p.cancel != nil {
				p.cancel()
			}
		})
	}

	readers := make([]*io.PipeReader, len(p.processors))
	writers := make([]*io.PipeWriter, len(p.processors))
	for i := range p.processors {
		readers[i], writers[i] = io.Pipe()
	}

	go func() {
		err := p.source(ctx, writers[0])
		_ = writers[0].Close()
		errCh <- err
		if err != nil {
			cancel()
		}
	}()

	for i, processor := range p.processors {
		i := i
		processor := processor
		go func() {
			defer readers[i].Close()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("stream processor panic: %v", r)
					cancel()
				}
			}()
			var dst io.Writer = p.hub
			var next *io.PipeWriter
			if i+1 < len(writers) {
				next = writers[i+1]
				dst = next
			}
			if next != nil {
				defer next.Close()
			}
			err := processor.Run(ctx, readers[i], dst)
			errCh <- err
			if err != nil {
				cancel()
			}
		}()
	}

	var result error
	for range len(p.processors) + 1 {
		if err := <-errCh; err != nil && !util.IsExpectedStreamCloseError(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (p *streamPipeline) close(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return
	default:
		p.err = err
		close(p.done)
	}
}
