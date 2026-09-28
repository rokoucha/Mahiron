package fanout

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type packetSubscription struct {
	ctx        context.Context
	drops      DropDetector
	done       chan struct{}
	err        error
	filter     Filter
	finished   bool
	queue      *packetQueue
	serviceID  *uint16
	stats      tuner.StreamInfo
	statsKey   string
	writerDone chan struct{}

	// dropping and droppedBytes belong to the engine's mutex, unlike stats
	// which the writer goroutine owns.
	dropping     bool
	droppedBytes int
}

type sectionSubscription[S any] struct {
	accept     func(S) bool
	done       chan struct{}
	err        error
	finished   bool
	observe    func(S) error
	queue      chan S
	writerDone chan struct{}
}

// packetWriteBatchBytes bounds one write to a subscriber. It matches the 64
// TS packets the TS demuxer used to batch.
const packetWriteBatchBytes = 64 * 188

// packetQueue holds a subscriber's pending packets within a byte budget, in
// a ring that grows as needed.
type packetQueue struct {
	mu     sync.Mutex
	ring   [][]byte
	head   int
	count  int
	bytes  int
	closed bool
	ready  chan struct{}
}

func newPacketQueue() *packetQueue {
	return &packetQueue{ring: make([][]byte, 64), ready: make(chan struct{}, 1)}
}

func (q *packetQueue) popLocked() []byte {
	packet := q.ring[q.head]
	q.ring[q.head] = nil
	q.head = (q.head + 1) % len(q.ring)
	q.count--
	q.bytes -= len(packet)
	return packet
}

// push appends packet, first dropping the oldest packets that do not leave
// room for it. It returns the number of bytes dropped.
func (q *packetQueue) push(packet []byte, limit int) int {
	q.mu.Lock()
	dropped := 0
	for q.count > 0 && q.bytes+len(packet) > limit {
		dropped += len(q.popLocked())
	}
	if q.bytes+len(packet) > limit {
		// A single packet larger than the whole budget.
		q.mu.Unlock()
		return dropped + len(packet)
	}
	if q.count == len(q.ring) {
		ring := make([][]byte, 2*len(q.ring))
		n := copy(ring, q.ring[q.head:])
		copy(ring[n:], q.ring[:q.head])
		q.ring = ring
		q.head = 0
	}
	q.ring[(q.head+q.count)%len(q.ring)] = packet
	q.count++
	q.bytes += len(packet)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return dropped
}

func (q *packetQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// take waits for packets and appends those that fit in max bytes (at least
// one) to dst. It returns false once the queue is closed and drained.
func (q *packetQueue) take(dst [][]byte, max int) ([][]byte, bool) {
	for {
		q.mu.Lock()
		if q.count > 0 {
			size := 0
			for q.count > 0 && (size == 0 || size+len(q.ring[q.head]) <= max) {
				packet := q.popLocked()
				size += len(packet)
				dst = append(dst, packet)
			}
			q.mu.Unlock()
			return dst, true
		}
		if q.closed {
			q.mu.Unlock()
			return dst, false
		}
		q.mu.Unlock()
		<-q.ready
	}
}

func (e *Engine[S]) writePackets(id uint64, sub *packetSubscription, dst io.Writer) {
	defer close(sub.writerDone)
	transport := e.transport.Name()
	buf := make([]byte, 0, packetWriteBatchBytes)
	var batch [][]byte
	for {
		var ok bool
		batch, ok = sub.queue.take(batch[:0], packetWriteBatchBytes)
		if !ok {
			return
		}
		buf = buf[:0]
		for _, packet := range batch {
			buf = append(buf, packet...)
			sub.stats.Packet++
			if drop := sub.drops.Observe(packet); drop != nil {
				sub.stats.Drop++
				logStreamDrop(transport, e.channelType, e.channelID, sub.statsKey, *drop)
			}
		}
		clear(batch)
		n, err := dst.Write(buf)
		if err == nil && n != len(buf) {
			err = io.ErrShortWrite
		}
		if err != nil {
			observability.RecordStreamSubscriberError(context.Background(), transport, e.channelType, "write")
			e.finishPacket(id, err)
			return
		}
		tuner.ReportStreamInfo(sub.ctx, sub.statsKey, sub.stats)
	}
}

func (e *Engine[S]) streamInfoKey(serviceID *uint16) string {
	key := e.channelType
	if e.channelID != "" {
		if key != "" {
			key += "/"
		}
		key += e.channelID
	}
	if serviceID != nil {
		key += ":" + strconv.Itoa(int(*serviceID))
	}
	if key == "" {
		key = "stream"
	}
	return key
}

func (e *Engine[S]) writeSections(id uint64, sub *sectionSubscription[S]) {
	defer close(sub.writerDone)
	for section := range sub.queue {
		if err := sub.observe(section); err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.transport.Name(), e.channelType, "observe")
			e.finishSection(id, err)
			return
		}
	}
}

func (e *Engine[S]) finishPacket(id uint64, err error) {
	e.mu.Lock()
	e.finishPacketLocked(id, err)
	e.mu.Unlock()
}

func (e *Engine[S]) finishPacketLocked(id uint64, err error) {
	sub := e.packets[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.packets, id)
	e.packetSubs = slices.DeleteFunc(e.packetSubs, func(entry packetSubscriptionEntry) bool {
		return entry.id == id
	})
	sub.queue.close()
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *Engine[S]) finishSection(id uint64, err error) {
	e.mu.Lock()
	e.finishSectionLocked(id, err)
	e.mu.Unlock()
}

func (e *Engine[S]) finishSectionLocked(id uint64, err error) {
	sub := e.sections[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.sections, id)
	e.sectionSubs = slices.DeleteFunc(e.sectionSubs, func(entry sectionSubscriptionEntry[S]) bool {
		return entry.id == id
	})
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *Engine[S]) cancelIfEmptyLocked() {
	if len(e.packets) != 0 || len(e.sections) != 0 {
		return
	}
	e.stopped = true
	if e.cancel != nil {
		e.cancel()
	} else {
		go e.close(nil)
	}
}

func (e *Engine[S]) close(err error) {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.err = err
		e.stopped = true
		for len(e.packetSubs) > 0 {
			e.finishPacketLocked(e.packetSubs[0].id, err)
		}
		for len(e.sectionSubs) > 0 {
			e.finishSectionLocked(e.sectionSubs[0].id, err)
		}
		onEmpty := e.onEmpty
		e.mu.Unlock()
		close(e.done)
		if onEmpty != nil {
			onEmpty()
		}
	})
}

const streamDropLogInterval = 5 * time.Second

var streamDropLogLast sync.Map // string -> int64 (UnixNano)

// logStreamDrop logs a sequence gap, at most once per interval for each
// channel, subscriber stream and packet sequence.
func logStreamDrop(transport, channelType, channelID, streamKey string, drop Drop) {
	key := transport + "/" + channelType + "/" + channelID + "/" + streamKey + "/" + drop.Sequence
	now := time.Now().UnixNano()
	if last, ok := streamDropLogLast.Load(key); ok {
		if now-last.(int64) < streamDropLogInterval.Nanoseconds() {
			return
		}
	}
	streamDropLogLast.Store(key, now)

	attrs := []any{
		"sequence", drop.Sequence,
		"expected", drop.Expected,
		"actual", drop.Actual,
	}
	if channelType != "" {
		attrs = append(attrs, "type", channelType)
	}
	if channelID != "" {
		attrs = append(attrs, "channel", channelID)
	}
	if streamKey != "" {
		attrs = append(attrs, "stream", streamKey)
	}
	slog.Warn(upperTransport(transport)+" packet drop detected", attrs...)
}
