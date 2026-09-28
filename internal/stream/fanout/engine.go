package fanout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"

	"github.com/21S1298001/mahiron/internal/observability"
)

const (
	// Room a stalled consumer gets before packets are dropped, matching
	// Mirakurun's budget for a backed-up client. Counted in bytes because
	// TLV packets vary in size.
	SubscriberBufferBytes   = 8 << 20
	sectionSubscriberBuffer = 512
)

var (
	ErrSubscriberOverflow = errors.New("stream subscriber buffer overflow")
	ErrStopped            = errors.New("stream engine stopped")
)

// SourceSubscriber writes the live source bytes to dst until ctx ends.
type SourceSubscriber func(context.Context, io.Writer) error

// Engine fans one source out to packet and signaling subscribers, running
// while at least one is attached and stopping for good after the last one
// leaves.
type Engine[S any] struct {
	cancel      context.CancelFunc
	channelID   string
	channelType string
	drops       DropDetector
	done        chan struct{}
	err         error
	mu          sync.Mutex
	nextID      uint64
	onEmpty     func()
	onPackets   []func([]byte)
	onSections  []func(S)
	packets     map[uint64]*packetSubscription
	packetSubs  []packetSubscriptionEntry
	sections    map[uint64]*sectionSubscription[S]
	sectionSubs []sectionSubscriptionEntry[S]
	source      SourceSubscriber
	started     bool
	stopped     bool
	stopOnce    sync.Once
	transport   Transport[S]
}

type packetSubscriptionEntry struct {
	id  uint64
	sub *packetSubscription
}

type sectionSubscriptionEntry[S any] struct {
	id  uint64
	sub *sectionSubscription[S]
}

// New builds an engine reading source through transport. onEmpty runs once
// the engine has stopped.
func New[S any](transport Transport[S], source SourceSubscriber, onEmpty func()) *Engine[S] {
	return &Engine[S]{
		drops:     transport.NewDropDetector(),
		done:      make(chan struct{}),
		onEmpty:   onEmpty,
		packets:   map[uint64]*packetSubscription{},
		sections:  map[uint64]*sectionSubscription[S]{},
		source:    source,
		transport: transport,
	}
}

// WithPackets adds hooks that see every source packet, under the engine
// lock and before any subscriber.
func (e *Engine[S]) WithPackets(onPackets ...func([]byte)) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onPackets = append(e.onPackets, onPackets...)
	return e
}

// WithSections adds hooks that see every completed signaling unit, under the
// engine lock and before any subscriber.
func (e *Engine[S]) WithSections(onSections ...func(S)) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onSections = append(e.onSections, onSections...)
	return e
}

func (e *Engine[S]) WithMetricLabels(channelType, channelID string) *Engine[S] {
	e.channelType = channelType
	e.channelID = channelID
	return e
}

// SubscribeChannel writes every packet to dst until ctx ends or the source
// stops.
func (e *Engine[S]) SubscribeChannel(ctx context.Context, dst io.Writer) error {
	return e.subscribePackets(ctx, nil, dst)
}

// SubscribeService writes one service's packets, as the transport's filter
// selects them, to dst.
func (e *Engine[S]) SubscribeService(ctx context.Context, serviceID uint16, dst io.Writer) error {
	return e.subscribePackets(ctx, &serviceID, dst)
}

// ObserveSections starts the source if necessary and passes the accepted
// signaling to observe until ctx ends, observe fails or the source stops.
func (e *Engine[S]) ObserveSections(ctx context.Context, accept func(S) bool, observe func(S) error) error {
	return e.observeSections(ctx, accept, observe, nil, true)
}

// ObserveSectionsPassive observes like ObserveSections without starting the
// source; attached is closed once the subscription is registered.
func (e *Engine[S]) ObserveSectionsPassive(ctx context.Context, accept func(S) bool, observe func(S) error, attached chan<- struct{}) error {
	return e.observeSections(ctx, accept, observe, attached, false)
}

// KeepAlive starts the engine and keeps it running until ctx is canceled,
// for callers that consume state from the engine's hooks rather than
// signaling directly.
func (e *Engine[S]) KeepAlive(ctx context.Context) error {
	return e.observeSections(ctx, func(S) bool {
		return false
	}, func(S) error {
		return nil
	}, nil, true)
}

func (e *Engine[S]) observeSections(ctx context.Context, accept func(S) bool, observe func(S) error, attached chan<- struct{}, start bool) error {
	sub := &sectionSubscription[S]{
		accept:     accept,
		done:       make(chan struct{}),
		observe:    observe,
		queue:      make(chan S, sectionSubscriberBuffer),
		writerDone: make(chan struct{}),
	}
	id, err := e.attachSection(ctx, sub, start)
	if err != nil {
		return err
	}
	go e.writeSections(id, sub)
	if attached != nil {
		close(attached)
	}
	select {
	case <-ctx.Done():
		e.finishSection(id, ctx.Err())
		<-sub.done
		<-sub.writerDone
		return ctx.Err()
	case <-sub.done:
		if sub.err == nil {
			<-sub.writerDone
		}
		return sub.err
	case <-e.done:
		e.finishSection(id, e.Err())
		<-sub.done
		<-sub.writerDone
		return sub.err
	}
}

func (e *Engine[S]) Stop() {
	e.mu.Lock()
	if e.stopped {
		started := e.started
		done := e.done
		e.mu.Unlock()
		if started {
			<-done
		}
		return
	}
	e.stopped = true
	cancel := e.cancel
	started := e.started
	e.mu.Unlock()
	if !started {
		e.close(nil)
		return
	}
	if cancel != nil {
		cancel()
	}
	<-e.done
}

func (e *Engine[S]) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// PacketSubscriberCount reports the number of attached packet subscribers,
// for tests to wait on without reaching into the engine's internals.
func (e *Engine[S]) PacketSubscriberCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.packets)
}

// SectionSubscriberCount reports the number of attached signaling
// subscribers, for tests.
func (e *Engine[S]) SectionSubscriberCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sections)
}

// Stopped reports whether the engine has permanently stopped and will
// reject any new subscription attempts.
func (e *Engine[S]) Stopped() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopped
}

func (e *Engine[S]) subscribePackets(ctx context.Context, serviceID *uint16, dst io.Writer) error {
	sub := &packetSubscription{
		ctx:        ctx,
		done:       make(chan struct{}),
		queue:      newPacketQueue(),
		serviceID:  serviceID,
		statsKey:   e.streamInfoKey(serviceID),
		writerDone: make(chan struct{}),
	}
	id, err := e.attachPacket(ctx, sub)
	if err != nil {
		return err
	}
	go e.writePackets(id, sub, dst)
	select {
	case <-ctx.Done():
		e.finishPacket(id, ctx.Err())
		<-sub.done
		<-sub.writerDone
		return nil
	case <-sub.done:
		if sub.err == nil {
			<-sub.writerDone
		}
		return sub.err
	case <-e.done:
		e.finishPacket(id, e.Err())
		<-sub.done
		if sub.err == nil {
			<-sub.writerDone
		}
		return sub.err
	}
}

func (e *Engine[S]) attachPacket(ctx context.Context, sub *packetSubscription) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, ErrStopped
	}
	id := e.nextID
	e.nextID++
	sub.drops = e.transport.NewDropDetector()
	if sub.serviceID != nil {
		sub.filter = e.transport.NewFilter(*sub.serviceID)
		if f, ok := sub.filter.(FilterDropDetector); ok {
			sub.drops = f.NewDropDetector()
		}
	}
	e.packets[id] = sub
	e.packetSubs = append(e.packetSubs, packetSubscriptionEntry{id: id, sub: sub})
	e.startLocked(ctx)
	return id, nil
}

func (e *Engine[S]) attachSection(ctx context.Context, sub *sectionSubscription[S], start bool) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, ErrStopped
	}
	id := e.nextID
	e.nextID++
	e.sections[id] = sub
	e.sectionSubs = append(e.sectionSubs, sectionSubscriptionEntry[S]{id: id, sub: sub})
	if start {
		e.startLocked(ctx)
	}
	return id, nil
}

func (e *Engine[S]) startLocked(sourceCtx context.Context) {
	if e.started {
		return
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(sourceCtx))
	e.cancel = cancel
	e.started = true
	go e.run(ctx)
}

func (e *Engine[S]) run(ctx context.Context) {
	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- e.source(ctx, w)
		_ = w.Close()
	}()

	transport := e.transport.Name()
	reader := e.transport.NewReader(r)
	var runErr error
	var packetCount int64
	var byteCount int64
	flushPackets := func() {
		if packetCount == 0 && byteCount == 0 {
			return
		}
		observability.RecordStreamPackets(ctx, transport, e.channelType, e.channelID, packetCount, byteCount)
		packetCount = 0
		byteCount = 0
	}
	for {
		packet, err := reader.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				runErr = err
				observability.RecordStreamPacketError(ctx, transport, e.channelType, e.channelID, "read")
			}
			break
		}
		packetCount++
		byteCount += int64(len(packet))
		if packetCount >= 256 {
			flushPackets()
		}
		if drop := e.drops.Observe(packet); drop != nil {
			observability.RecordStreamDrop(ctx, transport, e.channelType, e.channelID)
			logStreamDrop(transport, e.channelType, e.channelID, "", *drop)
		}
		e.mu.Lock()
		sections, err := e.transport.Feed(packet)
		if err != nil {
			e.mu.Unlock()
			runErr = err
			observability.RecordStreamPacketError(ctx, transport, e.channelType, e.channelID, "demux")
			break
		}
		e.dispatchLocked(packet, sections)
		e.mu.Unlock()
	}
	flushPackets()
	_ = r.Close()
	if err := <-sourceDone; err != nil && ctx.Err() == nil && !errors.Is(err, io.ErrClosedPipe) {
		runErr = errors.Join(runErr, err)
	}
	e.close(runErr)
}

// Dispatch delivers one packet and its signaling as if the source had
// produced them. It exists for tests that drive subscribers directly.
func (e *Engine[S]) Dispatch(packet []byte, sections []S) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatchLocked(packet, sections)
}

func (e *Engine[S]) dispatchLocked(packet []byte, sections []S) {
	for _, hook := range e.onPackets {
		hook(packet)
	}
	var rawPacket []byte
	for i := 0; i < len(e.packetSubs); {
		entry := e.packetSubs[i]
		id := entry.id
		sub := entry.sub
		var out []byte
		if sub.filter != nil {
			filtered, err := sub.filter.Packet(packet)
			if err != nil {
				e.finishPacketLocked(id, err)
				continue
			}
			if filtered == nil {
				i++
				continue
			}
			out = append([]byte(nil), filtered...)
		} else {
			if rawPacket == nil {
				rawPacket = append([]byte(nil), packet...)
			}
			out = rawPacket
		}
		e.enqueueLocked(sub, out)
		i++
	}
	for _, section := range sections {
		for _, hook := range e.onSections {
			hook(section)
		}
		for i := 0; i < len(e.sectionSubs); {
			entry := e.sectionSubs[i]
			id := entry.id
			sub := entry.sub
			if sub.accept != nil && !sub.accept(section) {
				i++
				continue
			}
			select {
			case sub.queue <- section:
				i++
			default:
				observability.RecordStreamSubscriberOverflow(context.Background(), e.transport.Name(), e.channelType, "section_overflow")
				e.finishSectionLocked(id, ErrSubscriberOverflow)
			}
		}
	}
}

// enqueueLocked queues a packet for a subscriber. A slow consumer cannot be
// waited for without stalling every other subscriber, so the oldest packets
// are dropped instead, as Mirakurun and mirakc do; writePackets reports the
// resulting sequence gap as drops.
func (e *Engine[S]) enqueueLocked(sub *packetSubscription, packet []byte) {
	dropped := sub.queue.push(packet, SubscriberBufferBytes)
	transport := e.transport.Name()
	if dropped > 0 {
		if !sub.dropping {
			slog.Warn(transport+" subscriber buffer full, dropping packets", "type", e.channelType, "channel", e.channelID, "stream", sub.statsKey, "bufferBytes", SubscriberBufferBytes)
			sub.dropping = true
		}
		observability.RecordStreamSubscriberOverflow(context.Background(), transport, e.channelType, "packet_overflow")
		sub.droppedBytes += dropped
		return
	}
	if sub.dropping {
		slog.Warn(transport+" subscriber caught up", "type", e.channelType, "channel", e.channelID, "stream", sub.statsKey, "droppedBytes", sub.droppedBytes)
		sub.dropping = false
		sub.droppedBytes = 0
	}
}

func upperTransport(name string) string { return strings.ToUpper(name) }
