package demux

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

const (
	packetSubscriberBuffer  = 16384 // ~3 MB / ~1.4 s of a 17 Mbps TS
	sectionSubscriberBuffer = 512
)

var (
	ErrSubscriberOverflow = errors.New("ts subscriber buffer overflow")
	ErrDemuxerStopped     = errors.New("ts demuxer stopped")
)

type SourceSubscriber func(context.Context, io.Writer) error

type Demuxer struct {
	cancel        context.CancelFunc
	channelID     string
	channelType   string
	continuity    *continuityMonitor
	demux         *ts.Demuxer
	done          chan struct{}
	err           error
	mu            sync.Mutex
	nextID        uint64
	onEmpty       func()
	onPIDSections []func(ts.PIDSection)
	onSections    []func(ts.Section)
	packets       map[uint64]*packetSubscription
	packetSubs    []packetSubscriptionEntry
	sections      map[uint64]*sectionSubscription
	sectionSubs   []sectionSubscriptionEntry
	source        SourceSubscriber
	started       bool
	stopped       bool
	stopOnce      sync.Once
}

type packetSubscriptionEntry struct {
	id  uint64
	sub *packetSubscription
}

func (e *Demuxer) WithPIDSections(onSections ...func(ts.PIDSection)) *Demuxer {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onPIDSections = append(e.onPIDSections, onSections...)
	return e
}

type sectionSubscriptionEntry struct {
	id  uint64
	sub *sectionSubscription
}

func New(source SourceSubscriber, onEmpty func(), onSections ...func(ts.Section)) *Demuxer {
	return &Demuxer{
		continuity: &continuityMonitor{},
		demux:      ts.NewDemuxer(),
		done:       make(chan struct{}),
		onEmpty:    onEmpty,
		onSections: onSections,
		packets:    map[uint64]*packetSubscription{},
		sections:   map[uint64]*sectionSubscription{},
		source:     source,
	}
}

func (e *Demuxer) WithMetricLabels(channelType, channelID string) *Demuxer {
	e.channelType = channelType
	e.channelID = channelID
	return e
}

func (e *Demuxer) SubscribeChannel(ctx context.Context, dst io.Writer) error {
	return e.subscribePackets(ctx, nil, dst)
}

func (e *Demuxer) SubscribeService(ctx context.Context, serviceID uint16, dst io.Writer) error {
	return e.subscribePackets(ctx, &serviceID, dst)
}

func (e *Demuxer) ObserveSections(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error) error {
	return e.observeSections(ctx, accept, observe, nil, true)
}

func (e *Demuxer) ObserveSectionsPassive(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error, attached chan<- struct{}) error {
	return e.observeSections(ctx, accept, observe, attached, false)
}

func (e *Demuxer) observeSections(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error, attached chan<- struct{}, start bool) error {
	sub := &sectionSubscription{
		accept:     accept,
		done:       make(chan struct{}),
		observe:    observe,
		queue:      make(chan ts.Section, sectionSubscriberBuffer),
		writerDone: make(chan struct{}),
	}
	id, err := e.attachSection(sub, start)
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

func (e *Demuxer) Stop() {
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

func (e *Demuxer) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// PacketSubscriberCount reports the number of attached packet subscribers.
// It exists so tests can wait for subscribers without reaching into the
// demuxer's internals.
func (e *Demuxer) PacketSubscriberCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.packets)
}

// Stopped reports whether the demuxer has permanently stopped and will
// reject any new subscription attempts.
func (e *Demuxer) Stopped() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopped
}

func (e *Demuxer) subscribePackets(ctx context.Context, serviceID *uint16, dst io.Writer) error {
	sub := &packetSubscription{
		ctx:        ctx,
		continuity: &continuityMonitor{},
		done:       make(chan struct{}),
		queue:      make(chan ts.Packet, packetSubscriberBuffer),
		serviceID:  serviceID,
		statsKey:   e.streamInfoKey(serviceID),
		writerDone: make(chan struct{}),
	}
	id, err := e.attachPacket(sub)
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

func (e *Demuxer) attachPacket(sub *packetSubscription) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, ErrDemuxerStopped
	}
	id := e.nextID
	e.nextID++
	if sub.serviceID != nil {
		sub.service = e.demux.Service(*sub.serviceID)
	}
	e.packets[id] = sub
	e.packetSubs = append(e.packetSubs, packetSubscriptionEntry{id: id, sub: sub})
	e.startLocked()
	return id, nil
}

func (e *Demuxer) attachSection(sub *sectionSubscription, start bool) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, ErrDemuxerStopped
	}
	id := e.nextID
	e.nextID++
	e.sections[id] = sub
	e.sectionSubs = append(e.sectionSubs, sectionSubscriptionEntry{id: id, sub: sub})
	if start {
		e.startLocked()
	}
	return id, nil
}

func (e *Demuxer) startLocked() {
	if e.started {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.started = true
	go e.run(ctx)
}

func (e *Demuxer) run(ctx context.Context) {
	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- e.source(ctx, w)
		_ = w.Close()
	}()

	reader := ts.NewPacketReader(r)
	packetBuf := make([]byte, ts.PacketSize)
	var runErr error
	var packetCount int64
	var byteCount int64
	flushPackets := func() {
		if packetCount == 0 && byteCount == 0 {
			return
		}
		observability.RecordStreamPackets(ctx, e.channelType, e.channelID, packetCount, byteCount)
		packetCount = 0
		byteCount = 0
	}
	for {
		packet, err := reader.NextInto(packetBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				runErr = err
				observability.RecordStreamPacketError(ctx, e.channelType, e.channelID, "read")
			}
			break
		}
		packetCount++
		byteCount += int64(len(packet))
		if packetCount >= 256 {
			flushPackets()
		}
		if e.continuity.observe(packet) {
			observability.RecordStreamContinuityCounterError(ctx, e.channelType, e.channelID)
		}
		sections, err := e.demux.FeedWithPID(packet)
		if err != nil {
			runErr = err
			observability.RecordStreamPacketError(ctx, e.channelType, e.channelID, "demux")
			break
		}
		e.dispatch(packet, sections)
	}
	flushPackets()
	_ = r.Close()
	if err := <-sourceDone; err != nil && ctx.Err() == nil && !errors.Is(err, io.ErrClosedPipe) {
		runErr = errors.Join(runErr, err)
	}
	e.close(runErr)
}

type continuityMonitor struct {
	seen [ts.PIDNull + 1]bool
	last [ts.PIDNull + 1]byte
}

func (m *continuityMonitor) observe(packet ts.Packet) bool {
	if len(packet) != ts.PacketSize || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() || !packet.HasPayload() {
		return false
	}
	pid := packet.PID()
	counter := packet.ContinuityCounter()
	last := m.last[pid]
	ok := m.seen[pid]
	m.seen[pid] = true
	m.last[pid] = counter
	return ok && counter != ((last+1)&0x0f)
}

func (e *Demuxer) dispatch(packet ts.Packet, sections []ts.PIDSection) {
	e.mu.Lock()
	var rawPacket ts.Packet
	for i := 0; i < len(e.packetSubs); {
		entry := e.packetSubs[i]
		id := entry.id
		sub := entry.sub
		out := packet
		if sub.serviceID != nil {
			if e.demux.PATReady() && !e.demux.HasService(*sub.serviceID) {
				e.finishPacketLocked(id, ts.ErrServiceNotFound)
				continue
			}
			out = sub.service.Packet(packet)
		}
		if out == nil {
			i++
			continue
		}
		if sub.serviceID == nil {
			if rawPacket == nil {
				rawPacket = append(ts.Packet(nil), packet...)
			}
			out = rawPacket
		} else {
			out = append(ts.Packet(nil), out...)
		}
		select {
		case sub.queue <- out:
			i++
		default:
			observability.RecordStreamSubscriberOverflow(context.Background(), e.channelType, "packet_overflow")
			e.finishPacketLocked(id, ErrSubscriberOverflow)
		}
	}
	for _, pidSection := range sections {
		section := pidSection.Section
		for _, hook := range e.onPIDSections {
			hook(pidSection)
		}
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
				observability.RecordStreamSubscriberOverflow(context.Background(), e.channelType, "section_overflow")
				e.finishSectionLocked(id, ErrSubscriberOverflow)
			}
		}
	}
	e.mu.Unlock()
}
