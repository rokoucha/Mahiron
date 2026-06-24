package stream

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

const (
	packetSubscriberBuffer  = 512
	sectionSubscriberBuffer = 64
)

var ErrSubscriberOverflow = errors.New("ts subscriber buffer overflow")

type sourceSubscriber func(context.Context, io.Writer) error

type packetEngine struct {
	cancel      context.CancelFunc
	channelID   string
	channelType string
	continuity  *continuityMonitor
	demux       *ts.Demuxer
	done        chan struct{}
	err         error
	mu          sync.Mutex
	nextID      uint64
	onEmpty     func()
	onSections  []func(ts.Section)
	packets     map[uint64]*packetSubscription
	sections    map[uint64]*sectionSubscription
	source      sourceSubscriber
	started     bool
	stopped     bool
	stopOnce    sync.Once
}

type packetSubscription struct {
	ctx        context.Context
	continuity *continuityMonitor
	done       chan struct{}
	err        error
	finished   bool
	queue      chan ts.Packet
	service    *ts.ServiceDemux
	serviceID  *uint16
	stats      tuner.StreamInfo
	statsKey   string
	writerDone chan struct{}
}

type sectionSubscription struct {
	accept     func(ts.Section) bool
	done       chan struct{}
	err        error
	finished   bool
	observe    func(ts.Section) error
	queue      chan ts.Section
	writerDone chan struct{}
}

func newPacketEngine(source sourceSubscriber, onEmpty func(), onSections ...func(ts.Section)) *packetEngine {
	return &packetEngine{
		continuity: &continuityMonitor{last: map[uint16]byte{}},
		demux:      ts.NewDemuxer(),
		done:       make(chan struct{}),
		onEmpty:    onEmpty,
		onSections: onSections,
		packets:    map[uint64]*packetSubscription{},
		sections:   map[uint64]*sectionSubscription{},
		source:     source,
	}
}

func (e *packetEngine) withMetricLabels(channelType, channelID string) *packetEngine {
	e.channelType = channelType
	e.channelID = channelID
	return e
}

func (e *packetEngine) SubscribeChannel(ctx context.Context, dst io.Writer) error {
	return e.subscribePackets(ctx, nil, dst)
}

func (e *packetEngine) SubscribeService(ctx context.Context, serviceID uint16, dst io.Writer) error {
	return e.subscribePackets(ctx, &serviceID, dst)
}

func (e *packetEngine) ObserveSections(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error) error {
	return e.observeSections(ctx, accept, observe, nil, true)
}

func (e *packetEngine) observeSectionsPassive(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error, attached chan<- struct{}) error {
	return e.observeSections(ctx, accept, observe, attached, false)
}

func (e *packetEngine) observeSections(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error, attached chan<- struct{}, start bool) error {
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
		return ctx.Err()
	case <-sub.done:
		if sub.err == nil {
			<-sub.writerDone
		}
		return sub.err
	case <-e.done:
		e.finishSection(id, e.Err())
		<-sub.done
		if sub.err == nil {
			<-sub.writerDone
		}
		return sub.err
	}
}

func (e *packetEngine) Stop() {
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

func (e *packetEngine) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *packetEngine) subscribePackets(ctx context.Context, serviceID *uint16, dst io.Writer) error {
	sub := &packetSubscription{
		ctx:        ctx,
		continuity: &continuityMonitor{last: map[uint16]byte{}},
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

func (e *packetEngine) attachPacket(sub *packetSubscription) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, errors.New("ts engine stopped")
	}
	id := e.nextID
	e.nextID++
	if sub.serviceID != nil {
		sub.service = e.demux.Service(*sub.serviceID)
	}
	e.packets[id] = sub
	e.startLocked()
	return id, nil
}

func (e *packetEngine) attachSection(sub *sectionSubscription, start bool) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return 0, errors.New("ts engine stopped")
	}
	id := e.nextID
	e.nextID++
	e.sections[id] = sub
	if start {
		e.startLocked()
	}
	return id, nil
}

func (e *packetEngine) startLocked() {
	if e.started {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.started = true
	go e.run(ctx)
}

func (e *packetEngine) run(ctx context.Context) {
	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- e.source(ctx, w)
		_ = w.Close()
	}()

	reader := ts.NewPacketReader(r)
	var runErr error
	for {
		packet, err := reader.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				runErr = err
				observability.RecordStreamPacketError(ctx, e.channelType, e.channelID, "read")
			}
			break
		}
		observability.RecordStreamPacket(ctx, e.channelType, e.channelID, int64(len(packet)))
		if e.continuity.observe(packet) {
			observability.RecordStreamContinuityCounterError(ctx, e.channelType, e.channelID)
		}
		sections, err := e.demux.Feed(packet)
		if err != nil {
			runErr = err
			observability.RecordStreamPacketError(ctx, e.channelType, e.channelID, "demux")
			break
		}
		e.dispatch(packet, sections)
	}
	_ = r.Close()
	if err := <-sourceDone; err != nil && ctx.Err() == nil && !errors.Is(err, io.ErrClosedPipe) {
		runErr = errors.Join(runErr, err)
	}
	e.close(runErr)
}

type continuityMonitor struct {
	last map[uint16]byte
}

func (m *continuityMonitor) observe(packet ts.Packet) bool {
	if len(packet) != ts.PacketSize || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() || !packet.HasPayload() {
		return false
	}
	pid := packet.PID()
	counter := packet.ContinuityCounter()
	last, ok := m.last[pid]
	m.last[pid] = counter
	return ok && counter != ((last+1)&0x0f)
}

func (e *packetEngine) dispatch(packet ts.Packet, sections []ts.Section) {
	e.mu.Lock()
	for id, sub := range e.packets {
		out := packet
		if sub.serviceID != nil {
			if e.demux.PATReady() && !e.demux.HasService(*sub.serviceID) {
				e.finishPacketLocked(id, ts.ErrServiceNotFound)
				continue
			}
			out = sub.service.Packet(packet)
		}
		if out == nil {
			continue
		}
		select {
		case sub.queue <- out:
		default:
			observability.RecordStreamSubscriberOverflow(context.Background(), e.channelType, "packet_overflow")
			e.finishPacketLocked(id, ErrSubscriberOverflow)
		}
	}
	for _, section := range sections {
		for _, hook := range e.onSections {
			hook(section)
		}
		for id, sub := range e.sections {
			if sub.accept != nil && !sub.accept(section) {
				continue
			}
			select {
			case sub.queue <- section:
			default:
				observability.RecordStreamSubscriberOverflow(context.Background(), e.channelType, "section_overflow")
				e.finishSectionLocked(id, ErrSubscriberOverflow)
			}
		}
	}
	e.mu.Unlock()
}

func (e *packetEngine) writePackets(id uint64, sub *packetSubscription, dst io.Writer) {
	defer close(sub.writerDone)
	for packet := range sub.queue {
		n, err := dst.Write(packet)
		if err == nil && n != len(packet) {
			err = io.ErrShortWrite
		}
		if err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "write")
			e.finishPacket(id, err)
			return
		}
		sub.stats.Packet++
		if sub.continuity.observe(packet) {
			sub.stats.Drop++
		}
		tuner.ReportStreamInfo(sub.ctx, sub.statsKey, sub.stats)
	}
}

func (e *packetEngine) streamInfoKey(serviceID *uint16) string {
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

func (e *packetEngine) writeSections(id uint64, sub *sectionSubscription) {
	defer close(sub.writerDone)
	for section := range sub.queue {
		if err := sub.observe(section); err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "observe")
			e.finishSection(id, err)
			return
		}
	}
}

func (e *packetEngine) finishPacket(id uint64, err error) {
	e.mu.Lock()
	e.finishPacketLocked(id, err)
	e.mu.Unlock()
}

func (e *packetEngine) finishPacketLocked(id uint64, err error) {
	sub := e.packets[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.packets, id)
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *packetEngine) finishSection(id uint64, err error) {
	e.mu.Lock()
	e.finishSectionLocked(id, err)
	e.mu.Unlock()
}

func (e *packetEngine) finishSectionLocked(id uint64, err error) {
	sub := e.sections[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.sections, id)
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *packetEngine) cancelIfEmptyLocked() {
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

func (e *packetEngine) close(err error) {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.err = err
		e.stopped = true
		for id := range e.packets {
			e.finishPacketLocked(id, err)
		}
		for id := range e.sections {
			e.finishSectionLocked(id, err)
		}
		onEmpty := e.onEmpty
		e.mu.Unlock()
		close(e.done)
		if onEmpty != nil {
			onEmpty()
		}
	})
}
