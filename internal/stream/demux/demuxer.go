// Package demux is the MPEG-2 TS side of stream fan-out: it splits the
// source into 188-byte packets, demuxes PSI/SI sections, extracts services by
// rewriting PAT/PMT and watches continuity counters, while fanout.Engine
// owns the subscribers.
package demux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
)

type SourceSubscriber = fanout.SourceSubscriber

var (
	ErrSubscriberOverflow = fanout.ErrSubscriberOverflow
	ErrDemuxerStopped     = fanout.ErrStopped
)

// ErrNotTSStream is returned when the bytes delivered for a TS channel do
// not start with the TS sync byte, e.g. a misconfigured channel or a remote
// that converts to another format.
var ErrNotTSStream = errors.New("demux: stream is not MPEG-2 TS")

// Demuxer fans a TS source out to packet and section subscribers.
type Demuxer struct {
	*fanout.Engine[ts.PIDSection]
}

func New(source SourceSubscriber, onEmpty func(), onSections ...func(ts.Section)) *Demuxer {
	engine := fanout.New[ts.PIDSection](&tsTransport{demux: ts.NewDemuxer()}, source, onEmpty)
	for _, hook := range onSections {
		engine.WithSections(func(section ts.PIDSection) { hook(section.Section) })
	}
	return &Demuxer{Engine: engine}
}

func (e *Demuxer) WithPackets(onPackets ...func(ts.Packet)) *Demuxer {
	for _, hook := range onPackets {
		e.Engine.WithPackets(func(packet []byte) { hook(ts.Packet(packet)) })
	}
	return e
}

func (e *Demuxer) WithPIDSections(onSections ...func(ts.PIDSection)) *Demuxer {
	e.WithSections(onSections...)
	return e
}

func (e *Demuxer) WithMetricLabels(channelType, channelID string) *Demuxer {
	e.Engine.WithMetricLabels(channelType, channelID)
	return e
}

func (e *Demuxer) ObserveSections(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error) error {
	return e.Engine.ObserveSections(ctx, acceptPIDSection(accept), observePIDSection(observe))
}

func (e *Demuxer) ObserveSectionsPassive(ctx context.Context, accept func(ts.Section) bool, observe func(ts.Section) error, attached chan<- struct{}) error {
	return e.Engine.ObserveSectionsPassive(ctx, acceptPIDSection(accept), observePIDSection(observe), attached)
}

func acceptPIDSection(accept func(ts.Section) bool) func(ts.PIDSection) bool {
	if accept == nil {
		return nil
	}
	return func(section ts.PIDSection) bool { return accept(section.Section) }
}

func observePIDSection(observe func(ts.Section) error) func(ts.PIDSection) error {
	return func(section ts.PIDSection) error { return observe(section.Section) }
}

// tsTransport plugs TS into fanout.Engine. The engine serializes Feed and
// the filters, so the shared ts.Demuxer needs no lock of its own.
type tsTransport struct {
	demux *ts.Demuxer
}

func (t *tsTransport) Name() string { return "ts" }

// NewReader checks the stream is TS before any packet is delivered, which
// catches a route configured as TS that actually carries another format.
func (t *tsTransport) NewReader(r io.Reader) fanout.PacketReader {
	return &packetReader{reader: ts.NewPacketReader(packet.NewCheckReader(r, ts.Detector, mmt.Detector)), buf: make([]byte, ts.PacketSize)}
}

func (t *tsTransport) Feed(packet []byte) ([]ts.PIDSection, error) {
	return t.demux.FeedWithPID(ts.Packet(packet))
}

func (t *tsTransport) NewFilter(serviceID uint16) fanout.Filter {
	return &serviceFilter{demux: t.demux, serviceID: serviceID, service: t.demux.Service(serviceID)}
}

func (t *tsTransport) NewDropDetector() fanout.DropDetector { return &continuityMonitor{} }

type packetReader struct {
	reader *ts.PacketReader
	buf    []byte
}

func (r *packetReader) Next() ([]byte, error) {
	pkt, err := r.reader.NextInto(r.buf)
	var mismatch *packet.MismatchError
	var unknown *packet.UnknownError
	switch {
	case errors.As(err, &mismatch):
		return nil, fmt.Errorf("%w: %s", ErrNotTSStream, mismatch)
	case errors.As(err, &unknown):
		return nil, fmt.Errorf("%w: %s", ErrNotTSStream, unknown)
	}
	return pkt, err
}

// serviceFilter extracts one service by rewriting PAT/PMT. It ends the
// subscription once a complete PAT no longer lists the service.
type serviceFilter struct {
	demux     *ts.Demuxer
	serviceID uint16
	service   *ts.ServiceDemux
}

func (f *serviceFilter) Packet(packet []byte) ([]byte, error) {
	if f.demux.PATReady() && !f.demux.HasService(f.serviceID) {
		return nil, ts.ErrServiceNotFound
	}
	return f.service.Packet(ts.Packet(packet)), nil
}

type continuityMonitor struct {
	seen          [ts.PIDNull + 1]bool
	last          [ts.PIDNull + 1]byte
	duplicateSeen [ts.PIDNull + 1]bool
	lastPacket    map[uint16]ts.Packet
}

type continuityDrop struct {
	PID             uint16
	ExpectedCounter byte
	ActualCounter   byte
}

func (m *continuityMonitor) Observe(packet []byte) *fanout.Drop {
	drop := m.observe(ts.Packet(packet))
	if drop == nil {
		return nil
	}
	return &fanout.Drop{
		Sequence: fmt.Sprintf("pid=0x%04X", drop.PID),
		Expected: uint64(drop.ExpectedCounter),
		Actual:   uint64(drop.ActualCounter),
	}
}

func (m *continuityMonitor) observe(packet ts.Packet) *continuityDrop {
	if len(packet) != ts.PacketSize || packet[0] != ts.SyncByte || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
		return nil
	}
	pid := packet.PID()
	if packet.DiscontinuityIndicator() {
		m.seen[pid] = false
		m.duplicateSeen[pid] = false
		if m.lastPacket != nil {
			delete(m.lastPacket, pid)
		}
	}
	if !packet.HasPayload() {
		return nil
	}
	counter := packet.ContinuityCounter()
	last := m.last[pid]
	ok := m.seen[pid]
	m.seen[pid] = true
	m.last[pid] = counter

	if ok && counter == last && !m.duplicateSeen[pid] && m.sameAsLast(pid, packet) {
		m.duplicateSeen[pid] = true
		return nil
	}
	m.duplicateSeen[pid] = false
	m.remember(pid, packet)

	expected := (last + 1) & 0x0f
	if ok && counter != expected {
		return &continuityDrop{
			PID:             pid,
			ExpectedCounter: expected,
			ActualCounter:   counter,
		}
	}
	return nil
}

func (m *continuityMonitor) sameAsLast(pid uint16, packet ts.Packet) bool {
	previous := m.lastPacket[pid]
	return len(previous) == ts.PacketSize && bytes.Equal(previous, packet)
}

func (m *continuityMonitor) remember(pid uint16, packet ts.Packet) {
	if m.lastPacket == nil {
		m.lastPacket = make(map[uint16]ts.Packet)
	}
	previous := m.lastPacket[pid]
	if len(previous) != ts.PacketSize {
		previous = make(ts.Packet, ts.PacketSize)
	}
	copy(previous, packet)
	m.lastPacket[pid] = previous
}
