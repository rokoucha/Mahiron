package tlv

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
)

// ntpPort is the UDP port of the NTP packets sent next to MMTP. They are
// not MMTP and must not be parsed as such.
const ntpPort = 123

// signal is one unit of signaling a TLV stream completed: a TLV-SI section
// from a transmission control signal packet, an M2 (short) section from an
// MMTP signaling message, or a table of a PA message.
type signal struct {
	// TLVSI marks a section of a transmission control signal packet, whose
	// table IDs mean something else than MMT-SI's.
	TLVSI   bool
	Section mmt.Section
	Table   mmt.Table
	Key     mmt.PacketKey
}

// tlvTransport plugs TLV into fanout.Engine. The engine serializes Feed and
// the filters, so the state needs no lock of its own.
type tlvTransport struct {
	flows     mmt.FlowTracker
	assembler mmt.MessageAssembler

	// current describes the packet the last Feed parsed, for the filters
	// that run on the same packet right after.
	current packetInfo

	pltSeen     bool
	pltServices map[uint16]bool
	// mpts holds each service's latest MPT assets, and owners the services
	// whose MPT lists an asset.
	mpts   map[uint16]map[mmt.PacketKey]bool
	owners map[mmt.PacketKey]map[uint16]bool
}

type packetInfo struct {
	media bool
	key   mmt.PacketKey
}

func newTransport() *tlvTransport {
	return &tlvTransport{
		pltServices: map[uint16]bool{},
		mpts:        map[uint16]map[mmt.PacketKey]bool{},
		owners:      map[mmt.PacketKey]map[uint16]bool{},
	}
}

func (t *tlvTransport) Name() string { return "tlv" }

// NewReader checks the stream is TLV before any packet is delivered, which
// catches a remote that silently converts ISDB-S3 to TS.
func (t *tlvTransport) NewReader(r io.Reader) fanout.PacketReader {
	return &packetReader{reader: mmt.NewTLVReader(packet.NewCheckReader(r, mmt.Detector, ts.Detector))}
}

type packetReader struct {
	reader *mmt.TLVReader
}

func (r *packetReader) Next() ([]byte, error) {
	pkt, err := r.reader.Next()
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// A trailing partial packet of a finite source.
		return nil, io.EOF
	}
	var mismatch *packet.MismatchError
	var unknown *packet.UnknownError
	switch {
	case errors.As(err, &mismatch):
		return nil, fmt.Errorf("%w: %s", ErrNotTLVStream, mismatch)
	case errors.As(err, &unknown):
		return nil, fmt.Errorf("%w: %s", ErrNotTLVStream, unknown)
	}
	return pkt, err
}

// parseMMTP returns the MMTP packet a TLV packet carries and its key. It
// reports false for TLV-SI, null and NTP packets, malformed packets and
// compressed packets of a context whose flow is not yet known.
func parseMMTP(flows *mmt.FlowTracker, packet mmt.TLVPacket) (*mmt.MMTPPacket, mmt.PacketKey, bool) {
	u, err := mmt.ParseUDPPacket(packet)
	if err != nil {
		return nil, mmt.PacketKey{}, false
	}
	flow, ok := flows.Flow(u)
	if !ok || flow.DestinationPort == ntpPort {
		return nil, mmt.PacketKey{}, false
	}
	m, err := mmt.ParseMMTPPacket(u.Payload)
	if err != nil {
		return nil, mmt.PacketKey{}, false
	}
	return m, mmt.PacketKey{Flow: flow, PacketID: m.PacketID}, true
}

// Feed returns the signaling a packet completes. Malformed packets are
// skipped rather than failing the stream: the bytes are still delivered.
func (t *tlvTransport) Feed(packet []byte) ([]signal, error) {
	t.current = packetInfo{}
	p := mmt.TLVPacket(packet)
	if p.Type() == mmt.TLVPacketTypeTransmissionControl {
		section := mmt.Section(p.Data())
		if len(section) < 3 || section.TotalLength() > len(section) {
			return nil, nil
		}
		return []signal{{TLVSI: true, Section: append(mmt.Section(nil), section[:section.TotalLength()]...)}}, nil
	}
	m, key, ok := parseMMTP(&t.flows, p)
	if !ok {
		return nil, nil
	}
	if m.PayloadType != mmt.MMTPPayloadTypeSignaling {
		t.current = packetInfo{media: true, key: key}
		return nil, nil
	}
	messages, err := t.assembler.Feed(key.Flow, m)
	if err != nil {
		return nil, nil
	}
	var signals []signal
	for _, message := range messages {
		if len(message) < 2 {
			continue
		}
		switch message.ID() {
		case mmt.MessageIDPA:
			pa, err := mmt.ParsePAMessage(message)
			if err != nil {
				continue
			}
			for _, table := range pa.Tables {
				t.observeTable(key, table)
				signals = append(signals, signal{Table: table, Key: key})
			}
		case mmt.MessageIDM2Section, mmt.MessageIDM2ShortSection:
			section, err := message.Section()
			if err != nil {
				continue
			}
			signals = append(signals, signal{Section: section, Key: key})
		}
	}
	return signals, nil
}

// observeTable keeps the PLT's services and each service's MPT assets, which
// service extraction needs.
func (t *tlvTransport) observeTable(key mmt.PacketKey, table mmt.Table) {
	switch {
	case table.TableID() == mmt.TableIDPLT:
		plt, err := mmt.ParsePLT(table)
		if err != nil {
			return
		}
		services := make(map[uint16]bool, len(plt.Packages))
		for _, pkg := range plt.Packages {
			services[pkg.ServiceID()] = true
		}
		t.pltSeen = true
		t.pltServices = services
	case table.TableID() == mmt.TableIDMPT:
		mpt, err := mmt.ParseMPT(table)
		if err != nil {
			return
		}
		serviceID := mpt.ServiceID()
		for asset := range t.mpts[serviceID] {
			delete(t.owners[asset], serviceID)
			if len(t.owners[asset]) == 0 {
				delete(t.owners, asset)
			}
		}
		assets := map[mmt.PacketKey]bool{}
		for _, asset := range mpt.Assets {
			for _, location := range asset.Locations {
				if asset, ok := assetKey(key.Flow, location); ok {
					assets[asset] = true
				}
			}
		}
		t.mpts[serviceID] = assets
		for asset := range assets {
			if t.owners[asset] == nil {
				t.owners[asset] = map[uint16]bool{}
			}
			t.owners[asset][serviceID] = true
		}
	}
}

// assetKey resolves where an MPT asset is sent. The same-flow location
// means the flow of the PA message that carried the MPT.
func assetKey(mptFlow mmt.IPDataFlow, location mmt.GeneralLocationInfo) (mmt.PacketKey, bool) {
	switch location.LocationType {
	case mmt.LocationTypeSameFlow:
		return mmt.PacketKey{Flow: mptFlow, PacketID: location.PacketID}, true
	case mmt.LocationTypeIPv4, mmt.LocationTypeIPv6:
		flow := mmt.IPDataFlow{Source: location.Source, Destination: location.Destination, DestinationPort: location.DestinationPort}
		return mmt.PacketKey{Flow: flow, PacketID: location.PacketID}, true
	default:
		return mmt.PacketKey{}, false
	}
}

func (t *tlvTransport) NewFilter(serviceID uint16) fanout.Filter {
	return &serviceFilter{transport: t, serviceID: serviceID}
}

func (t *tlvTransport) NewDropDetector() fanout.DropDetector { return &sequenceMonitor{} }

// serviceFilter picks one service out of a TLV stream. A stream with a
// single service passes through untouched. With several services it drops
// the media packets of assets that only other services' MPTs list, and
// keeps everything else: the service's own and shared assets, signaling
// (including the PLT, which is not rewritten), TLV-SI and NTP.
type serviceFilter struct {
	transport *tlvTransport
	serviceID uint16
	// removing is set once the filter drops a packet. The subscriber then
	// sees gaps in the header compression sequence numbers that are not
	// losses, so its drop detection stops.
	removing atomic.Bool
}

// NewDropDetector returns the subscriber's detector, which stops once the
// filter removes packets.
func (f *serviceFilter) NewDropDetector() fanout.DropDetector {
	return &sequenceMonitor{paused: f.removing.Load}
}

var ErrServiceNotFound = errors.New("tlv: service not found")

func (f *serviceFilter) Packet(packet []byte) ([]byte, error) {
	t := f.transport
	if t.pltSeen && !t.pltServices[f.serviceID] {
		return nil, ErrServiceNotFound
	}
	if len(t.mpts) <= 1 || !t.current.media {
		return packet, nil
	}
	owners := t.owners[t.current.key]
	if len(owners) == 0 || owners[f.serviceID] {
		return packet, nil
	}
	f.removing.Store(true)
	return nil, nil
}

// sequenceMonitor detects lost packets from the sequence numbers of
// header-compressed IP packets, which count every packet of a context
// (ARIB STD-B32 Part 3, 3.7). MMTP packet_sequence_number is not used: some
// broadcasters skip it now and then for some assets while the header
// compression sequence stays unbroken, which would report losses that never
// happened. The 4-bit number misses a loss of a multiple of 16 packets.
type sequenceMonitor struct {
	last   map[uint16]byte
	paused func() bool
}

func (m *sequenceMonitor) Observe(packet []byte) *fanout.Drop {
	p := mmt.TLVPacket(packet)
	if len(p) < 6 || p.Type() != mmt.TLVPacketTypeCompressedIP {
		return nil
	}
	if m.paused != nil && m.paused() {
		m.last = nil
		return nil
	}
	data := p.Data()
	contextID := uint16(data[0])<<4 | uint16(data[1]>>4)
	sequence := data[1] & 0x0f
	if m.last == nil {
		m.last = map[uint16]byte{}
	}
	last, seen := m.last[contextID]
	m.last[contextID] = sequence
	expected := (last + 1) & 0x0f
	if !seen || sequence == expected {
		return nil
	}
	return &fanout.Drop{
		Sequence: fmt.Sprintf("cid=%d", contextID),
		Expected: uint64(expected),
		Actual:   uint64(sequence),
	}
}
