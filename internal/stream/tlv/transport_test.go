package tlv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/mmt/mmttest"
)

var (
	testSource      = [16]byte{0x20, 0x01, 0x0d, 0xb8, 15: 0x02}
	testDestination = [16]byte{0xff, 0x3e, 15: 0x01}
)

const testMMTPPort = 51216

// streamBuilder assembles a TLV stream whose MMTP packets share one
// header-compressed context, as advanced BS sends them.
type streamBuilder struct {
	out      []byte
	started  bool
	sequence map[uint16]uint32
	// compressed is the header compression sequence number of the next
	// packet.
	compressed byte
}

func (b *streamBuilder) mmtp(p mmttest.MMTP) {
	if b.sequence == nil {
		b.sequence = map[uint16]uint32{}
	}
	p.SequenceNumber = b.sequence[p.PacketID]
	b.sequence[p.PacketID]++
	sequence := b.compressed
	b.compressed = (b.compressed + 1) & 0x0f
	if !b.started {
		b.started = true
		b.out = append(b.out, mmttest.CompressedIPv6UDP(1, sequence, testSource, testDestination, 1000, testMMTPPort, p.Bytes())...)
		return
	}
	b.out = append(b.out, mmttest.CompressedIP(1, sequence, p.Bytes())...)
}

func (b *streamBuilder) message(packetID uint16, message []byte) {
	b.mmtp(mmttest.MMTP{PayloadType: mmt.MMTPPayloadTypeSignaling, PacketID: packetID, Payload: mmttest.SignalingPayload(0, 0, message)})
}

func (b *streamBuilder) media(packetID uint16, fill byte) {
	b.mmtp(mmttest.MMTP{PayloadType: mmt.MMTPPayloadTypeMPU, PacketID: packetID, Payload: bytes.Repeat([]byte{fill}, 32)})
}

func (b *streamBuilder) ntp() {
	b.out = append(b.out, mmttest.IPv6UDP(testSource, [16]byte{0xff, 0x02, 15: 0x01}, ntpPort, ntpPort, []byte("ntp"))...)
}

func (b *streamBuilder) tlvSI(section []byte) {
	b.out = append(b.out, mmttest.TLV(mmt.TLVPacketTypeTransmissionControl, section)...)
}

type testPackage struct {
	serviceID   uint16
	mptPacketID uint16
}

func pltTable(packages ...testPackage) []byte {
	body := []byte{byte(len(packages))}
	for _, p := range packages {
		body = append(body, 2, byte(p.serviceID>>8), byte(p.serviceID), mmt.LocationTypeSameFlow, byte(p.mptPacketID>>8), byte(p.mptPacketID))
	}
	return mmttest.Table(mmt.TableIDPLT, 0, append(body, 0))
}

type testAsset struct {
	packetID     uint16
	assetType    string
	componentTag uint16
}

func mptTable(serviceID uint16, assets ...testAsset) []byte {
	body := []byte{0xff, 2, byte(serviceID >> 8), byte(serviceID), 0, 0, byte(len(assets))}
	for _, a := range assets {
		body = append(body, 0, 0, 0, 0, 0, 0)
		body = append(body, a.assetType...)
		body = append(body, 0, 1, mmt.LocationTypeSameFlow, byte(a.packetID>>8), byte(a.packetID))
		descriptor := mmttest.Descriptor(mmt.DescriptorTagMHStreamIdentifier, []byte{byte(a.componentTag >> 8), byte(a.componentTag)})
		body = append(body, byte(len(descriptor)>>8), byte(len(descriptor)))
		body = append(body, descriptor...)
	}
	return mmttest.Table(mmt.TableIDMPT, 0, body)
}

// twoServiceStream carries services 1 and 2, whose video assets are packet
// 0x0100 and 0x0200, plus NTP and TLV-SI.
func twoServiceStream() []byte {
	var b streamBuilder
	b.message(mmt.PacketIDPA, mmttest.PAMessage(0, pltTable(testPackage{1, 0xFF01}, testPackage{2, 0xFF02})))
	b.message(0xFF01, mmttest.PAMessage(0, mptTable(1, testAsset{0x0100, "hev1", 0})))
	b.message(0xFF02, mmttest.PAMessage(0, mptTable(2, testAsset{0x0200, "hev1", 0})))
	for range 2 {
		b.media(0x0100, 0xA1)
		b.media(0x0200, 0xA2)
	}
	b.ntp()
	b.tlvSI(mmttest.Section(mmt.TableIDTLVNITActual, 0x000B, 0, 0, 0, append(mmttest.Loop(), mmttest.Loop()...)))
	return b.out
}

// mediaPackets counts the MPU packets of each packet_id and the NTP and
// TLV-SI packets in a stream.
func mediaPackets(t *testing.T, stream []byte) (media map[uint16]int, ntp, tlvSI int) {
	t.Helper()
	media = map[uint16]int{}
	reader := mmt.NewTLVReader(bytes.NewReader(stream))
	var flows mmt.FlowTracker
	for {
		p, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return media, ntp, tlvSI
		}
		if err != nil {
			t.Fatal(err)
		}
		if p.Type() == mmt.TLVPacketTypeTransmissionControl {
			tlvSI++
			continue
		}
		u, err := mmt.ParseUDPPacket(p)
		if err != nil {
			t.Fatal(err)
		}
		if !u.Compressed && u.Destination.Port() == ntpPort {
			ntp++
			continue
		}
		if _, ok := flows.Flow(u); !ok {
			t.Fatal("compressed packet of an unknown context")
		}
		m, err := mmt.ParseMMTPPacket(u.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if m.PayloadType == mmt.MMTPPayloadTypeMPU {
			media[m.PacketID]++
		}
	}
}

func TestServiceStreamExtractsOneOfSeveralServices(t *testing.T) {
	stream := twoServiceStream()
	session := testSession(source.NewBroadcast(newFiniteSource(stream), nil), nil)

	var out bytes.Buffer
	if err := session.ServiceStream(context.Background(), 1, false, &out); err != nil {
		t.Fatal(err)
	}
	media, ntp, tlvSI := mediaPackets(t, out.Bytes())
	if media[0x0100] != 2 || media[0x0200] != 0 {
		t.Fatalf("media packets = %v, want only service 1's two packets", media)
	}
	if ntp != 1 || tlvSI != 1 {
		t.Fatalf("NTP = %d, TLV-SI = %d, want both kept", ntp, tlvSI)
	}
}

func TestServiceStreamEndsForServiceMissingFromPLT(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource(twoServiceStream()), nil), nil)

	if err := session.ServiceStream(context.Background(), 9, false, io.Discard); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("error = %v, want ErrServiceNotFound", err)
	}
}

func sequenceDrops(monitor *sequenceMonitor, stream []byte) []fanout.Drop {
	reader := mmt.NewTLVReader(bytes.NewReader(stream))
	var drops []fanout.Drop
	for {
		p, err := reader.Next()
		if err != nil {
			return drops
		}
		if drop := monitor.Observe(p); drop != nil {
			drops = append(drops, *drop)
		}
	}
}

func TestSequenceMonitorDetectsLostPacket(t *testing.T) {
	var b streamBuilder
	for range 20 {
		b.media(0x0100, 0)
	}
	b.compressed++ // lose one packet
	b.media(0x0100, 0)
	b.ntp()
	drops := sequenceDrops(&sequenceMonitor{}, b.out)
	if len(drops) != 1 || drops[0].Sequence != "cid=1" || drops[0].Expected != 4 || drops[0].Actual != 5 {
		t.Fatalf("drops = %+v, want one on cid 1, expected 4 actual 5", drops)
	}
}

// TestSequenceMonitorIgnoresSkippedPacketSequenceNumber covers an observed
// real-world case: an asset's packet_sequence_number skips while no packet
// is lost.
func TestSequenceMonitorIgnoresSkippedPacketSequenceNumber(t *testing.T) {
	var b streamBuilder
	b.media(0xF140, 0)
	b.sequence[0xF140]++
	b.media(0xF140, 0)
	if drops := sequenceDrops(&sequenceMonitor{}, b.out); len(drops) != 0 {
		t.Fatalf("drops = %+v, want none", drops)
	}
}

func TestExtractingFilterStopsSubscriberDropDetection(t *testing.T) {
	stream := twoServiceStream()
	transport := newTransport()
	filter := transport.NewFilter(1).(*serviceFilter)
	monitor := filter.NewDropDetector().(*sequenceMonitor)
	var delivered []byte
	reader := mmt.NewTLVReader(bytes.NewReader(stream))
	for {
		p, err := reader.Next()
		if err != nil {
			break
		}
		if _, err := transport.Feed(p); err != nil {
			t.Fatal(err)
		}
		out, err := filter.Packet(p)
		if err != nil {
			t.Fatal(err)
		}
		delivered = append(delivered, out...)
	}
	if drops := sequenceDrops(monitor, delivered); len(drops) != 0 {
		t.Fatalf("drops = %+v, want none for packets the filter removed", drops)
	}
	if drops := sequenceDrops(&sequenceMonitor{}, delivered); len(drops) == 0 {
		t.Fatal("the extracted stream has no sequence gap; the test does not remove packets")
	}
}

// scanStream carries TLV-NIT with remote control keys and MH-SDT[actual]
// with two services, the second referring to the first's logo indirectly.
func scanStream() []byte {
	var b streamBuilder
	keys := mmttest.TLVDescriptor(mmt.TLVDescriptorTagRemoteControlKey, []byte{2, 1, 0, 1, 0xff, 0xff, 3, 0, 2, 0xff, 0xff})
	b.tlvSI(mmttest.Section(mmt.TableIDTLVNITActual, 0x000B, 1, 0, 0, append(mmttest.Loop(keys), mmttest.Loop()...)))
	service := func(serviceID uint16, name string, logo []byte) []byte {
		descriptors := mmttest.Descriptor(mmt.DescriptorTagMHService, append(append([]byte{0x01, 3}, "NHK"...), append([]byte{byte(len(name))}, name...)...))
		descriptors = append(descriptors, mmttest.Descriptor(mmt.DescriptorTagMHLogoTransmission, logo)...)
		entry := []byte{byte(serviceID >> 8), byte(serviceID), 0xe3, 0x80 | byte(len(descriptors)>>8), byte(len(descriptors))}
		return append(entry, descriptors...)
	}
	body := []byte{0x00, 0x0B, 0xff}
	body = append(body, service(1, "ＢＳ４Ｋ", []byte{mmt.LogoTransmissionTypeCDT1, 0, 5, 0, 2, 0, 9, mmt.LogoType2K, 0, 1})...)
	body = append(body, service(2, "ＢＳ８Ｋ", []byte{mmt.LogoTransmissionTypeCDT2, 0, 5})...)
	b.message(mmt.PacketIDMHSDT, mmttest.M2SectionMessage(0, mmttest.Section(mmt.TableIDMHSDTActual, 0xB110, 0, 0, 0, body)))
	return b.out
}

func TestScanServices(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource(scanStream()), nil), nil)

	services, err := session.ScanServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 {
		t.Fatalf("services = %+v, want 2", services)
	}
	for i, want := range []struct {
		id   uint16
		name string
		key  uint8
	}{{1, "ＢＳ４Ｋ", 1}, {2, "ＢＳ８Ｋ", 3}} {
		got := services[i]
		if got.Key != (model.ServiceKey{NetworkID: 0x000B, StreamID: 0xB110, ServiceID: want.id}) || got.Name != want.name || got.ProviderName != "NHK" || got.Type != 0x01 {
			t.Fatalf("service %d = %+v", i, got)
		}
		if !got.EITSchedule || !got.EITPresentFollow || got.RunningStatus != 4 {
			t.Fatalf("service %d flags = %+v", i, got)
		}
		if got.RemoteControlKey == nil || *got.RemoteControlKey != want.key {
			t.Fatalf("service %d remote control key = %v, want %d", i, got.RemoteControlKey, want.key)
		}
		if got.Logo == nil || got.Logo.LogoID != 5 || got.Logo.Version == nil || *got.Logo.Version != 2 || got.Logo.DownloadDataID == nil || *got.Logo.DownloadDataID != 9 {
			t.Fatalf("service %d logo = %+v, want logo 5 version 2 download 9", i, got.Logo)
		}
	}
}

type recordingUpdater struct {
	mu     sync.Mutex
	events []model.Event
}

func (u *recordingUpdater) UpsertEvents(_ context.Context, events []model.Event) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.events = append(u.events, events...)
	return nil
}

func (u *recordingUpdater) snapshot() []model.Event {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]model.Event(nil), u.events...)
}

// presentEventStream carries service 1's MPT with an HEVC asset of
// component tag 0 and its MH-EIT[p/f] present event.
func presentEventStream(start time.Time) []byte {
	var b streamBuilder
	b.message(0xFF01, mmttest.PAMessage(0, mptTable(1, testAsset{0x0100, "hev1", 0})))
	name := "番組"
	short := append(append([]byte("jpn"), byte(len(name))), name...)
	short = append(short, 0, 0)
	video := append([]byte{6<<4 | 3, 0x80 | 8, 0, 0, 5 << 4}, "jpn"...)
	descriptors := append(mmttest.Descriptor(mmt.DescriptorTagMHShortEvent, short), mmttest.Descriptor(mmt.DescriptorTagVideoComponent, video)...)
	event := []byte{0x12, 0x34}
	event = append(event, mmttest.MJDTime(start)...)
	event = append(event, mmttest.BCDDuration(30*time.Minute)...)
	event = append(event, 0x80|byte(len(descriptors)>>8), byte(len(descriptors)))
	event = append(event, descriptors...)
	body := append([]byte{0xB1, 0x10, 0x00, 0x0B, 0, mmt.TableIDMHEITPF}, event...)
	b.message(mmt.PacketIDMHEIT, mmttest.M2SectionMessage(0, mmttest.Section(mmt.TableIDMHEITPF, 1, 0, 0, 1, body)))
	return b.out
}

func TestPresentEventsReachUpdaterWithCodec(t *testing.T) {
	start := time.Date(2026, 9, 23, 21, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	updater := &recordingUpdater{}
	session := NewSession(Config{
		Channel:      "101",
		Type:         "BS4K",
		Broadcast:    source.NewBroadcast(&hangSource{prefix: presentEventStream(start)}, nil),
		EventUpdater: updater,
	})
	ctx, cancel := context.WithCancel(context.Background())
	streamDone := make(chan error, 1)
	go func() { streamDone <- session.ChannelStream(ctx, false, io.Discard) }()
	defer func() {
		cancel()
		if err := <-streamDone; err != nil {
			t.Error(err)
		}
	}()
	var events []model.Event
	for deadline := time.Now().Add(2 * time.Second); len(events) == 0 && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		events = updater.snapshot()
	}
	if len(events) != 1 {
		t.Fatalf("events = %+v, want the present event", events)
	}
	got := events[0]
	if got.Key != (model.ServiceKey{NetworkID: 0x000B, StreamID: 0xB110, ServiceID: 1}) || got.EventID != 0x1234 || got.Name != "番組" {
		t.Fatalf("event = %+v", got)
	}
	if got.StartAt == nil || *got.StartAt != start.UnixMilli() || got.DurationMS == nil || *got.DurationMS != 30*60*1000 {
		t.Fatalf("event timing = %v %v", got.StartAt, got.DurationMS)
	}
	want := model.VideoComponent{Codec: model.VideoCodecH265, Resolution: model.VideoResolution2160p, Aspect: model.VideoAspect16x9NoPanVector, Progressive: true, FrameRateNumer: 60000, FrameRateDenom: 1001, HasFrameRate: true, Transfer: model.TransferHLG, Language: "jpn"}
	if len(got.Videos) != 1 || got.Videos[0] != want {
		t.Fatalf("videos = %+v, want %+v", got.Videos, want)
	}
}

func programEvent(eventID uint16, duration time.Duration) model.Event {
	start := time.Now().UnixMilli()
	ms := int(duration / time.Millisecond)
	return model.Event{Key: model.ServiceKey{NetworkID: 0x000B, StreamID: 0xB110, ServiceID: 1}, EventID: eventID, StartAt: &start, DurationMS: &ms}
}

func TestProgramStreamPassesPacketsWhileEventIsPresent(t *testing.T) {
	session := NewSession(Config{
		Channel:   "101",
		Type:      "BS4K",
		Broadcast: source.NewBroadcast(&hangSource{prefix: presentEventStream(time.Now())}, nil),
	})
	ctx, cancel := context.WithCancel(context.Background())
	var out lockedBuffer
	done := make(chan error, 1)
	go func() { done <- session.ProgramStream(ctx, programEvent(0x1234, time.Minute), false, &out) }()
	deadline := time.Now().Add(2 * time.Second)
	for out.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("no packets while the requested event was on air")
	}
	if out.Bytes()[0] != mmt.TLVSyncByte {
		t.Fatalf("first byte = 0x%02X, want the TLV sync byte", out.Bytes()[0])
	}
}

func TestProgramStreamWithholdsOtherEvents(t *testing.T) {
	session := NewSession(Config{
		Channel:   "101",
		Type:      "BS4K",
		Broadcast: source.NewBroadcast(&hangSource{prefix: presentEventStream(time.Now())}, nil),
	})
	var out lockedBuffer
	if err := session.ProgramStream(context.Background(), programEvent(0x4321, 100*time.Millisecond), false, &out); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("delivered %d bytes for an event that never aired", out.Len())
	}
}

// liveRemoteClient serves a TLV stream that never ends and records whether
// it was asked for the decoded variant.
type liveRemoteClient struct {
	prefix  []byte
	decoded atomic.Bool
}

func (c *liveRemoteClient) CheckAvailableForRoute(context.Context, string, string) error { return nil }

func (c *liveRemoteClient) ChannelStream(ctx context.Context, _, _ string, decode bool, dst io.Writer) error {
	c.decoded.Store(decode)
	if _, err := dst.Write(c.prefix); err != nil {
		return err
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := dst.Write(tlvBytes(0xAA)); err != nil {
				return err
			}
		}
	}
}

func TestProgramStreamOverRemoteHandleAsksForDecodedStream(t *testing.T) {
	client := &liveRemoteClient{prefix: presentEventStream(time.Now())}
	channel := config.ChannelConfig{Type: "BS4K", Channel: "101", Transport: config.TransportTLV}
	handle := source.NewRemoteInputHandle(client, channel, channel, "living", "BS4K")
	session := NewSession(Config{Channel: "101", Type: "BS4K", Handle: handle})

	ctx, cancel := context.WithCancel(context.Background())
	var out lockedBuffer
	done := make(chan error, 1)
	go func() { done <- session.ProgramStream(ctx, programEvent(0x1234, time.Minute), true, &out) }()
	deadline := time.Now().Add(2 * time.Second)
	for out.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if out.Len() == 0 {
		t.Fatal("no packets from the remote while the requested event was on air")
	}
	if !client.decoded.Load() {
		t.Fatal("decode=1 did not ask the remote for the decoded stream")
	}
}
