package channel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/ts"
)

func TestSessionSectionUpdaterIgnoresScheduleEIT(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	for range sectionQueueSize + 1 {
		session.observeSection(ts.Section{ts.TableIDEITSStart})
	}
	if got := len(session.sectionQueue); got != 0 {
		t.Fatalf("section updater queue length = %d, want 0 for schedule EIT", got)
	}

	session.observeSection(ts.Section{ts.TableIDEITPF0})
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want EIT p/f to be queued", got)
	}
}

func TestSessionSectionUpdaterRoutesCommonLogoSections(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		logoUpdater:   noopLogoUpdater{},
		logoCarousel:  ts.NewDSMCCLogoCarousel(),
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	session.observeSection(ts.Section{ts.TableIDCDT})
	session.observeSection(ts.Section{ts.TableIDSDTT})
	if got := len(session.sectionQueue); got != 2 {
		t.Fatalf("section updater queue length = %d, want CDT and SDTT to be queued", got)
	}
	if got := len(session.carouselQueue); got != 0 {
		t.Fatalf("carousel updater queue length = %d, want 0 before carousel sections", got)
	}

	dii := streamBuildDSMCCDII(t, 1, 16, 2, 4, 1, []byte("LOGO-05"))
	ddb := streamBuildDSMCCDDB(t, 1, 2, 1, 0, []byte{1, 2, 3, 4})
	session.observeSection(dii)
	session.observeSection(ddb)
	if got := len(session.carouselQueue); got != 2 {
		t.Fatalf("carousel updater queue length = %d, want DII and DDB to be queued", got)
	}
	if got := len(session.sectionQueue); got != 2 {
		t.Fatalf("section updater queue length = %d, want unchanged after carousel sections", got)
	}
}

func TestSessionSectionUpdaterIgnoresUnrelatedDSMCCCarousel(t *testing.T) {
	session := &Session{
		channel:       "27",
		typ:           "user-defined",
		logoUpdater:   noopLogoUpdater{},
		logoCarousel:  ts.NewDSMCCLogoCarousel(),
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	session.observeSection(streamBuildDSMCCDII(t, 1, 16, 2, 4, 1, []byte("index.bml")))
	for i := range carouselQueueSize + 1 {
		session.observeSection(streamBuildDSMCCDDB(t, 1, 2, 1, uint16(i), []byte{1, 2, 3, 4}))
	}
	if got := len(session.carouselQueue); got != 0 {
		t.Fatalf("carousel updater queue length = %d, want unrelated data carousel ignored", got)
	}
}

func TestSessionSectionUpdaterCarouselOverflowDoesNotBlockSections(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		logoUpdater:   noopLogoUpdater{},
		logoCarousel:  ts.NewDSMCCLogoCarousel(),
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	session.observeSection(streamBuildDSMCCDII(t, 1, 16, 2, 4, 1, []byte("LOGO-05")))
	for range carouselQueueSize + 1 {
		session.observeSection(streamBuildDSMCCDDB(t, 1, 2, 1, 0, []byte{1, 2, 3, 4}))
	}
	if got := len(session.carouselQueue); got != carouselQueueSize {
		t.Fatalf("carousel updater queue length = %d, want capped at %d", got, carouselQueueSize)
	}

	session.observeSection(ts.Section{ts.TableIDEITPF0})
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want EIT p/f unaffected by carousel overflow", got)
	}
}

func TestSessionSectionUpdaterCoalescesRepeatedEITPF(t *testing.T) {
	key := epgClockTestKey{networkID: 4, serviceID: 101}
	section := streamBuildEIT(ts.TableIDEITPF0, key, 10)
	session := &Session{
		channel:      "BS01_0",
		typ:          "BS",
		sectionQueue: make(chan ts.Section, sectionQueueSize),
	}

	for range sectionQueueSize + 1 {
		session.observeSection(section)
	}
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want repeated EIT p/f coalesced to 1", got)
	}

	session.observeSection(streamBuildEIT(ts.TableIDEITPF0, key, 11))
	if got := len(session.sectionQueue); got != 2 {
		t.Fatalf("section updater queue length = %d, want changed EIT p/f to be queued", got)
	}
}

func TestSessionDataBroadcastDDBQueueIsBounded(t *testing.T) {
	session := &Session{
		channel:            "27",
		typ:                "GR",
		dataBroadcast:      databroadcast.NewDataBroadcastHub(),
		dataBroadcastQueue: make(chan ts.PIDSection, 1),
	}
	ddb := streamBuildDSMCCDDB(t, 1, 2, 1, 0, []byte("block"))
	section := ts.PIDSection{PID: 0x0200, Section: ddb}

	session.observePIDSection(section)
	session.observePIDSection(section)

	if got := len(session.dataBroadcastQueue); got != 1 {
		t.Fatalf("DDB queue length = %d, want 1", got)
	}
	// Balance the wait group for the accepted item because this focused test
	// intentionally does not start the worker.
	<-session.dataBroadcastQueue
	session.dataBroadcastWG.Done()
}

func TestSessionPrioritizesEntryDocumentDDB(t *testing.T) {
	serviceID, pmtPID, carouselPID := uint16(101), uint16(0x0100), uint16(0x0200)
	componentTag := byte(0x40)
	hub := databroadcast.NewDataBroadcastHub()
	hub.Observe(ts.PIDSection{PID: pmtPID, Section: streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag)})
	moduleInfo := []byte{ts.DSMCCModuleDescriptorName, 9, 'i', 'n', 'd', 'e', 'x', '.', 'b', 'm', 'l'}
	hub.Observe(ts.PIDSection{PID: carouselPID, Section: streamBuildDSMCCDII(t, 1, 4, 2, 4, 1, moduleInfo)})
	session := &Session{
		channel:                    "27",
		typ:                        "GR",
		dataBroadcast:              hub,
		dataBroadcastQueue:         make(chan ts.PIDSection, 1),
		dataBroadcastPriorityQueue: make(chan ts.PIDSection, 1),
	}

	session.observePIDSection(ts.PIDSection{PID: carouselPID, Section: streamBuildDSMCCDDB(t, 1, 2, 1, 0, []byte("bml!"))})

	if got := len(session.dataBroadcastPriorityQueue); got != 1 {
		t.Fatalf("priority DDB queue length = %d, want 1", got)
	}
	if got := len(session.dataBroadcastQueue); got != 0 {
		t.Fatalf("normal DDB queue length = %d, want 0", got)
	}
	<-session.dataBroadcastPriorityQueue
	session.dataBroadcastWG.Done()
}

func TestSessionRestoresDataBroadcastModuleAcrossSessions(t *testing.T) {
	serviceID, pmtPID, carouselPID := uint16(101), uint16(0x0100), uint16(0x0200)
	componentTag := byte(0x40)
	moduleData := []byte("bml")
	cache := databroadcast.NewModuleCache(1024)
	base := append(streamSectionPackets(ts.PIDPAT, streamBuildPAT(1, serviceID, pmtPID), 0), streamSectionPackets(pmtPID, streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag), 1)...)
	base = append(base, streamSectionPackets(carouselPID, streamBuildDSMCCDII(t, 1, uint16(len(moduleData)), 2, uint32(len(moduleData)), 1, []byte("index.bml")), 2)...)
	firstInput := append(append([]byte(nil), base...), streamSectionPackets(carouselPID, streamBuildDSMCCDDB(t, 1, 2, 1, 0, moduleData), 3)...)
	first := NewSession(Config{Broadcast: source.NewBroadcast(streamtest.NewFinitePacketSource(firstInput, streamtest.ClosedStart()), nil), Channel: "27", Type: "GR", ModuleCache: cache})
	if err := first.ObserveDataBroadcast(t.Context(), serviceID, false, func(databroadcast.DataBroadcastEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}

	second := NewSession(Config{Broadcast: source.NewBroadcast(streamtest.NewFinitePacketSource(base, streamtest.ClosedStart()), nil), Channel: "27", Type: "GR", ModuleCache: cache})
	restored := false
	if err := second.ObserveDataBroadcast(t.Context(), serviceID, false, func(event databroadcast.DataBroadcastEvent) error {
		if event.Type == "moduleUpdated" && event.Module != nil && event.Module.ModuleID == 2 {
			restored = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !restored {
		t.Fatal("second session did not restore completed module from cache")
	}
	module, ok := second.DataBroadcastModule(serviceID, componentTag, 2)
	if !ok || string(module.Data) != string(moduleData) {
		t.Fatalf("restored module = %q, %v", module.Data, ok)
	}
}

func TestSessionSectionUpdaterRetriesEITPFOnUpsertFailure(t *testing.T) {
	key := epgClockTestKey{networkID: 4, serviceID: 101}
	section := streamBuildEIT(ts.TableIDEITPF0, key, 10)
	session := &Session{
		channel:      "BS01_0",
		typ:          "BS",
		eitUpdater:   failingEITUpdater{},
		sectionQueue: make(chan ts.Section, sectionQueueSize),
	}

	session.observeSection(section)
	queued := <-session.sectionQueue
	session.updateSection(t.Context(), queued)
	session.observeSection(section)
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want failed EIT p/f to be retried", got)
	}
}

func TestChannelSessionCollectEITWithClockUsesLatestTOT(t *testing.T) {
	clock := time.Date(2026, 6, 29, 12, 34, 56, 0, time.FixedZone("JST", 9*60*60))
	key := epgClockTestKey{networkID: 4, serviceID: 101}
	input := append(streamSectionPackets(ts.PIDTOT, streamBuildTOT(clock), 0), streamSectionPackets(ts.PIDEIT, streamBuildEIT(ts.TableIDEITSStart, key, 10), 1)...)
	session := NewSession(Config{
		Broadcast: source.NewBroadcast(streamtest.NewFinitePacketSource(input, streamtest.ClosedStart()), nil),
		Channel:   "27",
		Type:      "GR",
	})

	var gotClock time.Time
	var gotEventID uint16
	err := session.CollectEITWithClock(t.Context(), func(eit *ts.EIT, observedClock time.Time) error {
		gotClock = observedClock
		if len(eit.Events) > 0 {
			gotEventID = eit.Events[0].EventID
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gotClock.Equal(clock) {
		t.Fatalf("clock = %s, want %s", gotClock, clock)
	}
	if gotEventID != 10 {
		t.Fatalf("event id = %d, want 10", gotEventID)
	}
}

func TestSessionObserveDataBroadcastEmitsSnapshotAndModule(t *testing.T) {
	serviceID := uint16(101)
	pmtPID := uint16(0x0100)
	carouselPID := uint16(0x0200)
	componentTag := byte(0x40)
	moduleData := []byte("bml")
	input := append(streamSectionPackets(ts.PIDPAT, streamBuildPAT(1, serviceID, pmtPID), 0), streamSectionPackets(pmtPID, streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag), 1)...)
	input = append(input, streamSectionPackets(pmtPID, streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag), 2)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDII(t, 1, uint16(len(moduleData)), 2, uint32(len(moduleData)), 1, []byte("index.bml")), 3)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDII(t, 1, uint16(len(moduleData)), 2, uint32(len(moduleData)), 1, []byte("index.bml")), 4)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDDB(t, 1, 2, 1, 0, moduleData), 5)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDDB(t, 1, 2, 1, 0, moduleData), 6)...)
	session := NewSession(Config{
		Broadcast: source.NewBroadcast(streamtest.NewFinitePacketSource(input, streamtest.ClosedStart()), nil),
		Channel:   "27",
		Type:      "GR",
	})

	var events []databroadcast.DataBroadcastEvent
	err := session.ObserveDataBroadcast(t.Context(), serviceID, false, func(event databroadcast.DataBroadcastEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[0].Type != "snapshot" {
		t.Fatalf("first event = %#v, want snapshot", events)
	}
	var gotModule *databroadcast.DataBroadcastModule
	for i := range events {
		if events[i].Type == "moduleUpdated" {
			gotModule = events[i].Module
			break
		}
	}
	if gotModule == nil {
		t.Fatalf("events did not include moduleUpdated: %#v", events)
	}
	for _, eventType := range []string{"pmt", "moduleListUpdated", "moduleUpdated"} {
		count := 0
		for _, event := range events {
			if event.Type == eventType {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%s event count = %d, want 1; events: %#v", eventType, count, events)
		}
	}
	if gotModule.ComponentTag != componentTag || gotModule.ModuleID != 2 {
		t.Fatalf("module = componentTag:%#x moduleID:%#x", gotModule.ComponentTag, gotModule.ModuleID)
	}
	stored, ok := session.DataBroadcastModule(serviceID, componentTag, 2)
	if !ok {
		t.Fatal("module was not retained in live session")
	}
	if string(stored.Data) != string(moduleData) {
		t.Fatalf("module data = %q, want %q", stored.Data, moduleData)
	}
}

type epgClockTestKey struct {
	networkID uint16
	serviceID uint16
}

type failingEITUpdater struct{}

func (failingEITUpdater) UpsertEIT(context.Context, *ts.EIT) error {
	return errors.New("upsert failed")
}

type noopLogoUpdater struct{}

func (noopLogoUpdater) UpsertLogoImage(context.Context, *ts.LogoImage) error { return nil }
func (noopLogoUpdater) UpsertCommonLogoImage(context.Context, ts.CommonLogoImage) error {
	return nil
}
func (noopLogoUpdater) UpsertCommonDataAnnouncement(context.Context, ts.CommonDataAnnouncement, string, string) error {
	return nil
}

func streamBuildTOT(jstTime time.Time) ts.Section {
	encodedTime := streamEncodeMJDTime(jstTime)
	length := 5 + 2 + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDTOT
	s[1] = 0x70 | byte(length>>8)
	s[2] = byte(length)
	copy(s[3:8], encodedTime)
	s[8] = 0xf0
	s[9] = 0
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamBuildPAT(tsid, serviceID, pmtPID uint16) ts.Section {
	length := 5 + 4 + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDPAT
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(tsid >> 8)
	s[4] = byte(tsid)
	s[5] = 0xc1
	s[8] = byte(serviceID >> 8)
	s[9] = byte(serviceID)
	s[10] = 0xe0 | byte(pmtPID>>8)
	s[11] = byte(pmtPID)
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamBuildDataBroadcastPMT(serviceID, carouselPID uint16, componentTag byte) ts.Section {
	esInfo := []byte{0x52, 0x01, componentTag}
	length := 9 + 5 + len(esInfo) + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDPMT
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5] = 0xc1
	s[8] = 0x1f
	s[9] = 0xff
	off := 12
	s[off] = ts.StreamTypeDSMCCDataCarousel
	s[off+1] = 0xe0 | byte(carouselPID>>8)
	s[off+2] = byte(carouselPID)
	s[off+3] = 0xf0 | byte(len(esInfo)>>8)
	s[off+4] = byte(len(esInfo))
	copy(s[off+5:], esInfo)
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamBuildDSMCCDII(t *testing.T, downloadID uint32, blockSize, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte) ts.Section {
	t.Helper()
	body := []byte{
		byte(downloadID >> 24), byte(downloadID >> 16), byte(downloadID >> 8), byte(downloadID),
		byte(blockSize >> 8), byte(blockSize),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
		0, 1,
		byte(moduleID >> 8), byte(moduleID),
		byte(moduleSize >> 24), byte(moduleSize >> 16), byte(moduleSize >> 8), byte(moduleSize),
		moduleVersion,
		byte(len(moduleInfo)),
	}
	body = append(body, moduleInfo...)
	return streamBuildDSMCCSection(ts.TableIDDSMCCDII, 0x1002, 1, body)
}

func streamBuildDSMCCDDB(t *testing.T, downloadID uint32, moduleID uint16, moduleVersion byte, blockNumber uint16, data []byte) ts.Section {
	t.Helper()
	body := []byte{byte(moduleID >> 8), byte(moduleID), moduleVersion, 0, byte(blockNumber >> 8), byte(blockNumber)}
	body = append(body, data...)
	return streamBuildDSMCCSection(ts.TableIDDSMCCDDB, 0x1003, downloadID, body)
}

func streamBuildDSMCCSection(tableID byte, messageID uint16, headerID uint32, body []byte) ts.Section {
	message := []byte{0x11, 0x03, byte(messageID >> 8), byte(messageID), byte(headerID >> 24), byte(headerID >> 16), byte(headerID >> 8), byte(headerID), 0xff, 0}
	message = append(message, byte(len(body)>>8), byte(len(body)))
	message = append(message, body...)
	length := 5 + len(message) + 4
	s := make([]byte, 3+length)
	s[0] = tableID
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = 0
	s[4] = 1
	s[5] = 0xc1
	copy(s[8:], message)
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamBuildEIT(tableID byte, key epgClockTestKey, eventID uint16) ts.Section {
	length := 11 + 12 + 4
	s := make([]byte, 3+length)
	s[0] = tableID
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(key.serviceID >> 8)
	s[4] = byte(key.serviceID)
	s[5] = 0xc1
	s[8] = 0
	s[9] = 1
	s[10] = byte(key.networkID >> 8)
	s[11] = byte(key.networkID)
	s[12] = 0
	s[13] = tableID
	off := 14
	s[off] = byte(eventID >> 8)
	s[off+1] = byte(eventID)
	copy(s[off+2:off+7], streamEncodeMJDTime(time.Date(2026, 6, 29, 13, 0, 0, 0, time.FixedZone("JST", 9*60*60))))
	copy(s[off+7:off+10], []byte{0x00, 0x30, 0x00})
	s[off+10] = 0x80
	s[off+11] = 0
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamSectionPackets(pid uint16, section ts.Section, counter byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, ts.PacketSize)
	packet[0] = ts.SyncByte
	packet[1] = 0x40 | byte(pid>>8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | counter&0x0f
	packet[4] = 0
	copy(packet[5:], section)
	return packet
}

func streamEncodeMJDTime(t time.Time) []byte {
	jst := time.FixedZone("JST", 9*60*60)
	t = t.In(jst)
	mjd := streamMJDFromDate(t)
	return []byte{byte(mjd >> 8), byte(mjd), streamEncodeBCD(t.Hour()), streamEncodeBCD(t.Minute()), streamEncodeBCD(t.Second())}
}

func streamMJDFromDate(t time.Time) int {
	y := t.Year() - 1900
	m := int(t.Month())
	d := t.Day()
	l := 0
	if m == 1 || m == 2 {
		l = 1
	}
	return 14956 + d + int(float64(y-l)*365.25) + int(float64(m+1+l*12)*30.6001)
}

func streamEncodeBCD(v int) byte {
	return byte((v/10)<<4 | (v % 10))
}

func streamWriteCRC(s []byte) {
	crc := streamCRC32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func streamCRC32MPEG2(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func TestSharedSessionUsesOneDescramblerForDecodedSubscribers(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	packetSource := streamtest.NewFinitePacketSource(bytes.Repeat(packet, 4), start)
	descrambler := &passthroughDescrambler{}
	session := NewSession(Config{
		Broadcast:   source.NewBroadcast(packetSource, nil),
		Channel:     "27",
		Descrambler: descrambler,
		OnStop:      func() {},
		Type:        "GR",
	})

	var first, second bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- session.ChannelStream(t.Context(), true, &first) }()
	go func() { errs <- session.ChannelStream(t.Context(), true, &second) }()
	if !streamtest.Eventually(time.Second, func() bool { return session.decodedDemuxer.PacketSubscriberCount() == 2 }) {
		t.Fatal("decoded subscribers did not reach 2")
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if descrambler.starts.Load() != 1 {
		t.Fatalf("descrambler starts = %d, want 1", descrambler.starts.Load())
	}
	if first.Len() != 4*ts.PacketSize || second.Len() != 4*ts.PacketSize {
		t.Fatalf("decoded subscriber bytes = %d/%d", first.Len(), second.Len())
	}
}

func TestSessionStopsSectionUpdatesWhenRawDemuxerStops(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	packetSource := streamtest.NewFinitePacketSource(packet, streamtest.ClosedStart())
	var stopped atomic.Bool
	session := NewSession(Config{
		Broadcast: source.NewBroadcast(packetSource, nil),
		Channel:   "27",
		OnStop:    func() { stopped.Store(true) },
		Type:      "GR",
	})

	var out bytes.Buffer
	if err := session.ChannelStream(t.Context(), false, &out); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.sectionDone:
	case <-time.After(time.Second):
		t.Fatal("section updater did not stop after raw demuxer stopped")
	}
	if !streamtest.Eventually(time.Second, stopped.Load) {
		t.Fatal("session onStop callback was not called")
	}
}

// decodedSubscriberCount and decodedDemuxerStopped read session.decodedDemuxer
// under the session mutex, since attachDemuxer may concurrently replace that
// field with a freshly recreated demuxer (see the decoded-demuxer revival
// logic in attachDemuxer).
func decodedSubscriberCount(s *Session) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decodedDemuxer.PacketSubscriberCount()
}

func decodedDemuxerStopped(s *Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decodedDemuxer.Stopped()
}

func TestSessionRecreatesDecodedDemuxerAfterAllDecodedSubscribersDetach(t *testing.T) {
	packetSource := newInfiniteRepeatingSource(streamtest.TestPacket(0x0100, 1))
	descrambler := &passthroughDescrambler{}
	session := NewSession(Config{
		Broadcast:   source.NewBroadcast(packetSource, nil),
		Channel:     "27",
		Descrambler: descrambler,
		OnStop:      func() {},
		Type:        "GR",
	})
	t.Cleanup(func() { _ = session.Stop(context.Background()) })

	// Keep a raw subscriber attached throughout so only the decoded demuxer,
	// not the whole session, goes through a stop/empty cycle below.
	rawCtx, rawCancel := context.WithCancel(context.Background())
	t.Cleanup(rawCancel)
	rawDone := make(chan error, 1)
	go func() { rawDone <- session.ChannelStream(rawCtx, false, io.Discard) }()
	if !streamtest.Eventually(time.Second, func() bool { return session.rawDemuxer.PacketSubscriberCount() >= 1 }) {
		t.Fatal("raw subscriber did not attach")
	}

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- session.ChannelStream(firstCtx, true, io.Discard) }()
	if !streamtest.Eventually(time.Second, func() bool { return decodedSubscriberCount(session) == 1 }) {
		t.Fatal("first decoded subscriber did not attach")
	}

	firstCancel()
	if err := <-firstDone; err != nil {
		t.Fatalf("first decoded stream error = %v, want nil", err)
	}
	if !streamtest.Eventually(time.Second, func() bool { return decodedDemuxerStopped(session) }) {
		t.Fatal("decoded demuxer did not stop after its last subscriber detached")
	}

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- session.ChannelStream(secondCtx, true, io.Discard) }()
	if !streamtest.Eventually(time.Second, func() bool { return decodedSubscriberCount(session) == 1 }) {
		t.Fatal("second decoded subscriber did not attach to a fresh demuxer")
	}
	secondCancel()
	if err := <-secondDone; err != nil {
		t.Fatalf("second decoded stream error = %v, want nil", err)
	}

	rawCancel()
	if err := <-rawDone; err != nil {
		t.Fatalf("raw stream error = %v, want nil", err)
	}
}

func TestStoppingSessionDoesNotStopSharedInputPeer(t *testing.T) {
	packetSource := newInfiniteRepeatingSource(streamtest.TestPacket(0x0100, 1))
	broadcast := source.NewBroadcast(packetSource, nil)
	first := NewChannelSession(Config{Broadcast: broadcast, Channel: "27", Type: "GR"})
	second := NewChannelSession(Config{Broadcast: broadcast, Channel: "28", Type: "GR"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- first.ChannelStream(ctx, false, io.Discard) }()
	go func() { secondDone <- second.ChannelStream(ctx, false, io.Discard) }()
	if !streamtest.Eventually(time.Second, func() bool {
		return first.rawDemuxer.PacketSubscriberCount() == 1 && second.rawDemuxer.PacketSubscriberCount() == 1
	}) {
		t.Fatal("peer sessions did not attach")
	}

	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-packetSource.done:
		t.Fatal("stopping one session stopped the shared physical input")
	default:
	}
	if second.rawDemuxer.Stopped() {
		t.Fatal("stopping one session stopped its peer demuxer")
	}

	cancel()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

// infiniteRepeatingSource is a LiveSource that keeps writing the same packet
// until its context is canceled, used to keep a broadcast alive across
// attach/detach cycles in tests.
type infiniteRepeatingSource struct {
	packet []byte
	done   chan struct{}
}

func newInfiniteRepeatingSource(packet []byte) *infiniteRepeatingSource {
	return &infiniteRepeatingSource{packet: packet, done: make(chan struct{})}
}

func (s *infiniteRepeatingSource) Start(ctx context.Context, dst io.Writer) error {
	go func() {
		defer close(s.done)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, err := dst.Write(s.packet); err != nil {
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	return nil
}

func (*infiniteRepeatingSource) Stop(context.Context) error { return nil }
func (s *infiniteRepeatingSource) Done() <-chan struct{}    { return s.done }
func (*infiniteRepeatingSource) Err() error                 { return nil }
func (*infiniteRepeatingSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}

type passthroughDescrambler struct {
	starts atomic.Int32
}

func (d *passthroughDescrambler) Descramble(_ context.Context, src io.Reader, dst io.Writer) error {
	d.starts.Add(1)
	_, err := io.Copy(dst, src)
	return err
}
