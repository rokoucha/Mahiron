package channel

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/ts"
)

type countingSnapshotStore struct {
	puts int
	last databroadcast.PersistedService
}

func (s *countingSnapshotStore) PutSnapshot(_, _ string, service databroadcast.PersistedService) error {
	s.puts++
	s.last = service
	return nil
}

func (s *countingSnapshotStore) GetSnapshot(_, _ string, _ uint16) (databroadcast.PersistedService, bool) {
	return databroadcast.PersistedService{}, false
}

func TestSessionFlushDataBroadcastSnapshotsSkipsUnchangedState(t *testing.T) {
	serviceID, pmtPID, carouselPID := uint16(101), uint16(0x0100), uint16(0x0200)
	componentTag := byte(0x40)
	hub := databroadcast.NewDataBroadcastHub()
	hub.Observe(ts.PIDSection{PID: pmtPID, Section: streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag)})
	hub.Observe(ts.PIDSection{PID: carouselPID, Section: streamBuildDSMCCDII(t, 1, 4, 2, 4, 1, []byte("index.bml"))})

	store := &countingSnapshotStore{}
	session := &Session{
		channel:                "27",
		typ:                    "GR",
		dataBroadcast:          hub,
		snapshotStore:          store,
		lastPersistedSnapshots: map[uint16]databroadcast.PersistedService{},
	}

	session.flushDataBroadcastSnapshots()
	session.flushDataBroadcastSnapshots()
	if store.puts != 1 {
		t.Fatalf("PutSnapshot calls = %d, want 1 for repeated unchanged state", store.puts)
	}
	if store.last.ServiceID != serviceID || len(store.last.Carousels) != 1 {
		t.Fatalf("persisted service = %#v", store.last)
	}

	// A DII version bump changes the observed bytes, so the next flush must
	// write again.
	hub.Observe(ts.PIDSection{PID: carouselPID, Section: streamBuildDSMCCDII(t, 1, 4, 2, 4, 2, []byte("index.bml"))})
	session.flushDataBroadcastSnapshots()
	if store.puts != 2 {
		t.Fatalf("PutSnapshot calls = %d, want 2 after DII changed", store.puts)
	}
}

func TestSessionFlushDataBroadcastSnapshotsNoopWithoutStore(t *testing.T) {
	hub := databroadcast.NewDataBroadcastHub()
	hub.Observe(ts.PIDSection{PID: 0x0100, Section: streamBuildDataBroadcastPMT(101, 0x0200, 0x40)})
	session := &Session{
		channel:                "27",
		typ:                    "GR",
		dataBroadcast:          hub,
		lastPersistedSnapshots: map[uint16]databroadcast.PersistedService{},
	}
	// Must not panic when no snapshot store is configured.
	session.flushDataBroadcastSnapshots()
}

func TestSessionPersistsAndRestoresProvisionalDataBroadcastSnapshot(t *testing.T) {
	serviceID, pmtPID, carouselPID := uint16(101), uint16(0x0100), uint16(0x0200)
	componentTag := byte(0x40)
	moduleData := []byte("bml")
	moduleInfo := []byte{ts.DSMCCModuleDescriptorName, 9, 'i', 'n', 'd', 'e', 'x', '.', 'b', 'm', 'l'}

	store, err := databroadcast.NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	input := append(streamSectionPackets(ts.PIDPAT, streamBuildPAT(1, serviceID, pmtPID), 0), streamSectionPackets(pmtPID, streamBuildDataBroadcastPMT(serviceID, carouselPID, componentTag), 1)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDII(t, 1, uint16(len(moduleData)), 2, uint32(len(moduleData)), 1, moduleInfo), 2)...)
	input = append(input, streamSectionPackets(carouselPID, streamBuildDSMCCDDB(t, 1, 2, 1, 0, moduleData), 3)...)

	session := NewSession(Config{
		Broadcast:     source.NewBroadcast(streamtest.NewFinitePacketSource(input, streamtest.ClosedStart()), nil),
		Channel:       "27",
		Type:          "GR",
		ModuleStore:   store,
		SnapshotStore: store,
	})
	if err := session.ObserveDataBroadcast(t.Context(), serviceID, false, func(databroadcast.DataBroadcastEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}

	live := session.DataBroadcastSnapshot(serviceID)

	// The finite source draining triggers the session's own auto-stop, which
	// runs the snapshot worker's final flush on its own goroutine. Poll
	// instead of calling flushDataBroadcastSnapshots directly here: that
	// method is only safe from its owning goroutine, and the auto-stop flush
	// races with a second, test-issued call to it.
	var persisted databroadcast.PersistedService
	var ok bool
	if !streamtest.Eventually(time.Second, func() bool {
		persisted, ok = store.GetSnapshot("GR", "27", serviceID)
		return ok
	}) {
		t.Fatal("snapshot was not persisted after the session drained its input")
	}
	if persisted.StoredAt == 0 {
		t.Fatal("persisted snapshot has no stored-at timestamp")
	}

	restored := databroadcast.RestoreSnapshot("GR", "27", persisted, store)
	if restored.ProgramInfo != nil || restored.CurrentTime != nil || restored.PCR != nil {
		t.Fatalf("provisional snapshot carried clock state: %#v", restored)
	}
	if len(restored.Components) != len(live.Components) || len(restored.Components) != 1 {
		t.Fatalf("component count = %d, want 1 matching the live snapshot (%d)", len(restored.Components), len(live.Components))
	}
	if len(restored.Components[0].Modules) != 1 {
		t.Fatalf("modules = %#v", restored.Components[0].Modules)
	}
	module := restored.Components[0].Modules[0]
	if module.ModuleID != 2 || module.ComponentTag != componentTag {
		t.Fatalf("module = %#v, want moduleId 2 componentTag %#x", module, componentTag)
	}
	if !module.Complete || module.Status != "complete" {
		t.Fatalf("module = %#v, want complete (module payload was persisted to the module store)", module)
	}
	if module.ReceivedBlocks != module.TotalBlocks {
		t.Fatalf("receivedBlocks = %d, totalBlocks = %d, want equal for a complete module", module.ReceivedBlocks, module.TotalBlocks)
	}
}

func TestSessionSnapshotCarouselRemovedWhenComponentDropsFromPMT(t *testing.T) {
	serviceID, pmtPID, firstPID, secondPID := uint16(101), uint16(0x0100), uint16(0x0200), uint16(0x0201)
	firstTag, secondTag := byte(0x40), byte(0x41)

	store, err := databroadcast.NewSQLiteModuleStore(filepath.Join(t.TempDir(), "cache.sqlite3"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hub := databroadcast.NewDataBroadcastHub()
	session := &Session{
		channel:                "27",
		typ:                    "GR",
		dataBroadcast:          hub,
		snapshotStore:          store,
		lastPersistedSnapshots: map[uint16]databroadcast.PersistedService{},
	}

	hub.Observe(ts.PIDSection{PID: pmtPID, Section: streamBuildTwoComponentPMT(serviceID, firstPID, firstTag, secondPID, secondTag)})
	hub.Observe(ts.PIDSection{PID: firstPID, Section: streamBuildDSMCCDII(t, 1, 4, 2, 4, 1, []byte("index.bml"))})
	hub.Observe(ts.PIDSection{PID: secondPID, Section: streamBuildDSMCCDII(t, 1, 4, 3, 4, 1, []byte("sub.bml"))})
	session.flushDataBroadcastSnapshots()

	persisted, ok := store.GetSnapshot("GR", "27", serviceID)
	if !ok || len(persisted.Carousels) != 2 {
		t.Fatalf("persisted = %#v, found = %v, want 2 carousels", persisted, ok)
	}

	// The PMT no longer carries the second component.
	hub.Observe(ts.PIDSection{PID: pmtPID, Section: streamBuildDataBroadcastPMT(serviceID, firstPID, firstTag)})
	session.flushDataBroadcastSnapshots()

	persisted, ok = store.GetSnapshot("GR", "27", serviceID)
	if !ok || len(persisted.Carousels) != 1 || persisted.Carousels[0].ComponentTag != firstTag {
		t.Fatalf("persisted = %#v, found = %v, want only component %#x", persisted, ok, firstTag)
	}
}

func streamBuildTwoComponentPMT(serviceID, firstPID uint16, firstTag byte, secondPID uint16, secondTag byte) ts.Section {
	firstInfo := []byte{0x52, 0x01, firstTag}
	secondInfo := []byte{0x52, 0x01, secondTag}
	length := 9 + (5 + len(firstInfo)) + (5 + len(secondInfo)) + 4
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
	s[off+1] = 0xe0 | byte(firstPID>>8)
	s[off+2] = byte(firstPID)
	s[off+3] = 0xf0 | byte(len(firstInfo)>>8)
	s[off+4] = byte(len(firstInfo))
	copy(s[off+5:], firstInfo)
	off += 5 + len(firstInfo)
	s[off] = ts.StreamTypeDSMCCDataCarousel
	s[off+1] = 0xe0 | byte(secondPID>>8)
	s[off+2] = byte(secondPID)
	s[off+3] = 0xf0 | byte(len(secondInfo)>>8)
	s[off+4] = byte(len(secondInfo))
	copy(s[off+5:], secondInfo)
	streamWriteCRC(s)
	return ts.Section(s)
}
