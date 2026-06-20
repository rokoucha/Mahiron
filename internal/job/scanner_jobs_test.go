package job

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/stream"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

type noTunerManager struct{}

func (noTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return nil, errors.New("no tuner")
}

func TestServiceUpdaterDispatchesPerChannel(t *testing.T) {
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "26"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mgr := newTestManager(t)
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	RegisterServiceUpdater(mgr, program.NewProgramManager(program.NewSQLiteStore(database)), sm, stream.NewServiceScannerAdapter(stm), stream.NewEPGCollectorAdapter(stm), channels, 10*time.Minute)
	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:    true,
		"service-scan:GR:27": true,
		"service-scan:GR:26": true,
	})
}

func TestEPGGathererDispatchesPerNetwork(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "BS", Channel: "BS01"},
		{Type: "BS", Channel: "BS03"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(serviceStore, channels)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "327360001", NetworkId: 32736, ServiceId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400101", NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS03", []*service.Service{
		{Id: "0000400103", NetworkId: 4, ServiceId: 103, ChannelType: "BS", ChannelId: "BS03"},
	}); err != nil {
		t.Fatal(err)
	}

	programDatabase, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer programDatabase.Close()
	mgr := newTestManager(t)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	RegisterEPGGatherer(mgr, program.NewProgramManager(program.NewSQLiteStore(programDatabase)), sm, stream.NewEPGCollectorAdapter(stm), channels, 3, 10*time.Minute)
	if _, err := mgr.Enqueue(EPGGathererKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		EPGGathererKey:         true,
		"epg-gather:nid:32736": true,
		"epg-gather:nid:4":     true,
	})
}

func TestEnqueueEPGGatherForNetworkDispatches(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{
		{Type: "BS", Channel: "BS01"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(serviceStore, channels)
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400101", NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
		{Id: "0000400102", NetworkId: 4, ServiceId: 102, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := newTestManager(t)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))

	enqueued, err := enqueueEPGGatherForNetwork(ctx, mgr, pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 10*time.Minute, 4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued {
		t.Fatal("expected job to be enqueued")
	}
	waitForJobKeys(t, mgr, map[string]bool{
		"epg-gather:nid:4": true,
	})
}

func TestEnqueueEPGGatherForNetworkIgnoresMissingNetwork(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{{Type: "BS", Channel: "BS01"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mgr := newTestManager(t)
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))

	enqueued, err := enqueueEPGGatherForNetwork(ctx, mgr, pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 10*time.Minute, 999, nil, nil)
	if err != nil {
		t.Fatalf("expected nil error for missing network, got %v", err)
	}
	if enqueued {
		t.Fatal("expected no job to be enqueued for missing network")
	}
	for _, item := range mgr.GetJobs() {
		if item.Key == "epg-gather:nid:999" {
			t.Fatalf("unexpected job enqueued for missing network: %#v", item)
		}
	}
}

// TestServiceUpdaterTriggersEPGGatherForNewNetworks verifies that a successful
// service scan which introduces a new network causes an EPG gather job for
// that network to be enqueued, without waiting for the EPG gatherer cron.
// It also verifies that a subsequent scan which finds no new services does
// not re-enqueue the EPG gather.
func TestServiceUpdaterTriggersEPGGatherForNewNetworks(t *testing.T) {
	channels := config.ChannelsConfig{
		{Type: "BS", Channel: "BS01"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(serviceStore, channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{
		Channels:     channels,
		TunerManager: fakeScanTunerManager{},
		Scanner:      fakeScanScanner{services: []*scanServiceJSON{{Nid: 4, Sid: 101}, {Nid: 4, Sid: 102}}},
	})
	mgr := newTestManager(t)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	RegisterServiceUpdater(mgr, pm, sm, stream.NewServiceScannerAdapter(stm), stream.NewEPGCollectorAdapter(stm), channels, 10*time.Minute)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:      true,
		"service-scan:BS:BS01": true,
		"epg-gather:nid:4":     true,
	})

	// Abort the EPG gather so the test doesn't wait for its (never-completing
	// with a fake device) handler, and clear the active key.
	for _, item := range mgr.GetJobs() {
		if item.Key == "epg-gather:nid:4" {
			_ = mgr.Abort(item.ID)
		}
	}
	waitJobIdle(t, mgr, "epg-gather:nid:4")

	// A second scan of the same channel re-finds the same services (now in DB),
	// so no new networks are detected and EPG gather is NOT re-enqueued.
	// Record the scan count before enqueuing so we can wait for the new scan
	// to finish before asserting.
	scansBefore := countFinishedJobs(t, mgr, "service-scan:BS:BS01")
	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	// Wait for the second service-scan to finish, then verify no new gather.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countFinishedJobs(t, mgr, "service-scan:BS:BS01") >= scansBefore+1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := countFinishedJobs(t, mgr, "service-scan:BS:BS01"); got < scansBefore+1 {
		t.Fatalf("second service-scan did not finish, finished=%d before=%d", got, scansBefore)
	}
	// Give the scan handler a moment to enqueue any EPG gather after the scan
	// result is processed.
	time.Sleep(50 * time.Millisecond)
	count := 0
	for _, item := range mgr.GetJobs() {
		if item.Key == "epg-gather:nid:4" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("epg-gather:nid:4 enqueued %d times, want exactly 1", count)
	}
}

type scanServiceJSON struct {
	Nid uint16 `json:"nid"`
	Sid uint16 `json:"sid"`
}

type fakeScanScanner struct {
	services []*scanServiceJSON
}

func (f fakeScanScanner) ScanServices(_ context.Context, _ io.Reader, dst io.Writer) error {
	return encodeScanJSON(f.services, dst)
}

func encodeScanJSON(services []*scanServiceJSON, dst io.Writer) error {
	type entry struct {
		Nid  uint16 `json:"nid"`
		Tsid uint16 `json:"tsid"`
		Sid  uint16 `json:"sid"`
		Name string `json:"name"`
		Type uint8  `json:"type"`
	}
	out := make([]entry, len(services))
	for i, s := range services {
		out[i] = entry{Nid: s.Nid, Tsid: 1, Sid: s.Sid, Name: "test", Type: 1}
	}
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	_, err = dst.Write(data)
	return err
}

type fakeScanTunerManager struct{}

func (fakeScanTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return &fakeScanDevice{}, nil
}

type fakeScanDevice struct {
	done chan struct{}
}

func (d *fakeScanDevice) Start(_ context.Context, _ io.Writer) error {
	if d.done == nil {
		d.done = make(chan struct{})
	}
	return nil
}

func (d *fakeScanDevice) Stop(_ context.Context) error { return nil }
func (d *fakeScanDevice) Done() <-chan struct{}        { return d.done }
func (d *fakeScanDevice) Err() error                   { return nil }

func waitForJobKeys(t *testing.T, mgr *JobManager, expected map[string]bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		found := make(map[string]bool)
		for _, item := range mgr.GetJobs() {
			found[item.Key] = true
		}
		all := true
		for key := range expected {
			all = all && found[key]
		}
		if all {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("job keys not dispatched: %#v", mgr.GetJobs())
}

func countFinishedJobs(t *testing.T, mgr *JobManager, key string) int {
	t.Helper()
	count := 0
	for _, item := range mgr.GetJobs() {
		if item.Key == key && item.Status == StatusFinished {
			count++
		}
	}
	return count
}
