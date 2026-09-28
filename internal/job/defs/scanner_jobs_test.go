package defs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/logogather"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
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
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewManager(serviceStore, channels)
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewManager(program.NewSQLiteStore(database))
	scanService := servicescan.NewScanner(sm, stream.NewServiceScanAdapter(stm), channels, 30*time.Second)
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanService, epgService)
	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:           true,
		"service-scan:GR:27":        true,
		"service-scan:GR:26":        true,
		serviceUpdateEPGGathererKey: true,
	})
}

func TestServiceUpdaterScansWithoutWaitingForBusyTuner(t *testing.T) {
	channels := config.ChannelsConfig{{Type: "EXT1", Channel: "11"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	scanner := &recordingServiceScanner{channels: []servicescan.Channel{{Type: "EXT1", ID: "11"}}}
	sm := service.NewManager(service.NewSQLiteStore(database), channels)
	pm := program.NewManager(program.NewSQLiteStore(database))
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanner, epgService)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:      true,
		"service-scan:EXT1:11": true,
	})
	child := waitForFinishedJobKey(t, mgr, "service-scan:EXT1:11")
	if child.HasFailed {
		t.Fatalf("service scan failed: %v", child.Error)
	}
	if got := scanner.lastWait(); got {
		t.Fatal("service scan wait = true, want false")
	}
}

func TestEnqueueServiceScansUsesServiceScanJobBehavior(t *testing.T) {
	mgr := newTestManager(t)
	scanner := &recordingServiceScanner{newNIDs: []uint16{4}}

	queued, err := EnqueueServiceScans(t.Context(), mgr, scanner, fakeEPGGatherer{}, []servicescan.Channel{{Type: "EXT1", ID: "11"}})
	if err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("queued = %d, want 1", queued)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		"service-scan:EXT1:11": true,
	})
	child := waitForFinishedJobKey(t, mgr, "service-scan:EXT1:11")
	if child.HasFailed {
		t.Fatalf("service scan failed: %v", child.Error)
	}
	if got := scanner.lastWait(); got {
		t.Fatal("service scan wait = true, want false")
	}
}

func TestServiceScanRetriesWhenTunerUnavailable(t *testing.T) {
	channels := config.ChannelsConfig{{Type: "EXT1", Channel: "11"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	scanner := &recordingServiceScanner{
		channels: []servicescan.Channel{{Type: "EXT1", ID: "11"}},
		err:      tuner.ErrTunerUnavailable,
	}
	sm := service.NewManager(service.NewSQLiteStore(database), channels)
	pm := program.NewManager(program.NewSQLiteStore(database))
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanner, epgService)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:      true,
		"service-scan:EXT1:11": true,
	})
	child := waitForJobKeyStatus(t, mgr, "service-scan:EXT1:11", job.StatusStandby)
	if child.RetryCount != 1 {
		t.Fatalf("retry count = %d, want 1", child.RetryCount)
	}
}

func TestServiceScanDoesNotRetryChannelNotFound(t *testing.T) {
	channels := config.ChannelsConfig{{Type: "EXT1", Channel: "29"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	scanner := &recordingServiceScanner{
		channels: []servicescan.Channel{{Type: "EXT1", ID: "29"}},
		err:      errors.New("channel not found"),
	}
	sm := service.NewManager(service.NewSQLiteStore(database), channels)
	pm := program.NewManager(program.NewSQLiteStore(database))
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanner, epgService)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:      true,
		"service-scan:EXT1:29": true,
	})
	child := waitForFinishedJobKey(t, mgr, "service-scan:EXT1:29")
	if !child.HasFailed {
		t.Fatal("channel not found scan should fail without retry")
	}
	if child.RetryCount != 0 {
		t.Fatalf("retry count = %d, want 0", child.RetryCount)
	}
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
	defer func() { _ = database.Close() }()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewManager(serviceStore, channels)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "327360001", Service: model.Service{Key: model.ServiceKey{NetworkID: 32736, ServiceID: 1}, EITSchedule: true}, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400101", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}, EITSchedule: true}, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS03", []*service.Service{
		{Id: "0000400103", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 103}, EITSchedule: true}, ChannelType: "BS", ChannelId: "BS03"},
	}); err != nil {
		t.Fatal(err)
	}

	programDatabase, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = programDatabase.Close() }()
	mgr := newTestManager(t)
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewManager(program.NewSQLiteStore(programDatabase))
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterEPGGatherer(mgr, epgService, pm, 3)
	if _, err := mgr.Enqueue(EPGGathererKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		EPGGathererKey:         true,
		"epg-gather:nid:32736": true,
		"epg-gather:nid:4":     true,
	})
}

func TestEnqueueEPGGatherForNetworkIgnoresMissingNetwork(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{{Type: "BS", Channel: "BS01"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	sm := service.NewManager(service.NewSQLiteStore(database), channels)
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewManager(program.NewSQLiteStore(database))
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)

	enqueued, err := enqueueEPGGatherForNetwork(ctx, mgr, epgService, 999, nil, nil)
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

func TestLogoGathererDispatchesOneJobPerChannel(t *testing.T) {
	mgr := newTestManager(t)
	bs := logogather.Target{Service: model.ServiceKey{NetworkID: 4, ServiceID: 101}, ChannelType: "BS", ChannelID: "BS01", LogoID: 12, Version: 3, DownloadDataID: 7}
	bs2 := bs
	bs2.Service.ServiceID = 102
	tlv := logogather.Target{Service: model.ServiceKey{NetworkID: 0x000B, ServiceID: 101}, ChannelType: "BS4K", ChannelID: "BS1_0", LogoID: 101, Version: 1, DownloadDataID: 1}
	gatherer := &fakeLogoGatherer{targets: []logogather.Target{bs, tlv, bs2}}
	RegisterLogoGatherer(mgr, gatherer)

	parentID, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, parentID)
	waitForJobKeys(t, mgr, map[string]bool{
		LogoGathererKey:          true,
		"logo-gather:BS:BS01":    true,
		"logo-gather:BS4K:BS1_0": true,
	})
	for _, item := range mgr.GetJobs() {
		if item.Key != LogoGathererKey {
			waitJob(t, mgr, item.ID)
		}
	}
	got := gatherer.gathered()
	if len(got["BS/BS01"]) != 2 || len(got["BS4K/BS1_0"]) != 1 {
		t.Fatalf("gathered = %v, want both BS targets on one job and the TLV target on another", got)
	}
}

func TestLogoGathererSkipsWithoutTargets(t *testing.T) {
	mgr := newTestManager(t)
	gatherer := &fakeLogoGatherer{}
	RegisterLogoGatherer(mgr, gatherer)
	id, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, id)
	if len(gatherer.gathered()) != 0 {
		t.Fatalf("gathered = %v, want nothing", gatherer.gathered())
	}
	for _, item := range mgr.GetJobs() {
		if item.Key != LogoGathererKey {
			t.Fatalf("unexpected logo child job: %#v", item)
		}
	}
}

// TestLogoGathererRegathersCommonDataAfterProbe covers the SDTT probe: a
// channel gathered for an unresolved common data target may observe the
// announcement, after which the common data is gathered from the channel it
// names.
func TestLogoGathererRegathersCommonDataAfterProbe(t *testing.T) {
	mgr := newTestManager(t)
	probe := logogather.Target{Service: model.ServiceKey{NetworkID: 4, ServiceID: 101}, ChannelType: "BS", ChannelID: "BS01", CommonData: true, Probe: true}
	resolved := logogather.Target{Service: model.ServiceKey{NetworkID: 4, ServiceID: 101}, ChannelType: "BS", ChannelID: "BS15", CommonData: true}
	gatherer := &fakeLogoGatherer{targets: []logogather.Target{probe}, afterGather: []logogather.Target{resolved}}
	RegisterLogoGatherer(mgr, gatherer)

	parentID, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, parentID)
	waitForJobKeys(t, mgr, map[string]bool{
		LogoGathererKey:       true,
		"logo-gather:BS:BS01": true,
		"logo-gather:BS:BS15": true,
	})
}

func TestServiceUpdaterStartsEPGGatherAfterServiceScans(t *testing.T) {
	channels := config.ChannelsConfig{
		{Type: "BS", Channel: "BS01"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewManager(serviceStore, channels)
	mgr := newTestManager(t)
	pm := program.NewManager(program.NewSQLiteStore(database))
	scanService := servicescan.NewScanner(sm, fakeScanScanner{services: []model.Service{
		{Key: model.ServiceKey{NetworkID: 4, StreamID: 1, ServiceID: 101}, Name: "test", Type: 1, EITSchedule: true},
		{Key: model.ServiceKey{NetworkID: 4, StreamID: 1, ServiceID: 102}, Name: "test", Type: 1, EITSchedule: true},
	}}, channels, 30*time.Second)
	stm := stream.NewManager(stream.ManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epggather.NewGatherer(pm, pm, sm, stream.NewEPGGatherAdapter(stm), channels, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanService, epgService)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:           true,
		"service-scan:BS:BS01":      true,
		serviceUpdateEPGGathererKey: true,
		"epg-gather:nid:4":          true,
	})
}

type fakeScanScanner struct {
	services []model.Service
}

func (f fakeScanScanner) ScanServices(context.Context, context.Context, string, string, bool) ([]model.Service, error) {
	return append([]model.Service(nil), f.services...), nil
}

type recordingServiceScanner struct {
	channels []servicescan.Channel
	err      error
	newNIDs  []uint16
	wait     bool
}

func (s *recordingServiceScanner) Channels() []servicescan.Channel {
	return append([]servicescan.Channel(nil), s.channels...)
}

func (s *recordingServiceScanner) ScanChannel(_ context.Context, _, _ string, wait bool) ([]uint16, error) {
	s.wait = wait
	if s.err != nil {
		return nil, s.err
	}
	return append([]uint16(nil), s.newNIDs...), nil
}

func (s *recordingServiceScanner) lastWait() bool {
	return s.wait
}

type fakeEPGGatherer struct{}

func (fakeEPGGatherer) Groups(context.Context) (map[uint16]*epggather.Network, error) {
	return nil, nil
}

func (fakeEPGGatherer) BuildNetworkInputs(context.Context, uint16) ([]epggather.Candidate, []model.ServiceKey, error) {
	return nil, []model.ServiceKey{{NetworkID: 4, ServiceID: 101}}, nil
}

func (fakeEPGGatherer) GatherNetwork(context.Context, uint16, []epggather.Candidate, []model.ServiceKey) error {
	return nil
}

type fakeLogoGatherer struct {
	mu          sync.Mutex
	targets     []logogather.Target
	afterGather []logogather.Target
	channels    map[string][]logogather.Target
}

func (f *fakeLogoGatherer) Targets(context.Context) ([]logogather.Target, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]logogather.Target(nil), f.targets...), nil
}

func (f *fakeLogoGatherer) GatherChannel(_ context.Context, channelType, channelID string, targets []logogather.Target) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channels == nil {
		f.channels = map[string][]logogather.Target{}
	}
	f.channels[channelType+"/"+channelID] = append(f.channels[channelType+"/"+channelID], targets...)
	if f.afterGather != nil {
		f.targets = f.afterGather
	}
	return nil
}

func (f *fakeLogoGatherer) gathered() map[string][]logogather.Target {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.channels
}

func waitForJobKeys(t *testing.T, mgr *job.Manager, expected map[string]bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		changed := mgr.Changes()
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
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("job keys not dispatched: %#v", mgr.GetJobs())
		}
	}
}

func waitForJobKeyStatus(t *testing.T, mgr *job.Manager, key string, status job.JobStatus) *job.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		changed := mgr.Changes()
		for _, item := range mgr.GetJobs() {
			if item.Key == key && item.Status == status {
				return item
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("job %s did not reach status %s: %#v", key, status, mgr.GetJobs())
		}
	}
}

func waitForFinishedJobKey(t *testing.T, mgr *job.Manager, key string) *job.Job {
	t.Helper()
	return waitForJobKeyStatus(t, mgr, key, job.StatusFinished)
}

func newTestManager(t *testing.T) *job.Manager {
	t.Helper()
	mgr, err := job.NewManager(job.Config{MaxHistory: 10})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func waitJob(t *testing.T, mgr *job.Manager, id string) *job.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	item, err := mgr.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait(%q): %v", id, err)
	}
	return item
}
