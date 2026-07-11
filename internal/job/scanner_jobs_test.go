package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
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
	sm := service.NewServiceManager(serviceStore, channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	scanService := servicescan.NewService(sm, stream.NewServiceScannerAdapter(stm), channels, 30*time.Second)
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)
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
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)
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
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)
	RegisterServiceUpdater(mgr, scanner, epgService)

	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:      true,
		"service-scan:EXT1:11": true,
	})
	child := waitForJobKeyStatus(t, mgr, "service-scan:EXT1:11", StatusStandby)
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
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)
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
	sm := service.NewServiceManager(serviceStore, channels)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "327360001", NetworkId: 32736, ServiceId: 1, EITScheduleFlag: true, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400101", NetworkId: 4, ServiceId: 101, EITScheduleFlag: true, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS03", []*service.Service{
		{Id: "0000400103", NetworkId: 4, ServiceId: 103, EITScheduleFlag: true, ChannelType: "BS", ChannelId: "BS03"},
	}); err != nil {
		t.Fatal(err)
	}

	programDatabase, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = programDatabase.Close() }()
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

func TestEnqueueEPGGatherForNetworkIgnoresMissingNetwork(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{{Type: "BS", Channel: "BS01"}}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	mgr := newTestManager(t)
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)

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

func TestLogoGathererDispatchesOnlyMissingChannelsAndCompletesWhenSatisfied(t *testing.T) {
	mgr := newTestManager(t)
	target := service.LogoTarget{
		NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01",
		LogoId: 12, LogoVersion: 3, LogoDownloadDataId: 7,
	}
	collector := &fakeLogoObserver{image: &ts.LogoImage{
		OriginalNetworkID: 4, LogoID: 12, LogoVersion: 3, DownloadDataID: 7,
	}}
	store := &fakeLogoTargetStore{targets: []service.LogoTarget{target}}
	RegisterLogoGatherer(mgr, collector, store, 20*time.Minute)

	parentID, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, parentID)
	waitForJobKeys(t, mgr, map[string]bool{
		LogoGathererKey:       true,
		"logo-gather:BS:BS01": true,
	})
	for _, item := range mgr.GetJobs() {
		if item.Key == "logo-gather:BS:BS01" {
			waitJob(t, mgr, item.ID)
		}
	}
	if collector.calls != 1 {
		t.Fatalf("ObserveLogos calls = %d, want 1", collector.calls)
	}
	if len(store.images) != 1 || store.images[0] != collector.image {
		t.Fatalf("persisted images = %#v, want observed image", store.images)
	}
}

func TestLogoGathererSkipsChannelsWithoutMissingTargets(t *testing.T) {
	mgr := newTestManager(t)
	collector := &fakeLogoObserver{}
	RegisterLogoGatherer(mgr, collector, &fakeLogoTargetStore{}, 20*time.Minute)
	id, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, id)
	if collector.calls != 0 {
		t.Fatalf("ObserveLogos calls = %d, want 0", collector.calls)
	}
	for _, item := range mgr.GetJobs() {
		if item.Key != LogoGathererKey {
			t.Fatalf("unexpected logo child job: %#v", item)
		}
	}
}

func TestLogoGatherTimeoutIsSuccessful(t *testing.T) {
	mgr := newTestManager(t)
	target := service.LogoTarget{NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01", LogoId: 12, LogoVersion: 3, LogoDownloadDataId: 7}
	collector := &fakeLogoObserver{waitForContext: true}
	RegisterLogoGatherer(mgr, collector, &fakeLogoTargetStore{targets: []service.LogoTarget{target}}, time.Millisecond)
	parentID, err := mgr.Enqueue(LogoGathererKey)
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, parentID)
	var child *Job
	deadline := time.Now().Add(time.Second)
	for child == nil && time.Now().Before(deadline) {
		for _, item := range mgr.GetJobs() {
			if item.Key == "logo-gather:BS:BS01" {
				child = item
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if child == nil {
		t.Fatal("logo child job was not created")
	}
	finished := waitJob(t, mgr, child.ID)
	if finished.HasFailed {
		t.Fatalf("timed out logo job failed: %#v", finished)
	}
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
	sm := service.NewServiceManager(serviceStore, channels)
	mgr := newTestManager(t)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	scanService := servicescan.NewService(sm, fakeScanScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "test", Type: 1, EITScheduleFlag: true},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "test", Type: 1, EITScheduleFlag: true},
	}}, channels, 30*time.Second)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	epgService := epg.NewService(pm, sm, stream.NewEPGCollectorAdapter(stm), channels, 0, 10*time.Minute)
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
	services []ts.ServiceInfo
}

func (f fakeScanScanner) ScanServices(context.Context, string, string, bool) ([]ts.ServiceInfo, error) {
	return append([]ts.ServiceInfo(nil), f.services...), nil
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

func (fakeEPGGatherer) Groups(context.Context) (map[uint16]*epg.Network, error) {
	return nil, nil
}

func (fakeEPGGatherer) BuildNetworkInputs(context.Context, uint16) ([]epg.Candidate, []epg.ServiceKey, error) {
	return nil, []epg.ServiceKey{{NetworkID: 4, ServiceID: 101}}, nil
}

func (fakeEPGGatherer) GatherNetwork(context.Context, uint16, []epg.Candidate, []epg.ServiceKey) error {
	return nil
}

func (fakeEPGGatherer) Cleanup(context.Context, time.Time) error {
	return nil
}

type fakeLogoTargetStore struct {
	targets []service.LogoTarget
	images  []*ts.LogoImage
}

func (s fakeLogoTargetStore) MissingLogoTargets(context.Context) ([]service.LogoTarget, error) {
	return append([]service.LogoTarget(nil), s.targets...), nil
}

func (s *fakeLogoTargetStore) UpsertLogoImage(_ context.Context, image *ts.LogoImage) error {
	s.images = append(s.images, image)
	return nil
}

type fakeLogoObserver struct {
	calls          int
	image          *ts.LogoImage
	waitForContext bool
}

func (f *fakeLogoObserver) ObserveLogos(ctx context.Context, _, _ string, observe func(*ts.LogoImage) error) error {
	f.calls++
	if f.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return observe(f.image)
}

func waitForJobKeys(t *testing.T, mgr *JobManager, expected map[string]bool) {
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

func waitForJobKeyStatus(t *testing.T, mgr *JobManager, key string, status JobStatus) *Job {
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

func waitForFinishedJobKey(t *testing.T, mgr *JobManager, key string) *Job {
	t.Helper()
	return waitForJobKeyStatus(t, mgr, key, StatusFinished)
}
