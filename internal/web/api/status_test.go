package api

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func newStatusHandler(t *testing.T) (*Handler, *job.JobManager, *service.ServiceManager, *program.ProgramManager, *sql.DB) {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	mgr, err := job.NewManager(job.Config{MaxHistory: 10})
	if err != nil {
		t.Fatal(err)
	}
	store := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(store, config.ChannelsConfig{})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	return NewHandler(HandlerConfig{ServiceManager: sm, ProgramManager: pm, JobManager: mgr, EpgStaleAfter: 5000}), mgr, sm, pm, database
}

func TestGetStatusExposesEPGSnapshot(t *testing.T) {
	ctx := context.Background()
	handler, mgr, sm, pm, database := newStatusHandler(t)
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", ServiceId: 102, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sm.SetEPGSuccess(ctx, 1, 101, 1000); err != nil {
		t.Fatal(err)
	}
	if err := sm.SetEPGAttempt(ctx, 1, 102, 2000, "boom"); err != nil {
		t.Fatal(err)
	}
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{ID: program.ProgramID(1, 101, 9), NetworkID: 1, ServiceID: 101, EventID: 9, StartAt: 1000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	epgBlock := make(chan struct{})
	t.Cleanup(func() { close(epgBlock) })
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{Key: "epg-gather:nid:1", Name: "EPG Gather NID 1", Handler: func(ctx context.Context) error {
		<-epgBlock
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{Key: "service-scan:GR:27", Name: "Service Scan", Handler: func(context.Context) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	res, err := handler.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, ok := res.(*apigen.Status)
	if !ok {
		t.Fatalf("response type = %T, want *Status", res)
	}
	if !status.Time.IsSet() || status.Time.Value <= 0 {
		t.Fatalf("Time = %+v, want positive value", status.Time)
	}
	if status.Version.Value != currentVersion {
		t.Errorf("Version = %q, want %q", status.Version.Value, currentVersion)
	}
	if !status.Process.IsSet() {
		t.Fatal("status.Process is unset")
	}
	process := status.Process.Value
	if process.Arch.Value != runtime.GOARCH {
		t.Errorf("Process.Arch = %q, want %q", process.Arch.Value, runtime.GOARCH)
	}
	if process.Platform.Value != runtime.GOOS {
		t.Errorf("Process.Platform = %q, want %q", process.Platform.Value, runtime.GOOS)
	}
	if process.Pid.Value <= 0 {
		t.Errorf("Process.Pid = %d, want positive value", process.Pid.Value)
	}
	if !process.MemoryUsage.IsSet() {
		t.Fatal("Process.MemoryUsage is unset")
	}
	memory := process.MemoryUsage.Value
	if memory.Rss.Value <= 0 || memory.HeapTotal.Value <= 0 || memory.HeapUsed.Value <= 0 {
		t.Errorf("MemoryUsage = %+v, want positive values", memory)
	}
	if !status.Epg.IsSet() {
		t.Fatal("status.Epg is unset")
	}
	epg := status.Epg.Value
	if got, want := len(epg.GatheringNetworks), 1; got != want {
		t.Fatalf("GatheringNetworks = %d, want %d", got, want)
	}
	if epg.GatheringNetworks[0] != 1 {
		t.Errorf("GatheringNetworks[0] = %d, want 1", epg.GatheringNetworks[0])
	}
	if epg.StoredEvents.Value != 1 {
		t.Errorf("StoredEvents = %d, want 1", epg.StoredEvents.Value)
	}
	if epg.StaleServices.Value != 2 {
		t.Errorf("StaleServices = %d, want 2 (both stale with 5000ms window vs attempt=2000)", epg.StaleServices.Value)
	}
	if epg.FailedServices.Value != 1 {
		t.Errorf("FailedServices = %d, want 1", epg.FailedServices.Value)
	}
	if epg.LastUpdatedAt.Value != 1000 {
		t.Errorf("LastUpdatedAt = %d, want 1000", epg.LastUpdatedAt.Value)
	}
}

func TestGetStatusExposesStreamAndTunerCounts(t *testing.T) {
	handler := NewHandler(HandlerConfig{
		StreamManager: fakeStatusStreamManager{active: 3},
		TunerManager: fakeStatusTunerManager{statuses: []tuner.Status{
			{IsUsing: true},
			{IsUsing: false},
			{IsUsing: true},
		}},
	})

	res, err := handler.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status := res.(*apigen.Status)
	if !status.StreamCount.IsSet() {
		t.Fatal("StreamCount is unset")
	}
	streamCount := status.StreamCount.Value
	if streamCount.TunerDevice.Value != 2 {
		t.Errorf("TunerDevice = %d, want 2", streamCount.TunerDevice.Value)
	}
	if streamCount.TsFilter.Value != 3 {
		t.Errorf("TsFilter = %d, want 3", streamCount.TsFilter.Value)
	}
	if streamCount.Decoder.Value != 3 {
		t.Errorf("Decoder = %d, want 3", streamCount.Decoder.Value)
	}
}

func TestGetStatusToleratesMissingManagers(t *testing.T) {
	handler := NewHandler(HandlerConfig{})

	res, err := handler.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status := res.(*apigen.Status)
	if !status.Process.IsSet() {
		t.Fatal("Process is unset")
	}
	if !status.Epg.IsSet() {
		t.Fatal("Epg is unset")
	}
	epg := status.Epg.Value
	if epg.StoredEvents.IsSet() || epg.StaleServices.IsSet() || epg.FailedServices.IsSet() || epg.LastUpdatedAt.IsSet() {
		t.Fatalf("Epg optional fields should be unset without managers: %+v", epg)
	}
	if status.StreamCount.IsSet() {
		t.Fatalf("StreamCount should be unset without stream/tuner managers: %+v", status.StreamCount.Value)
	}
}

func TestGetStatusToleratesPartialEPGFailures(t *testing.T) {
	handler := NewHandler(HandlerConfig{
		ProgramManager: failingStatusProgramManager{},
		ServiceManager: failingStatusServiceManager{},
	})

	res, err := handler.GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status := res.(*apigen.Status)
	if !status.Epg.IsSet() {
		t.Fatal("Epg is unset")
	}
	epg := status.Epg.Value
	if epg.StoredEvents.IsSet() {
		t.Fatalf("StoredEvents = %+v, want unset after Count error", epg.StoredEvents)
	}
	if epg.StaleServices.IsSet() || epg.FailedServices.IsSet() || epg.LastUpdatedAt.IsSet() {
		t.Fatalf("service EPG fields should be unset after EPGSummary error: %+v", epg)
	}
}

type fakeStatusStreamManager struct {
	active int
}

func (m fakeStatusStreamManager) GetOrCreate(context.Context, string, string) (stream.Session, error) {
	return nil, errors.New("unexpected GetOrCreate call")
}

func (m fakeStatusStreamManager) GetExisting(string, string) (stream.Session, bool) {
	return nil, false
}

func (m fakeStatusStreamManager) ActiveSessionCount() int {
	return m.active
}

type fakeStatusTunerManager struct {
	statuses []tuner.Status
}

func (m fakeStatusTunerManager) KillProcess(context.Context, int) error {
	return errors.New("unexpected KillProcess call")
}

func (m fakeStatusTunerManager) Status(int) (tuner.Status, bool) {
	return tuner.Status{}, false
}

func (m fakeStatusTunerManager) Statuses() []tuner.Status {
	return append([]tuner.Status(nil), m.statuses...)
}

type failingStatusProgramManager struct{}

func (m failingStatusProgramManager) Count(context.Context) (int, error) {
	return 0, errors.New("count failed")
}

func (m failingStatusProgramManager) Get(context.Context, int64) (*program.Program, bool, error) {
	return nil, false, errors.New("unexpected Get call")
}

func (m failingStatusProgramManager) List(context.Context, program.Query) ([]*program.Program, error) {
	return nil, errors.New("unexpected List call")
}

type failingStatusServiceManager struct{}

func (m failingStatusServiceManager) EPGSummary(context.Context, int64, int64) (int, int, *int64, error) {
	return 0, 0, nil, errors.New("summary failed")
}

func (m failingStatusServiceManager) GetChannel(string, string) *config.ChannelConfig {
	return nil
}

func (m failingStatusServiceManager) GetChannels() config.ChannelsConfig {
	return nil
}

func (m failingStatusServiceManager) GetServiceByChannelAndId(context.Context, string, string, string) (*service.Service, error) {
	return nil, errors.New("unexpected GetServiceByChannelAndId call")
}

func (m failingStatusServiceManager) GetServiceById(context.Context, string) (*service.Service, error) {
	return nil, errors.New("unexpected GetServiceById call")
}

func (m failingStatusServiceManager) GetServiceByItemID(context.Context, int64) (*service.Service, error) {
	return nil, errors.New("unexpected GetServiceByItemID call")
}

func (m failingStatusServiceManager) GetLogoByServiceItemID(context.Context, int64) ([]byte, error) {
	return nil, errors.New("unexpected GetLogoByServiceItemID call")
}

func (m failingStatusServiceManager) GetServices(context.Context) ([]*service.Service, error) {
	return nil, errors.New("unexpected GetServices call")
}

func (m failingStatusServiceManager) GetServicesByChannel(context.Context, string, string) ([]*service.Service, error) {
	return nil, errors.New("unexpected GetServicesByChannel call")
}
