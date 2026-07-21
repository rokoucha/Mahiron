package app

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestParseRunOptions(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    runOptions
		wantErr bool
	}{
		{
			name: "default config dir",
			want: runOptions{ConfigDir: config.DefaultConfigDir},
		},
		{
			name: "custom config dir",
			args: []string{"--config-dir", "/etc/mahiron"},
			want: runOptions{ConfigDir: "/etc/mahiron"},
		},
		{
			name:    "unknown flag",
			args:    []string{"--unknown"},
			wantErr: true,
		},
		{
			name:    "unexpected positional argument",
			args:    []string{"extra"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRunOptions(tt.args, io.Discard)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseRunOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got != tt.want {
				t.Fatalf("parseRunOptions() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildRuntimeWiresCurrentApplication(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	obs := observability.Setup(t.Context(), config.ObservabilityConfig{}, nil)
	cfg := &config.Config{System: &config.SystemConfig{
		Addresses:              []config.ServerAddress{{Http: "127.0.0.1:0"}},
		DataBroadcastCachePath: ":memory:",
		MaxConcurrentJobs:      1,
		EpgRetrievalTime:       5_000,
		EpgStaleAfter:          7_200_000,
		LogoGatherTimeout:      1_200_000,
		ServiceScanTimeout:     30_000,
	}}

	runtime, message, err := buildRuntime(cfg, database, obs)
	if err != nil {
		t.Fatalf("buildRuntime() message=%q err=%v", message, err)
	}
	if runtime.database == nil || runtime.jobs == nil || runtime.epgScan == nil || runtime.programs == nil ||
		runtime.scanner == nil || runtime.server == nil || runtime.services == nil ||
		runtime.streams == nil || runtime.tuners == nil {
		t.Fatalf("incomplete runtime: %#v", runtime)
	}

	runtime.shutdown()
	if err := database.PingContext(t.Context()); err == nil {
		t.Fatal("runtime shutdown left the database open")
	}
}

func TestBuildRuntimeRegistersRuntimeMetrics(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := db.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	cfg := &config.Config{System: &config.SystemConfig{
		Addresses:              []config.ServerAddress{{Http: "127.0.0.1:0"}},
		DataBroadcastCachePath: ":memory:",
		MaxConcurrentJobs:      1,
		EpgRetrievalTime:       5_000,
		EpgStaleAfter:          7_200_000,
		LogoGatherTimeout:      1_200_000,
		ServiceScanTimeout:     30_000,
	}}
	obs := observability.SetupResult{
		LogStore:      observability.NewLogStore(16),
		MeterProvider: provider,
		Shutdown:      provider.Shutdown,
	}

	runtime, message, err := buildRuntime(cfg, database, obs)
	if err != nil {
		t.Fatalf("buildRuntime() message=%q err=%v", message, err)
	}
	t.Cleanup(runtime.shutdown)
	jobID, err := runtime.jobs.EnqueueDefinition(job.JobDefinition{
		Key:     "metrics-test",
		Name:    "Metrics Test",
		Handler: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.jobs.Wait(t.Context(), jobID); err != nil {
		t.Fatal(err)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		observability.MetricStreamSessionsActive,
		observability.MetricTunerDevices,
		observability.MetricTunerUsers,
		observability.MetricJobCount,
		observability.MetricEPGProgramsStored,
		observability.MetricEPGServicesStale,
		observability.MetricEPGServicesFailed,
		observability.MetricTunerProcessUptime,
		observability.MetricEventsSubscribers,
		observability.MetricLogsSubscribers,
	} {
		if !hasMetric(data, name) {
			t.Fatalf("collected metrics missing %s: %#v", name, data.ScopeMetrics)
		}
	}
}

func TestStartupQueuePolicyUsesCurrentState(t *testing.T) {
	tests := []struct {
		name         string
		serviceCount int
		state        channelConfigState
		stale        int
		wantService  bool
		wantEPG      bool
	}{
		{name: "empty cache scans services", serviceCount: 0, wantService: true},
		{name: "changed persisted channels rescan", serviceCount: 2, state: channelConfigState{storedHash: "old", currentHash: "new"}, wantService: true},
		{name: "stale EPG gathers", serviceCount: 2, state: channelConfigState{storedHash: "same", currentHash: "same"}, stale: 1, wantEPG: true},
		{name: "fresh populated cache does nothing", serviceCount: 2, state: channelConfigState{storedHash: "same", currentHash: "same"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := job.NewManager(job.Config{MaxHistory: 10})
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			for _, key := range []string{job.ServiceUpdaterKey, job.EPGGathererKey} {
				mgr.Register(job.JobDefinition{Key: key, Handler: func(ctx context.Context) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}})
			}

			enqueueStartupServiceUpdate(mgr, tt.serviceCount, tt.state)
			enqueueStartupEPGGather(mgr, tt.serviceCount, tt.stale)
			active := mgr.GetActiveJobKeysByPrefix("")
			if got := containsKey(active, job.ServiceUpdaterKey); got != tt.wantService {
				t.Errorf("service updater queued=%v, want %v; active=%v", got, tt.wantService, active)
			}
			if got := containsKey(active, job.EPGGathererKey); got != tt.wantEPG {
				t.Errorf("EPG gatherer queued=%v, want %v; active=%v", got, tt.wantEPG, active)
			}

			close(release)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := mgr.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestMissingScannedChannelsFindsOnlyConfiguredEmptyChannels(t *testing.T) {
	ctx := t.Context()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	disabled := true
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "26"},
		{Type: "GR", Channel: "26"},
		{Type: "GR", Channel: "25", IsDisabled: &disabled},
	}
	manager := service.NewServiceManager(store, channels)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := servicescan.NewService(manager, nil, channels, 0)

	missing, err := missingScannedChannels(ctx, manager, scanner.Channels())
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0].Type != "GR" || missing[0].ID != "26" {
		t.Fatalf("missing channels = %#v, want GR/26 only", missing)
	}
}

func TestStartupEnqueuesOnlyUnscannedChannelScans(t *testing.T) {
	mgr, err := job.NewManager(job.Config{MaxHistory: 10, MaxConcurrentJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	scanner := &blockingStartupScanner{release: release}
	enqueueStartupServiceScans(t.Context(), mgr, scanner, startupEPGGatherer{}, []servicescan.Channel{
		{Type: "GR", ID: "26"},
		{Type: "BS", ID: "101"},
	})
	active := mgr.GetActiveJobKeysByPrefix("service-scan:")
	if !containsKey(active, "service-scan:GR:26") || !containsKey(active, "service-scan:BS:101") {
		t.Fatalf("active service scans = %v, want GR/26 and BS/101", active)
	}

	close(release)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := mgr.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestHashChannelConfigIsStableAndSensitiveToRoutes(t *testing.T) {
	base := config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}}
	if hashChannelConfig(base) != hashChannelConfig(append(config.ChannelsConfig(nil), base...)) {
		t.Fatal("equivalent channel configurations produced different hashes")
	}
	changed := config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", Routes: []config.ChannelRouteConfig{{Id: "catv", Type: "CATV", Channel: "C27"}}}}
	if hashChannelConfig(base) == hashChannelConfig(changed) {
		t.Fatal("route change was not reflected in channel configuration hash")
	}
}

type blockingStartupScanner struct {
	release <-chan struct{}
}

func (s *blockingStartupScanner) Channels() []servicescan.Channel { return nil }

func (s *blockingStartupScanner) ScanChannel(ctx context.Context, _, _ string, _ bool) ([]uint16, error) {
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type startupEPGGatherer struct{}

func (startupEPGGatherer) Groups(context.Context) (map[uint16]*epg.Network, error) {
	return nil, nil
}

func (startupEPGGatherer) BuildNetworkInputs(context.Context, uint16) ([]epg.Candidate, []epg.ServiceKey, error) {
	return nil, nil, nil
}

func (startupEPGGatherer) GatherNetwork(context.Context, uint16, []epg.Candidate, []epg.ServiceKey) error {
	return nil
}

func (startupEPGGatherer) Cleanup(context.Context, time.Time) error {
	return nil
}

func containsKey(keys []string, want string) bool {
	for _, key := range keys {
		if key == want {
			return true
		}
	}
	return false
}

func hasMetric(data metricdata.ResourceMetrics, name string) bool {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name == name {
				return true
			}
		}
	}
	return false
}
