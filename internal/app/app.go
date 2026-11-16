package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/mahiron/internal/agent"
	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/bml/cache"
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/defs"
	"github.com/21S1298001/mahiron/internal/logogather"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/server"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/internal/web"
	"github.com/21S1298001/mahiron/internal/web/api"
)

type runOptions struct {
	ConfigDir string
}

func parseRunOptions(args []string, output io.Writer) (runOptions, error) {
	options := runOptions{ConfigDir: config.DefaultConfigDir}
	flags := flag.NewFlagSet("mahiron", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&options.ConfigDir, "config-dir", options.ConfigDir, "directory containing configuration files")
	if err := flags.Parse(args); err != nil {
		return runOptions{}, err
	}
	if flags.NArg() > 0 {
		return runOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return options, nil
}

func Run(ctx context.Context, args []string) int {
	options, err := parseRunOptions(args, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		slog.Error("failed to parse arguments", "err", err)
		return 1
	}

	cfg, err := config.LoadAndParseConfigFromDir(options.ConfigDir)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return 1
	}

	level := slog.LevelInfo
	switch cfg.System.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	obs := observability.Setup(ctx, cfg.System.Observability, level)

	slog.Info("opening database")
	database, err := db.Open(cfg.System.DatabasePath)
	if err != nil {
		slog.Error("failed to open database", "err", err)
		return 1
	}
	slog.Info("database opened", "vfs", db.VFSName(cfg.System.DatabasePath))

	slog.Info("running database migrations")
	if err := db.Migrate(context.Background(), database); err != nil {
		slog.Error("failed to run migrations", "err", err)
		return 1
	}
	slog.Info("database migrations completed")

	runtime, errMessage, err := buildRuntime(cfg, database, obs)
	if err != nil {
		slog.Error(errMessage, "err", err)
		return 1
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	stopCheckpointer := db.StartWALCheckpointer(context.Background(), database, db.DefaultWALCheckpointInterval)
	runtime.stopCheckpointer = stopCheckpointer

	runtime.jobs.Start()
	runtime.streams.StartRemoteProgramEventSync(signalCtx)

	if err := runStartupTasks(signalCtx, runtime.services, runtime.programs, runtime.jobs, runtime.serviceScanner, runtime.epgGatherer, runtime.database, cfg); err != nil {
		slog.Error("startup tasks failed", "err", err)
	}
	if runtime.epgController != nil {
		controllerCtx, stopController := context.WithCancel(signalCtx)
		runtime.stopEPGController = stopController
		runtime.epgControllerDone = make(chan struct{})
		go func() { defer close(runtime.epgControllerDone); runtime.epgController.Run(controllerCtx) }()
	}
	if err := runtime.services.SeedEventLog(signalCtx); err != nil {
		slog.Warn("failed to seed service events", "err", err)
	}
	runtime.tuners.SeedEventLog()

	slog.Info("starting servers")
	runtime.server.ListenAndServe(signalCtx)

	<-signalCtx.Done()
	runtime.shutdown()

	slog.Info("exiting")
	return 0
}

type applicationRuntime struct {
	dataBroadcastStore *cache.SQLiteModuleStore
	database           *db.DB
	jobs               *job.Manager
	obs                observability.SetupResult
	epgGatherer        *epggather.Gatherer
	epgController      *defs.EPGController
	stopEPGController  context.CancelFunc
	epgControllerDone  chan struct{}
	programs           *program.Manager
	server             *server.Server
	serviceScanner     *servicescan.Scanner
	services           *service.Manager
	streams            *stream.Manager
	tuners             *tuner.Manager
	agents             *agent.Hub
	stopCheckpointer   func()
}

func buildRuntime(cfg *config.Config, database *db.DB, obs observability.SetupResult) (*applicationRuntime, string, error) {
	epg, err := cfg.System.ResolveEPG()
	if err != nil {
		return nil, "invalid EPG configuration", err
	}
	serviceStore := service.NewSQLiteStore(database)
	programStore := program.NewSQLiteStore(database)
	events := event.New()

	credentials := map[string]string{}
	for _, a := range cfg.System.Agents {
		credentials[a.Name] = a.TokenHash
	}
	agents := agent.NewHub(credentials)
	tuners := tuner.NewManager(&tuner.ManagerConfig{
		TunersConfig: cfg.Tuners,
		Agents:       agents,
		EventHub:     events,
	})

	apiEvents := mirakurun.NewEventPublisher(events)
	services := service.NewManager(serviceStore, cfg.Channels, api.NewServiceEventPublisher(apiEvents))

	programs := program.NewManager(programStore, apiEvents)

	dataBroadcastEnabled := config.IsDataBroadcastEnabled(*cfg.System)
	dataBroadcastStore, moduleStore, snapshotStore := openDataBroadcastStores(cfg.System)
	streams := stream.NewManager(stream.ManagerConfig{
		Channels:       cfg.Channels,
		Remotes:        cfg.Remotes,
		EITUpdater:     programs,
		SnapshotStore:  snapshotStore,
		LogoUpdater:    services,
		ProgramUpdater: epggather.NewKnownServiceProgramUpdater(programs, services),
		TunerManager:   tuners,
		ModuleStore:    moduleStore,
	})
	scanAdapter := stream.NewServiceScanAdapter(streams)
	logoGatherer := logogather.NewGatherer(services, stream.NewLogoGatherAdapter(streams), cfg.System.LogoGatherTimeoutDuration())
	serviceScanner := servicescan.NewScanner(services, scanAdapter, cfg.Channels, cfg.System.ServiceScanTimeoutDuration())
	epgGatherer := buildEPGGatherer(cfg, epg.Mode, programs, services, streams)
	runtime := &applicationRuntime{
		dataBroadcastStore: dataBroadcastStore,
		agents:             agents,
		database:           database,
		obs:                obs,
		epgGatherer:        epgGatherer,
		programs:           programs,
		serviceScanner:     serviceScanner,
		services:           services,
		streams:            streams,
		tuners:             tuners,
	}
	jobs, controller, err := runtime.buildJobs(cfg.System, epg, events, logoGatherer)
	if err != nil {
		return nil, "failed to create job manager", err
	}

	var apiModuleStore bml.ModuleStore
	if dataBroadcastEnabled {
		apiModuleStore = moduleStore
	}
	handler, err := web.NewWeb(web.WebConfig{
		ServiceManager:        services,
		ProgramManager:        programs,
		StreamManager:         streams,
		TunerManager:          tuners,
		JobManager:            jobs,
		LogStore:              obs.LogStore,
		EventHub:              events,
		EpgStaleAfter:         int64(cfg.System.EpgStaleAfter),
		DataBroadcastDisabled: !dataBroadcastEnabled,
		BMLStore:              apiModuleStore,
		BMLSnapshotStore:      snapshotStore,
		MeterProvider:         obs.MeterProvider,
		TracerProvider:        obs.TracerProvider,
		Pprof:                 cfg.System.Observability.Pprof.Enabled,
	})
	if err != nil {
		return nil, "failed to create web handler", err
	}

	webHandler := handler
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/agent/v1/") {
			agents.ServeHTTP(w, r)
			return
		}
		webHandler.ServeHTTP(w, r)
	})
	registerRuntimeMetrics(obs.MeterProvider, streams, tuners, jobs, programs, services, events, obs.LogStore, int64(cfg.System.EpgStaleAfter))

	runtime.jobs = jobs
	runtime.epgController = controller
	runtime.server = server.NewServer(listenAddresses(cfg), handler)
	return runtime, "", nil
}

func listenAddresses(cfg *config.Config) []server.ListenAddress {
	addresses := make([]server.ListenAddress, len(cfg.System.Addresses))
	for i, addr := range cfg.System.Addresses {
		addresses[i] = server.ListenAddress{Http: addr.Http, Unix: addr.Unix}
	}
	return addresses
}

func (r *applicationRuntime) shutdown() {
	if r.stopEPGController != nil {
		r.stopEPGController()
		<-r.epgControllerDone
	}
	if r.agents != nil {
		r.agents.Close()
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		slog.Info("shutting down servers")
		if err := r.server.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown servers", "err", err)
		}
		slog.Info("servers shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down streams")
		if err := r.streams.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown streams", "err", err)
		}
		slog.Info("streams shut down")
		slog.Info("shutting down tuner")
		if err := r.tuners.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown tuner", "err", err)
		}
		slog.Info("tuner shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down job manager")
		if err := r.jobs.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown job manager", "err", err)
		}
		slog.Info("job manager shut down")
	}()
	wg.Wait()
	if r.stopCheckpointer != nil {
		slog.Info("stopping wal checkpointer")
		r.stopCheckpointer()
		slog.Info("wal checkpointer stopped")
	}
	if r.dataBroadcastStore != nil {
		slog.Info("closing data broadcast cache")
		if err := r.dataBroadcastStore.Close(); err != nil {
			slog.Error("failed to close data broadcast cache", "err", err)
		}
	}

	slog.Info("closing database")
	if err := r.database.Close(); err != nil {
		slog.Error("failed to close database", "err", err)
	}
	slog.Info("database closed")

	observabilityCtx, observabilityCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer observabilityCancel()
	slog.Info("shutting down observability")
	if err := r.obs.Shutdown(observabilityCtx); err != nil {
		slog.Error("failed to shutdown observability", "err", err)
	}
	slog.Info("observability shut down")
}

func runStartupTasks(ctx context.Context, services *service.Manager, programs *program.Manager, jobs *job.Manager, serviceScanner *servicescan.Scanner, epgGatherer *epggather.Gatherer, database *db.DB, cfg *config.Config) error {
	if err := services.ReconcileChannels(ctx); err != nil {
		return fmt.Errorf("reconcile service channels: %w", err)
	}
	channelState := loadChannelConfigState(ctx, database, cfg.Channels)

	count, err := services.CountServices(ctx)
	if err != nil {
		return fmt.Errorf("count services: %w", err)
	}

	enqueuedFullUpdate := enqueueStartupServiceUpdate(jobs, count, channelState)
	if !enqueuedFullUpdate {
		missing, err := missingScannedChannels(ctx, services, serviceScanner.Channels())
		if err != nil {
			return fmt.Errorf("find unscanned channels: %w", err)
		}
		enqueueStartupServiceScans(ctx, jobs, serviceScanner, epgGatherer, missing)
	}

	stale, _, _, err := services.EPGSummary(ctx, int64(cfg.System.EpgStaleAfter), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("read EPG status: %w", err)
	}
	if cfg.System.EPGMode() == config.EPGScheduled {
		enqueueStartupEPGGather(jobs, count, stale)
	}
	cleanupOldEPG(ctx, programs, cfg.System.EpgRetentionDays)

	return nil
}

type channelConfigState struct {
	currentHash string
	storedHash  string
}

func (s channelConfigState) changed() bool {
	return s.storedHash != s.currentHash
}

func (s channelConfigState) previouslyStored() bool {
	return s.storedHash != ""
}

func loadChannelConfigState(ctx context.Context, database *db.DB, channels config.ChannelsConfig) channelConfigState {
	state := channelConfigState{currentHash: hashChannelConfig(channels)}
	storedHash, err := readMetadata(ctx, database.Read, "channels_hash")
	if err != nil {
		slog.Warn("failed to read channels hash", "err", err)
	}
	state.storedHash = storedHash
	if state.changed() {
		slog.Info("channel config changed, will trigger service update")
		if err := writeMetadata(ctx, database.Write, "channels_hash", state.currentHash); err != nil {
			slog.Warn("failed to write channels hash", "err", err)
		}
	}
	return state
}

func enqueueStartupServiceUpdate(jobs *job.Manager, serviceCount int, channelState channelConfigState) bool {
	if serviceCount == 0 {
		slog.Info("no services cached, running initial service update")
		if _, err := jobs.Enqueue(defs.ServiceUpdaterKey); err != nil {
			slog.Error("failed to enqueue initial service update", "err", err)
			return false
		}
		return true
	}
	if channelState.previouslyStored() && channelState.changed() {
		slog.Info("channel config changed, enqueuing service update")
		if _, err := jobs.Enqueue(defs.ServiceUpdaterKey); err != nil {
			slog.Warn("failed to enqueue service update", "err", err)
			return false
		}
		return true
	}
	return false
}

func missingScannedChannels(ctx context.Context, services *service.Manager, channels []servicescan.Channel) ([]servicescan.Channel, error) {
	missing := make([]servicescan.Channel, 0)
	for _, channel := range channels {
		stored, err := services.GetServicesByChannel(ctx, channel.Type, channel.ID)
		if err != nil {
			return nil, err
		}
		if len(stored) == 0 {
			missing = append(missing, channel)
		}
	}
	return missing, nil
}

func enqueueStartupServiceScans(ctx context.Context, jobs *job.Manager, scanner defs.ServiceScanner, epgScan defs.EPGGatherer, channels []servicescan.Channel) {
	if len(channels) == 0 {
		return
	}
	queued, err := defs.EnqueueServiceScans(ctx, jobs, scanner, epgScan, channels)
	if err != nil {
		slog.Warn("failed to enqueue startup service scans", "err", err)
		return
	}
	slog.Info("unscanned channels found, enqueued service scans", "queued", queued, "channels", len(channels))
}

func enqueueStartupEPGGather(jobs *job.Manager, serviceCount int, staleServices int) {
	// EPG gathering requires a non-empty service list. If we don't have one
	// yet, the service updater above is responsible for populating it; each
	// scan that discovers a new network will immediately enqueue an EPG
	// gather, so we don't need to enqueue the gatherer here. For the stale
	// case (services exist but EPG is outdated), enqueue the gatherer to
	// refresh all networks.
	if serviceCount > 0 && staleServices > 0 {
		slog.Info("EPG is stale, enqueuing gatherer", "staleServices", staleServices)
		if _, err := jobs.Enqueue(defs.EPGGathererKey); err != nil && !errors.Is(err, job.ErrJobAlreadyRunning) {
			slog.Warn("failed to enqueue startup EPG gathering", "err", err)
		}
	}
}

func cleanupOldEPG(ctx context.Context, programs *program.Manager, retentionDays int) {
	if err := programs.DeleteExpired(ctx, time.Now(), retentionDays); err != nil {
		slog.Warn("failed to clean up old EPG data", "err", err)
	}
}

func hashChannelConfig(channels config.ChannelsConfig) string {
	data, err := json.Marshal(channels)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func readMetadata(ctx context.Context, db *sql.DB, key string) (string, error) {
	value, err := gen.New(db).GetMetadataValue(ctx, key)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func writeMetadata(ctx context.Context, db *sql.DB, key, value string) error {
	return gen.New(db).UpsertMetadata(ctx, gen.UpsertMetadataParams{
		Key:   key,
		Value: value,
	})
}

func buildEPGGatherer(cfg *config.Config, mode config.EPGMode, programs *program.Manager, services *service.Manager, streams *stream.Manager) *epggather.Gatherer {
	epgAdapter := stream.NewEPGGatherAdapter(streams)
	epgAdapter.WaitForTuner = mode != config.EPGContinuous
	epgGatherer := epggather.NewGatherer(programs, programs, services, epgAdapter, cfg.Channels, cfg.System.EpgRetrievalTimeDuration())

	return epgGatherer
}

func (r *applicationRuntime) buildJobs(system *config.SystemConfig, epg config.EPGConfig, events *event.Hub, logoGatherer *logogather.Gatherer) (*job.Manager, *defs.EPGController, error) {
	jobs, err := job.NewManager(job.Config{
		MaxHistory: 100, MaxConcurrentJobs: system.MaxConcurrentJobs,
		GroupLimits:       map[string]int{defs.EPGLiveGroup: system.MaxConcurrentJobs, defs.EPGStoredGroup: 1},
		IndependentGroups: map[string]bool{defs.EPGStoredGroup: true},
	}, events)
	if err != nil {
		return nil, nil, err
	}

	var scheduledEPG defs.EPGGatherer = r.epgGatherer
	var controller *defs.EPGController
	if epg.Mode == config.EPGContinuous {
		interval := system.EpgRefreshIntervalDuration()
		if interval <= 0 {
			interval = 30 * time.Minute
		}
		r.epgGatherer.SetReceptionPolicy(epggather.ReceptionPolicy{
			RefreshInterval:       interval,
			ExtendedProbeInterval: system.EpgExtendedProbeIntervalDuration(),
			UnknownExtendedIdle:   system.EpgUnknownExtendedIdleTimeDuration(),
		})
		controller = defs.NewEPGController(jobs, r.epgGatherer, interval, system.MaxConcurrentJobs)
		controller.SetCleaner(r.programs, system.EpgRetentionDays)
		controller.SetResourceChanges(r.tuners.Changes)
		scheduledEPG = controller
		jobs.AddManualSchedule(defs.EPGGathererKey, string(config.EPGContinuous))
	}
	automaticEPG := scheduledEPG
	if epg.Mode == config.EPGDisabled {
		automaticEPG = nil
	}
	defs.RegisterServiceUpdater(jobs, r.serviceScanner, automaticEPG)
	defs.RegisterEPGGatherer(jobs, scheduledEPG, r.programs, system.EpgRetentionDays)
	defs.RegisterLogoGatherer(jobs, logoGatherer)

	if len(system.Jobs) == 0 {
		slog.Info("no job schedules in config, using defaults")
	}
	for _, js := range epg.Schedules {
		if err := jobs.AddSchedule(js.Key, js.Schedule); err != nil {
			slog.Error("failed to add job schedule", "key", js.Key, "err", err)
		}
	}
	return jobs, controller, nil
}

// openDataBroadcastStores owns cache selection for the application.
func openDataBroadcastStores(system *config.SystemConfig) (*cache.SQLiteModuleStore, bml.ModuleStore, bml.SnapshotStore) {
	var dataBroadcastStore *cache.SQLiteModuleStore
	var moduleStore bml.ModuleStore
	var snapshotStore bml.SnapshotStore
	dataBroadcastEnabled := config.IsDataBroadcastEnabled(*system)
	if dataBroadcastEnabled {
		// Opening the cache migrates and prunes it, the only startup step whose
		// cost grows with stored data. Report how long it took so a slow start is
		// attributable from the log alone.
		cachePath := system.DataBroadcastCachePath
		slog.Info("opening data broadcast cache", "path", cachePath)
		cacheOpenedAt := time.Now()
		var err error
		dataBroadcastStore, err = cache.NewSQLiteModuleStoreWithOptions(cachePath, cache.SQLiteModuleStoreOptions{
			MaxBytes:       system.DataBroadcastCacheBytes,
			MaxAge:         time.Duration(system.DataBroadcastCacheMaxAgeDays) * 24 * time.Hour,
			SnapshotMaxAge: time.Duration(system.DataBroadcastSnapshotMaxAgeHours) * time.Hour,
		})
		if err != nil {
			slog.Warn("failed to open data broadcast cache; using memory cache", "err", err, "elapsed", time.Since(cacheOpenedAt))
		} else {
			slog.Info("data broadcast cache opened", "elapsed", time.Since(cacheOpenedAt))
			moduleStore = dataBroadcastStore
			if config.IsDataBroadcastSnapshotEnabled(*system) {
				snapshotStore = dataBroadcastStore
			}
		}
	} else {
		slog.Info("data broadcast API disabled")
	}
	if moduleStore == nil {
		// Keep one shared fallback for streams and the enabled BML API.
		// Streams retain modules even when the API is disabled.
		moduleStore = cache.NewModuleCache(0)
	}
	return dataBroadcastStore, moduleStore, snapshotStore
}
