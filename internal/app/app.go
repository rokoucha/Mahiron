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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/server"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/internal/web"
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

	database, err := db.Open(cfg.System.DatabasePath)
	if err != nil {
		slog.Error("failed to open database", "err", err)
		return 1
	}

	if err := db.Migrate(context.Background(), database); err != nil {
		slog.Error("failed to run migrations", "err", err)
		return 1
	}

	runtime, errMessage, err := buildRuntime(cfg, database, obs)
	if err != nil {
		slog.Error(errMessage, "err", err)
		return 1
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	runtime.jobs.Start()
	runtime.streams.StartRemoteProgramEventSync(signalCtx)

	if err := runStartupTasks(signalCtx, runtime.services, runtime.programs, runtime.jobs, runtime.scanner, runtime.epgScan, runtime.database, cfg); err != nil {
		slog.Error("startup tasks failed", "err", err)
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
	database *sql.DB
	jobs     *job.JobManager
	obs      observability.SetupResult
	epgScan  *epg.Service
	programs *program.ProgramManager
	server   *server.Server
	scanner  *servicescan.Service
	services *service.ServiceManager
	streams  *stream.StreamManager
	tuners   *tuner.TunerManager
}

func buildRuntime(cfg *config.Config, database *sql.DB, obs observability.SetupResult) (*applicationRuntime, string, error) {
	serviceStore := service.NewSQLiteStore(database)
	programStore := program.NewSQLiteStore(database)
	events := event.New()

	tuners := tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: cfg.Tuners,
		EventHub:     events,
	})

	services := service.NewServiceManager(serviceStore, cfg.Channels, events)

	programs := program.NewProgramManager(programStore, events)
	epgUpdater := epg.NewUpdater(programs)

	streams := stream.NewStreamManager(stream.StreamManagerConfig{
		Channels:       cfg.Channels,
		Remotes:        cfg.Remotes,
		EITUpdater:     epgUpdater,
		LogoUpdater:    services,
		ProgramUpdater: programs,
		ServiceLister:  services,
		TunerManager:   tuners,
	})
	serviceScanner := stream.NewServiceScannerAdapter(streams)
	logoCollector := stream.NewLogoCollectorAdapter(streams)
	scanService := servicescan.NewService(services, serviceScanner, cfg.Channels, time.Duration(cfg.System.ServiceScanTimeout)*time.Millisecond)
	epgService := epg.NewService(programs, services, streams, cfg.Channels, cfg.System.EpgRetentionDays, time.Duration(cfg.System.EpgRetrievalTime)*time.Millisecond)

	jobs, err := job.NewManager(job.Config{MaxHistory: 100, MaxConcurrentJobs: cfg.System.MaxConcurrentJobs}, events)
	if err != nil {
		return nil, "failed to create job manager", err
	}

	job.RegisterServiceUpdater(jobs, scanService, epgService)
	job.RegisterEPGGathererService(jobs, epgService)
	job.RegisterLogoGatherer(jobs, logoCollector, services, time.Duration(cfg.System.LogoGatherTimeout)*time.Millisecond)

	schedules := cfg.System.Jobs
	if len(schedules) == 0 {
		schedules = []config.JobScheduleConfig{
			{Key: job.ServiceUpdaterKey, Schedule: job.ServiceUpdaterDefaultSchedule},
			{Key: job.EPGGathererKey, Schedule: job.EPGGathererDefaultSchedule},
			{Key: job.LogoGathererKey, Schedule: job.LogoGathererDefaultSchedule},
		}
		slog.Info("no job schedules in config, using defaults")
	}

	for _, js := range schedules {
		if err := jobs.AddSchedule(js.Key, js.Schedule); err != nil {
			slog.Error("failed to add job schedule", "key", js.Key, "err", err)
		}
	}

	handler, err := web.NewWeb(web.WebConfig{
		ServiceManager: services,
		ProgramManager: programs,
		StreamManager:  streams,
		TunerManager:   tuners,
		JobManager:     jobs,
		LogStore:       obs.LogStore,
		EventHub:       events,
		EpgStaleAfter:  int64(cfg.System.EpgStaleAfter),
		MeterProvider:  obs.MeterProvider,
		TracerProvider: obs.TracerProvider,
	})
	if err != nil {
		return nil, "failed to create web handler", err
	}

	registerRuntimeMetrics(obs.MeterProvider, streams, tuners, jobs, programs, services, events, obs.LogStore, int64(cfg.System.EpgStaleAfter))

	return &applicationRuntime{
		database: database,
		jobs:     jobs,
		obs:      obs,
		epgScan:  epgService,
		programs: programs,
		server:   server.NewServer(listenAddresses(cfg), handler),
		scanner:  scanService,
		services: services,
		streams:  streams,
		tuners:   tuners,
	}, "", nil
}

func listenAddresses(cfg *config.Config) []server.ListenAddress {
	addresses := make([]server.ListenAddress, len(cfg.System.Addresses))
	for i, addr := range cfg.System.Addresses {
		addresses[i] = server.ListenAddress{Http: addr.Http, Unix: addr.Unix}
	}
	return addresses
}

func (r *applicationRuntime) shutdown() {
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

func runStartupTasks(ctx context.Context, services *service.ServiceManager, programs *program.ProgramManager, jobs *job.JobManager, scanner *servicescan.Service, epgScan *epg.Service, database *sql.DB, cfg *config.Config) error {
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
		missing, err := missingScannedChannels(ctx, services, scanner.Channels())
		if err != nil {
			return fmt.Errorf("find unscanned channels: %w", err)
		}
		enqueueStartupServiceScans(ctx, jobs, scanner, epgScan, missing)
	}

	stale, _, _, err := services.EPGSummary(ctx, int64(cfg.System.EpgStaleAfter), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("read EPG status: %w", err)
	}
	enqueueStartupEPGGather(jobs, count, stale)
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

func loadChannelConfigState(ctx context.Context, database *sql.DB, channels config.ChannelsConfig) channelConfigState {
	state := channelConfigState{currentHash: hashChannelConfig(channels)}
	storedHash, err := readMetadata(ctx, database, "channels_hash")
	if err != nil {
		slog.Warn("failed to read channels hash", "err", err)
	}
	state.storedHash = storedHash
	if state.changed() {
		slog.Info("channel config changed, will trigger service update")
		if err := writeMetadata(ctx, database, "channels_hash", state.currentHash); err != nil {
			slog.Warn("failed to write channels hash", "err", err)
		}
	}
	return state
}

func enqueueStartupServiceUpdate(jobs *job.JobManager, serviceCount int, channelState channelConfigState) bool {
	if serviceCount == 0 {
		slog.Info("no services cached, running initial service update")
		if _, err := jobs.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Error("failed to enqueue initial service update", "err", err)
			return false
		}
		return true
	}
	if channelState.previouslyStored() && channelState.changed() {
		slog.Info("channel config changed, enqueuing service update")
		if _, err := jobs.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Warn("failed to enqueue service update", "err", err)
			return false
		}
		return true
	}
	return false
}

func missingScannedChannels(ctx context.Context, services *service.ServiceManager, channels []servicescan.Channel) ([]servicescan.Channel, error) {
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

func enqueueStartupServiceScans(ctx context.Context, jobs *job.JobManager, scanner job.ServiceScanner, epgScan job.EPGGatherer, channels []servicescan.Channel) {
	if len(channels) == 0 {
		return
	}
	queued, err := job.EnqueueServiceScans(ctx, jobs, scanner, epgScan, channels)
	if err != nil {
		slog.Warn("failed to enqueue startup service scans", "err", err)
		return
	}
	slog.Info("unscanned channels found, enqueued service scans", "queued", queued, "channels", len(channels))
}

func enqueueStartupEPGGather(jobs *job.JobManager, serviceCount int, staleServices int) {
	// EPG gathering requires a non-empty service list. If we don't have one
	// yet, the service updater above is responsible for populating it; each
	// scan that discovers a new network will immediately enqueue an EPG
	// gather, so we don't need to enqueue the gatherer here. For the stale
	// case (services exist but EPG is outdated), enqueue the gatherer to
	// refresh all networks.
	if serviceCount > 0 && staleServices > 0 {
		slog.Info("EPG is stale, enqueuing gatherer", "staleServices", staleServices)
		if _, err := jobs.Enqueue(job.EPGGathererKey); err != nil && !errors.Is(err, job.ErrJobAlreadyRunning) {
			slog.Warn("failed to enqueue startup EPG gathering", "err", err)
		}
	}
}

func cleanupOldEPG(ctx context.Context, programs *program.ProgramManager, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour).UnixMilli()
	if err := programs.DeleteEndedBefore(ctx, cutoff); err != nil {
		slog.Warn("failed to clean up old EPG data", "err", err)
	} else {
		slog.Info("cleaned up EPG data", "cutoffDays", retentionDays)
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
