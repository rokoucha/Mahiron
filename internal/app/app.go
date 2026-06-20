package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/epg"
	"github.com/21S1298001/Mahiron5/internal/event"
	"github.com/21S1298001/Mahiron5/internal/filter"
	"github.com/21S1298001/Mahiron5/internal/job"
	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/server"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/servicescan"
	"github.com/21S1298001/Mahiron5/internal/stream"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/21S1298001/Mahiron5/internal/web"
)

func Run(ctx context.Context) int {
	cfg, err := config.LoadAndParseConfig()
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
		EITCollector:   epg.NewMirakcAribCollector(),
		EITUpdater:     epgUpdater,
		Filter:         filter.NewServiceFilter(),
		ProgramUpdater: programs,
		Scanner:        servicescan.NewMirakcAribScanner(),
		TunerManager:   tuners,
	})
	serviceScanner := stream.NewServiceScannerAdapter(streams)
	epgStreams := stream.NewEPGCollectorAdapter(streams)
	apiStreams := stream.NewAPIStreamAdapter(streams)
	scanService := servicescan.NewService(services, serviceScanner, cfg.Channels)
	epgService := epg.NewService(programs, services, epgStreams, cfg.Channels, cfg.System.EpgRetentionDays, time.Duration(cfg.System.EpgRetrievalTime)*time.Millisecond)

	jobs, err := job.NewManager(job.Config{MaxHistory: 100, MaxRunning: cfg.System.JobMaxRunning})
	if err != nil {
		slog.Error("failed to create job manager", "err", err)
		return 1
	}

	job.RegisterServiceUpdater(jobs, scanService, epgService)
	job.RegisterEPGGathererService(jobs, epgService)

	schedules := cfg.System.Jobs
	if len(schedules) == 0 {
		schedules = []config.JobScheduleConfig{
			{Key: job.ServiceUpdaterKey, Schedule: job.ServiceUpdaterDefaultSchedule},
			{Key: job.EPGGathererKey, Schedule: job.EPGGathererDefaultSchedule},
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
		StreamManager:  apiStreams,
		TunerManager:   tuners,
		JobManager:     jobs,
		LogStore:       obs.LogStore,
		EventHub:       events,
		EpgStaleAfter:  int64(cfg.System.EpgStaleAfter),
	})
	if err != nil {
		slog.Error("failed to create web handler", "err", err)
		return 1
	}

	addresses := make([]server.ListenAddress, len(cfg.System.Addresses))
	for i, addr := range cfg.System.Addresses {
		addresses[i] = server.ListenAddress{
			Http: addr.Http,
			Unix: addr.Unix,
		}
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	s := server.NewServer(addresses, handler)
	jobs.Start()

	if err := runStartupTasks(signalCtx, services, programs, jobs, database, cfg); err != nil {
		slog.Error("startup tasks failed", "err", err)
	}
	if err := services.SeedEventLog(signalCtx); err != nil {
		slog.Warn("failed to seed service events", "err", err)
	}
	tuners.SeedEventLog()

	slog.Info("starting servers")
	s.ListenAndServe()

	<-signalCtx.Done()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		slog.Info("shutting down servers")
		if err := s.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown servers", "err", err)
		}
		slog.Info("servers shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down streams")
		if err := streams.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown streams", "err", err)
		}
		slog.Info("streams shut down")
		slog.Info("shutting down tuner")
		if err := tuners.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown tuner", "err", err)
		}
		slog.Info("tuner shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down job manager")
		if err := jobs.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown job manager", "err", err)
		}
		slog.Info("job manager shut down")
	}()
	wg.Wait()

	slog.Info("closing database")
	if err := database.Close(); err != nil {
		slog.Error("failed to close database", "err", err)
	}
	slog.Info("database closed")

	observabilityCtx, observabilityCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer observabilityCancel()
	slog.Info("shutting down observability")
	if err := obs.Shutdown(observabilityCtx); err != nil {
		slog.Error("failed to shutdown observability", "err", err)
	}
	slog.Info("observability shut down")

	slog.Info("exiting")
	return 0
}

func runStartupTasks(ctx context.Context, services *service.ServiceManager, programs *program.ProgramManager, jobs *job.JobManager, database *sql.DB, cfg *config.Config) error {
	if err := services.ReconcileChannels(ctx); err != nil {
		return fmt.Errorf("reconcile service channels: %w", err)
	}
	channelsHash := hashChannelConfig(cfg.Channels)
	storedHash, err := readMetadata(ctx, database, "channels_hash")
	if err != nil {
		slog.Warn("failed to read channels hash", "err", err)
	}
	if storedHash == "" || storedHash != channelsHash {
		slog.Info("channel config changed, will trigger service update")
		if err := writeMetadata(ctx, database, "channels_hash", channelsHash); err != nil {
			slog.Warn("failed to write channels hash", "err", err)
		}
	}

	count, err := services.CountServices(ctx)
	if err != nil {
		return fmt.Errorf("count services: %w", err)
	}

	if count == 0 {
		slog.Info("no services cached, running initial service update")
		if _, err := jobs.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Error("failed to enqueue initial service update", "err", err)
		}
	} else if storedHash != "" && storedHash != channelsHash {
		slog.Info("channel config changed, enqueuing service update")
		if _, err := jobs.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Warn("failed to enqueue service update", "err", err)
		}
	}

	stale, _, _, err := services.EPGSummary(ctx, int64(cfg.System.EpgStaleAfter), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("read EPG status: %w", err)
	}
	// EPG gathering requires a non-empty service list. If we don't have one
	// yet, the service updater above is responsible for populating it; each
	// scan that discovers a new network will immediately enqueue an EPG
	// gather, so we don't need to enqueue the gatherer here. For the stale
	// case (services exist but EPG is outdated), enqueue the gatherer to
	// refresh all networks.
	if count > 0 && stale > 0 {
		slog.Info("EPG is stale, enqueuing gatherer", "staleServices", stale)
		if _, err := jobs.Enqueue(job.EPGGathererKey); err != nil && !errors.Is(err, job.ErrJobAlreadyRunning) {
			slog.Warn("failed to enqueue startup EPG gathering", "err", err)
		}
	}

	if cfg.System.EpgRetentionDays > 0 {
		cutoff := time.Now().Add(-time.Duration(cfg.System.EpgRetentionDays) * 24 * time.Hour).UnixMilli()
		if err := programs.DeleteEndedBefore(ctx, cutoff); err != nil {
			slog.Warn("failed to clean up old EPG data", "err", err)
		} else {
			slog.Info("cleaned up EPG data", "cutoffDays", cfg.System.EpgRetentionDays)
		}
	}

	return nil
}

func hashChannelConfig(channels config.ChannelsConfig) string {
	data, err := json.Marshal(channels)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func readMetadata(ctx context.Context, db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func writeMetadata(ctx context.Context, db *sql.DB, key, value string) error {
	_, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", key, value)
	return err
}
