package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/job"
	"github.com/21S1298001/Mahiron5/server"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/web"
)

func main() {
	cfg, err := config.LoadAndParseConfig()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
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
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	tm := tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: cfg.Tuners,
	})

	sm := service.NewServiceManager(&service.ServiceManagerConfig{
		Channels: cfg.Channels,
	})

	stm := stream.NewStreamManager(stream.StreamManagerConfig{
		Channels:     cfg.Channels,
		TunerManager: tm,
	})

	jm, err := job.NewManager(job.Config{MaxHistory: 100})
	if err != nil {
		slog.Error("failed to create job manager", "err", err)
		os.Exit(1)
	}

	job.RegisterServiceUpdater(jm, sm, stm, tm, cfg.Channels)
	job.RegisterEPGGatherer(jm)

	schedules := cfg.System.Jobs
	if len(schedules) == 0 {
		schedules = []config.JobScheduleConfig{
			{Key: job.ServiceUpdaterKey, Schedule: job.ServiceUpdaterDefaultSchedule},
			{Key: job.EPGGathererKey, Schedule: job.EPGGathererDefaultSchedule},
		}
		slog.Info("no job schedules in config, using defaults")
	}

	for _, js := range schedules {
		if err := jm.AddSchedule(js.Key, js.Schedule); err != nil {
			slog.Error("failed to add job schedule", "key", js.Key, "err", err)
		}
	}

	handler, err := web.NewWeb(web.WebConfig{
		ServiceManager: sm,
		StreamManager:  stm,
		TunerManager:   tm,
		JobManager:     jm,
	})
	if err != nil {
		slog.Error("failed to create web handler", "err", err)
		os.Exit(1)
	}

	addresses := make([]server.ListenAddress, len(cfg.System.Addresses))
	for i, addr := range cfg.System.Addresses {
		addresses[i] = server.ListenAddress{
			Http: addr.Http,
			Unix: addr.Unix,
		}
	}

	slog.Info("starting servers")
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	s := server.NewServer(addresses, handler)
	s.ListenAndServe()

	jm.Start()

	if sm.CountServices() == 0 {
		slog.Info("no services cached, running initial service update")
		if _, err := jm.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Error("failed to enqueue initial service update", "err", err)
		}
	}

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
		if err := stm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown streams", "err", err)
		}
		slog.Info("streams shut down")
		slog.Info("shutting down tuner")
		if err := tm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown tuner", "err", err)
		}
		slog.Info("tuner shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down job manager")
		if err := jm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown job manager", "err", err)
		}
		slog.Info("job manager shut down")
	}()
	wg.Wait()

	slog.Info("exiting")
	os.Exit(0)
}
