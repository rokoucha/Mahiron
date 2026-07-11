package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/servicescan"
	"github.com/21S1298001/mahiron/internal/tuner"
)

const (
	ServiceUpdaterKey             = "service-updater"
	ServiceUpdaterName            = "Service Updater"
	ServiceUpdaterDefaultSchedule = "5 6 * * *"
	serviceUpdateEPGGathererKey   = "epg-gather-after-service-update"
)

func RegisterServiceUpdater(registry Registry, scanner ServiceScanner, epgService EPGGatherer) {
	registry.Register(JobDefinition{
		Key: ServiceUpdaterKey, Name: ServiceUpdaterName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			channels := scanner.Channels()
			queued, err := EnqueueServiceScans(ctx, registry, scanner, epgService, channels)
			if err != nil {
				return err
			}
			// Build EPG inputs only after every scan has committed its result. This
			// is essential for satellite networks, whose services are distributed
			// across several transponders but share one original network ID.
			if _, err := registry.EnqueueDefinition(JobDefinition{
				Key:           serviceUpdateEPGGathererKey,
				Name:          "EPG Gatherer After Service Update",
				Handler:       epgGathererHandler(registry, epgService),
				DependsOn:     serviceScanJobKeys(channels),
				ExclusiveKeys: []string{"epg-service-topology"},
				IsRerunnable:  true,
				RetryDelays:   []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
			}); err != nil && !errors.Is(err, ErrJobAlreadyRunning) {
				return fmt.Errorf("enqueue EPG gather after service scans: %w", err)
			}
			run.Set(ctx, run.Result{
				Kind:    "service_updater",
				Summary: fmt.Sprintf("%d/%d channel scans queued", queued, len(channels)),
				Counts: map[string]int{
					"channels": len(channels),
					"queued":   queued,
				},
			})
			slog.Info("service updater dispatched", "queued", queued)
			return nil
		},
	})
}

func serviceScanJobKeys(channels []servicescan.Channel) []string {
	keys := make([]string, len(channels))
	for i, channel := range channels {
		keys[i] = fmt.Sprintf("service-scan:%s:%s", channel.Type, channel.ID)
	}
	return keys
}

func EnqueueServiceScans(ctx context.Context, registry Registry, scanner ServiceScanner, _ EPGGatherer, channels []servicescan.Channel) (int, error) {
	queued := 0
	for _, configured := range channels {
		if err := ctx.Err(); err != nil {
			return queued, err
		}
		channel := configured
		definition := JobDefinition{
			Key:           fmt.Sprintf("service-scan:%s:%s", channel.Type, channel.ID),
			Name:          fmt.Sprintf("Service Scan %s/%s", channel.Type, channel.ID),
			ExclusiveKeys: []string{"epg-service-topology"},
			IsRerunnable:  true,
			RetryDelays:   []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute},
			RetryIf: func(err error) bool {
				return errors.Is(err, tuner.ErrTunerUnavailable)
			},
			Handler: func(childCtx context.Context) error {
				_, err := scanner.ScanChannel(childCtx, channel.Type, channel.ID, false)
				return err
			},
		}
		if _, err := registry.EnqueueDefinition(definition); err != nil {
			if errors.Is(err, ErrJobAlreadyRunning) {
				continue
			}
			return queued, err
		}
		queued++
	}
	return queued, nil
}
