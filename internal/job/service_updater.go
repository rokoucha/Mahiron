package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

const (
	ServiceUpdaterKey             = "service-updater"
	ServiceUpdaterName            = "Service Updater"
	ServiceUpdaterDefaultSchedule = "5 6 * * *"
)

func RegisterServiceUpdater(registry Registry, scanner ServiceScanner, epgService EPGGatherer) {
	registry.Register(JobDefinition{
		Key: ServiceUpdaterKey, Name: ServiceUpdaterName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			queued := 0
			for _, configured := range scanner.Channels() {
				if err := ctx.Err(); err != nil {
					return err
				}
				channel := configured
				definition := JobDefinition{
					Key:          fmt.Sprintf("service-scan:%s:%s", channel.Type, channel.ID),
					Name:         fmt.Sprintf("Service Scan %s/%s", channel.Type, channel.ID),
					IsRerunnable: true,
					Handler: func(childCtx context.Context) error {
						newNIDs, err := scanner.ScanChannel(childCtx, channel.Type, channel.ID, true)
						if err != nil {
							return err
						}
						for _, nid := range newNIDs {
							if err := childCtx.Err(); err != nil {
								return err
							}
							if _, err := enqueueEPGGatherForNetwork(childCtx, registry, epgService, nid, nil, nil); err != nil {
								slog.Warn("failed to enqueue EPG gather for newly scanned network", "networkId", nid, "channel", fmt.Sprintf("%s/%s", channel.Type, channel.ID), "err", err)
							}
						}
						if len(newNIDs) > 0 {
							slog.Info("service scan discovered new networks, EPG gather enqueued", "channel", fmt.Sprintf("%s/%s", channel.Type, channel.ID), "networks", newNIDs)
						}
						return nil
					},
				}
				if _, err := registry.EnqueueDefinition(definition); err != nil {
					if errors.Is(err, ErrJobAlreadyRunning) {
						continue
					}
					return err
				}
				queued++
			}
			slog.Info("service updater dispatched", "queued", queued)
			return nil
		},
	})
}
