package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/service"
)

const (
	ServiceUpdaterKey             = "service-updater"
	ServiceUpdaterName            = "Service Updater"
	ServiceUpdaterDefaultSchedule = "5 6 * * *"
)

func RegisterServiceUpdater(registry Registry, programs EPGProgramStore, services ServiceScanner, streamScanner service.StreamScanner, epgStreams EPGStreamManager, channels config.ChannelsConfig, retrievalTime time.Duration) {
	registry.Register(JobDefinition{
		Key: ServiceUpdaterKey, Name: ServiceUpdaterName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			queued := 0
			for _, configured := range channels {
				if err := ctx.Err(); err != nil {
					return err
				}
				if configured.IsDisabled != nil && *configured.IsDisabled {
					continue
				}
				channel := configured
				definition := JobDefinition{
					Key:          fmt.Sprintf("service-scan:%s:%s", channel.Type, channel.Channel),
					Name:         fmt.Sprintf("Service Scan %s/%s", channel.Type, channel.Channel),
					IsRerunnable: true,
					Handler: func(childCtx context.Context) error {
						newNIDs, err := services.ScanServicesWait(childCtx, streamScanner, channel.Type, channel.Channel)
						if err != nil {
							return err
						}
						for _, nid := range newNIDs {
							if err := childCtx.Err(); err != nil {
								return err
							}
							if _, err := enqueueEPGGatherForNetwork(childCtx, registry, programs, services, epgStreams, channels, retrievalTime, nid, nil, nil); err != nil {
								slog.Warn("failed to enqueue EPG gather for newly scanned network", "networkId", nid, "channel", fmt.Sprintf("%s/%s", channel.Type, channel.Channel), "err", err)
							}
						}
						if len(newNIDs) > 0 {
							slog.Info("service scan discovered new networks, EPG gather enqueued", "channel", fmt.Sprintf("%s/%s", channel.Type, channel.Channel), "networks", newNIDs)
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
