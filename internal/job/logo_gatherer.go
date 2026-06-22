package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/ts"
)

const (
	LogoGathererKey             = "logo-gatherer"
	LogoGathererName            = "Logo Gatherer"
	LogoGathererDefaultSchedule = "15 3 * * *"
)

func RegisterLogoGatherer(registry Registry, scanner ServiceScanner, collector LogoCollector, updater LogoUpdater, duration time.Duration) {
	if duration <= 0 {
		duration = 24 * time.Hour
	}
	registry.Register(JobDefinition{
		Key: LogoGathererKey, Name: LogoGathererName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			queued := 0
			for _, configured := range scanner.Channels() {
				if err := ctx.Err(); err != nil {
					return err
				}
				channel := configured
				definition := JobDefinition{
					Key:          fmt.Sprintf("logo-gather:%s:%s", channel.Type, channel.ID),
					Name:         fmt.Sprintf("Logo Gather %s/%s", channel.Type, channel.ID),
					IsRerunnable: true,
					Handler: func(childCtx context.Context) error {
						gatherCtx, cancel := context.WithTimeout(childCtx, duration)
						defer cancel()
						count := 0
						err := collector.CollectLogos(gatherCtx, channel.Type, channel.ID, func(image *ts.LogoImage) error {
							count++
							return updater.UpsertLogoImage(childCtx, image)
						})
						if errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && childCtx.Err() == nil) {
							err = nil
						}
						if err != nil {
							return err
						}
						slog.Info("logo gather completed", "channel", fmt.Sprintf("%s/%s", channel.Type, channel.ID), "logos", count, "duration", duration)
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
			slog.Info("logo gatherer dispatched", "queued", queued)
			return nil
		},
	})
}
