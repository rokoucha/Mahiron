package job

import (
	"context"
	"log/slog"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
)

const (
	ServiceUpdaterKey  = "service-updater"
	ServiceUpdaterName = "Service Updater"

	ServiceUpdaterDefaultSchedule = "5 6 * * *"
)

func RegisterServiceUpdater(mgr *JobManager, sm *service.ServiceManager, stm *stream.StreamManager, tm *tuner.TunerManager, channels config.ChannelsConfig) {
	mgr.Register(JobDefinition{
		Key:          ServiceUpdaterKey,
		Name:         ServiceUpdaterName,
		Handler:      serviceUpdaterHandler(sm, stm, tm, channels),
		IsRerunnable: true,
	})
}

func serviceUpdaterHandler(sm *service.ServiceManager, stm *stream.StreamManager, tm *tuner.TunerManager, channels config.ChannelsConfig) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		var scanned, skipped, piggybacked int

		for _, channel := range channels {
			select {
			case <-ctx.Done():
				slog.Info("service updater aborted", "scanned", scanned, "skipped", skipped)
				return ctx.Err()
			default:
			}

			if channel.IsDisabled != nil && *channel.IsDisabled {
				continue
			}

			group := channel.Type
			if len(channel.TunerGroups) > 0 {
				group = channel.TunerGroups[0]
			}

			if stm.HasSession(channel.Type, channel.Channel) {
				if err := sm.ScanServices(ctx, stm, channel.Type, channel.Channel); err != nil {
					slog.Error("failed to scan services (piggyback)", "channel", channel.Channel, "err", err)
					continue
				}
				piggybacked++
				slog.Debug("scanned services (piggyback)", "group", group, "channel", channel.Channel)
				continue
			}

			if stm.ActiveSessionCountByGroup(group) >= tm.TunerCountByGroup(group) {
				skipped++
				slog.Info("skipping scan: tuner unavailable", "group", group, "channel", channel.Channel)
				continue
			}

			if err := sm.ScanServices(ctx, stm, channel.Type, channel.Channel); err != nil {
				slog.Error("failed to scan services", "channel", channel.Channel, "err", err)
				continue
			}
			scanned++
			slog.Debug("scanned services", "group", group, "channel", channel.Channel)
		}

		slog.Info("service updater completed",
			"scanned", scanned,
			"piggybacked", piggybacked,
			"skipped", skipped,
			"total", sm.CountServices(),
		)
		return nil
	}
}
