package stream

import (
	"context"
	"io"
	"log/slog"

	"github.com/21S1298001/Mahiron5/internal/util"
	"github.com/21S1298001/Mahiron5/ts"
)

type LogoPiggyback struct {
	channel     string
	channelType string
	collector   LogoCollector
	updater     LogoUpdater
}

func NewLogoPiggyback(channelType, channel string, collector LogoCollector, updater LogoUpdater) *LogoPiggyback {
	if collector == nil || updater == nil {
		return nil
	}
	return &LogoPiggyback{
		channel:     channel,
		channelType: channelType,
		collector:   collector,
		updater:     updater,
	}
}

func (p *LogoPiggyback) Hook(ctx context.Context, broadcast *Broadcast) {
	r, w := io.Pipe()
	go func() {
		slog.Debug("starting logo piggyback collection", "type", p.channelType, "channel", p.channel)
		defer r.Close()
		defer w.Close()
		defer slog.Debug("finished logo piggyback collection", "type", p.channelType, "channel", p.channel)

		done := make(chan error, 1)
		go func() {
			done <- broadcast.Tap(ctx, w)
		}()

		collectDone := make(chan error, 1)
		go func() {
			collectDone <- p.collector.Collect(ctx, r, func(image *ts.LogoImage) error {
				if err := p.updater.UpsertLogoImage(ctx, image); err != nil {
					slog.Error("failed to update logo", "type", p.channelType, "channel", p.channel, "networkId", image.OriginalNetworkID, "logoId", image.LogoID, "err", err)
				}
				return nil
			})
		}()
		if err := <-collectDone; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed to collect logos", "type", p.channelType, "channel", p.channel, "err", err)
		}
		if err := <-done; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed logo piggyback source", "type", p.channelType, "channel", p.channel, "err", err)
		}
	}()
}
