package stream

import (
	"bufio"
	"context"
	"io"
	"log/slog"

	"github.com/21S1298001/Mahiron5/internal/util"
)

type EITPFPiggyback struct {
	channel     string
	channelType string
	collector   EITCollector
	updater     EITSectionUpdater
}

func NewEITPFPiggyback(channelType, channel string, collector EITCollector, updater EITSectionUpdater) *EITPFPiggyback {
	if collector == nil || updater == nil {
		return nil
	}
	return &EITPFPiggyback{
		channel:     channel,
		channelType: channelType,
		collector:   collector,
		updater:     updater,
	}
}

func (p *EITPFPiggyback) Hook(ctx context.Context, broadcast *Broadcast) {
	r, w := io.Pipe()
	go func() {
		slog.Debug("starting EITPF piggyback collection", "type", p.channelType, "channel", p.channel)
		defer r.Close()
		defer w.Close()
		defer slog.Debug("finished EITPF piggyback collection", "type", p.channelType, "channel", p.channel)

		done := make(chan error, 1)
		go func() {
			done <- broadcast.Tap(ctx, w)
		}()

		pr, pw := io.Pipe()
		collectDone := make(chan error, 1)
		go func() {
			collectDone <- p.collector.CollectEITPF(ctx, r, pw)
			_ = pw.Close()
		}()

		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			if err := p.updater.UpsertEITSectionJSON(ctx, scanner.Bytes()); err != nil {
				slog.Error("failed to update EITPF", "type", p.channelType, "channel", p.channel, "err", err)
			}
		}
		_ = pr.Close()
		if err := <-collectDone; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed to collect EITPF", "type", p.channelType, "channel", p.channel, "err", err)
		}
		if err := <-done; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed EITPF piggyback source", "type", p.channelType, "channel", p.channel, "err", err)
		}
	}()
}
