package job

import (
	"context"
	"log/slog"
)

const (
	EPGGathererKey  = "epg-gatherer"
	EPGGathererName = "EPG Gatherer"

	EPGGathererDefaultSchedule = "20,50 * * * *"
)

func RegisterEPGGatherer(mgr *JobManager) {
	mgr.Register(JobDefinition{
		Key:          EPGGathererKey,
		Name:         EPGGathererName,
		Handler:      epgGathererHandler,
		IsRerunnable: true,
	})
}

func epgGathererHandler(ctx context.Context) error {
	slog.Warn("epg gatherer not implemented yet")
	return nil
}
