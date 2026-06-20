package stream

import (
	"context"
	"io"
)

type EPGCollectorAdapter struct {
	manager *StreamManager
}

func NewEPGCollectorAdapter(manager *StreamManager) *EPGCollectorAdapter {
	return &EPGCollectorAdapter{manager: manager}
}

func (a *EPGCollectorAdapter) HasSession(channelType, channel string) bool {
	return a.manager.HasSession(channelType, channel)
}

func (a *EPGCollectorAdapter) GetOrCreate(ctx context.Context, channelType, channel string) (interface {
	CollectEITS(context.Context, io.Writer) error
	CollectEITPF(context.Context, io.Writer) error
}, error) {
	return a.manager.GetOrCreate(ctx, channelType, channel)
}
