package stream

import (
	"context"
	"io"
)

type APIStreamAdapter struct {
	manager *StreamManager
}

func NewAPIStreamAdapter(manager *StreamManager) *APIStreamAdapter {
	return &APIStreamAdapter{manager: manager}
}

func (a *APIStreamAdapter) GetOrCreate(ctx context.Context, channelType, channel string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
}, error) {
	return a.manager.GetOrCreate(ctx, channelType, channel)
}
