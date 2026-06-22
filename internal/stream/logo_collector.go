package stream

import (
	"context"

	"github.com/21S1298001/Mahiron5/ts"
)

type LogoCollectorAdapter struct {
	manager *StreamManager
}

func NewLogoCollectorAdapter(manager *StreamManager) *LogoCollectorAdapter {
	return &LogoCollectorAdapter{manager: manager}
}

func (a *LogoCollectorAdapter) CollectLogos(ctx context.Context, channelType, channelID string, observe func(*ts.LogoImage) error) error {
	session, err := a.manager.GetOrCreateWait(ctx, channelType, channelID)
	if err != nil {
		return err
	}
	return session.CollectLogos(ctx, observe)
}
