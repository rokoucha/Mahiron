package stream

import (
	"context"
	"io"
)

type ServiceScannerAdapter struct {
	manager *StreamManager
}

func NewServiceScannerAdapter(manager *StreamManager) *ServiceScannerAdapter {
	return &ServiceScannerAdapter{manager: manager}
}

func (a *ServiceScannerAdapter) ScanServices(ctx context.Context, channelType, channelID string, wait bool, dst io.Writer) error {
	var (
		session *ChannelSession
		err     error
	)
	if wait {
		session, err = a.manager.GetOrCreateWait(ctx, channelType, channelID)
	} else {
		session, err = a.manager.GetOrCreate(ctx, channelType, channelID)
	}
	if err != nil {
		return err
	}
	return session.ScanServices(ctx, dst)
}
