package stream

import (
	"context"

	"github.com/21S1298001/mahiron/ts"
)

type LogoCollectorAdapter struct {
	manager *StreamManager
}

func NewLogoCollectorAdapter(manager *StreamManager) *LogoCollectorAdapter {
	return &LogoCollectorAdapter{manager: manager}
}

func (a *LogoCollectorAdapter) ObserveLogos(ctx context.Context, channelType, channelID string, observe func(*ts.LogoImage) error) error {
	session, err := a.manager.GetOrCreateWait(ctx, channelType, channelID)
	if err != nil {
		return err
	}
	return session.ObserveLogos(ctx, observe)
}

type ServiceScannerAdapter struct {
	manager *StreamManager
}

func NewServiceScannerAdapter(manager *StreamManager) *ServiceScannerAdapter {
	return &ServiceScannerAdapter{manager: manager}
}

func (a *ServiceScannerAdapter) ScanServices(scanCtx, acquireCtx context.Context, channelType, channelID string, wait bool) ([]ts.ServiceInfo, error) {
	var (
		session Session
		err     error
	)
	if wait {
		session, err = a.manager.GetOrCreateWait(acquireCtx, channelType, channelID)
	} else {
		session, err = a.manager.GetOrCreate(acquireCtx, channelType, channelID)
	}
	if err != nil {
		return nil, err
	}
	return session.ScanServices(scanCtx)
}
