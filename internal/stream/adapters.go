package stream

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

type LogoCollectorAdapter struct {
	manager *Manager
}

func NewLogoCollectorAdapter(manager *Manager) *LogoCollectorAdapter {
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
	manager *Manager
}

func NewServiceScannerAdapter(manager *Manager) *ServiceScannerAdapter {
	return &ServiceScannerAdapter{manager: manager}
}

func (a *ServiceScannerAdapter) ScanServices(scanCtx, acquireCtx context.Context, channelType, channelID string, wait bool) ([]model.Service, error) {
	if services, handled, err := a.manager.scanRemoteServices(scanCtx, channelType, channelID); handled {
		return services, err
	}
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

// EPGGatherAdapter gives EPG gathering its channel sessions in model and
// standard types only. A channel served by a remote yields the remote's
// stored-program lister instead of EIT collection.
type EPGGatherAdapter struct {
	manager *Manager
}

func NewEPGGatherAdapter(manager *Manager) *EPGGatherAdapter {
	return &EPGGatherAdapter{manager: manager}
}

func (a *EPGGatherAdapter) HasSession(channelType, channelID string) bool {
	return a.manager.HasSession(channelType, channelID)
}

func (a *EPGGatherAdapter) OpenSchedule(ctx context.Context, channelType, channelID string) (func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error, func(context.Context, uint16, uint16) ([]*program.Program, error), error) {
	session, err := a.manager.GetOrCreateWait(ctx, channelType, channelID)
	if err != nil {
		return nil, nil, err
	}
	if remote, ok := session.(interface {
		ListServicePrograms(context.Context, uint16, uint16) ([]*program.Program, error)
	}); ok {
		return nil, remote.ListServicePrograms, nil
	}
	return session.CollectSchedule, nil, nil
}
