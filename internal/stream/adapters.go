package stream

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/ts"
)

type APIStreamAdapter struct {
	manager *StreamManager
}

func NewAPIStreamAdapter(manager *StreamManager) *APIStreamAdapter {
	return &APIStreamAdapter{manager: manager}
}

func (a *APIStreamAdapter) GetOrCreate(ctx context.Context, channelType, channel string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ObserveDataBroadcast(context.Context, uint16, bool, func(databroadcast.DataBroadcastEvent) error) error
	DataBroadcastModule(uint16, byte, uint16) (databroadcast.DataBroadcastModule, bool)
}, error) {
	return a.manager.GetOrCreate(ctx, channelType, channel)
}

func (a *APIStreamAdapter) GetExisting(channelType, channel string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ObserveDataBroadcast(context.Context, uint16, bool, func(databroadcast.DataBroadcastEvent) error) error
	DataBroadcastModule(uint16, byte, uint16) (databroadcast.DataBroadcastModule, bool)
}, bool) {
	return a.manager.GetExisting(channelType, channel)
}

func (a *APIStreamAdapter) ActiveSessionCount() int {
	return a.manager.ActiveSessionCount()
}

type EPGCollectorAdapter struct {
	manager *StreamManager
}

func NewEPGCollectorAdapter(manager *StreamManager) *EPGCollectorAdapter {
	return &EPGCollectorAdapter{manager: manager}
}

func (a *EPGCollectorAdapter) HasSession(channelType, channel string) bool {
	return a.manager.HasSession(channelType, channel)
}

func (a *EPGCollectorAdapter) GetOrCreateWait(ctx context.Context, channelType, channel string) (interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, error) {
	return a.manager.GetOrCreateWait(ctx, channelType, channel)
}

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

func (a *ServiceScannerAdapter) ScanServices(ctx context.Context, channelType, channelID string, wait bool) ([]ts.ServiceInfo, error) {
	return a.ScanServicesWithAcquireContext(ctx, ctx, channelType, channelID, wait)
}

func (a *ServiceScannerAdapter) ScanServicesWithAcquireContext(scanCtx, acquireCtx context.Context, channelType, channelID string, wait bool) ([]ts.ServiceInfo, error) {
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
