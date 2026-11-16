package stream

import (
	"context"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
)

type LogoGatherAdapter struct {
	manager *Manager
}

func NewLogoGatherAdapter(manager *Manager) *LogoGatherAdapter {
	return &LogoGatherAdapter{manager: manager}
}

func (a *LogoGatherAdapter) ObserveLogos(ctx context.Context, channelType, channelID string, observe func(model.Logo) error) error {
	session, err := a.manager.GetOrCreateWait(ctx, channelType, channelID)
	if err != nil {
		return err
	}
	return session.ObserveLogos(ctx, observe)
}

// CommonDataNetwork reports whether the network's services take their logos
// from the ISDB-S all-receivers common data (BS and CS do; the rest don't).
func (a *LogoGatherAdapter) CommonDataNetwork(networkID uint16) bool {
	return isdb.IsSatelliteOriginalNetworkID(networkID)
}

// DefaultCommonDataService is the service carrying the all-receivers common
// data until an SDTT announcement names another.
func (a *LogoGatherAdapter) DefaultCommonDataService() model.ServiceKey {
	return channel.DefaultCommonDataService()
}

type ServiceScanAdapter struct {
	manager *Manager
}

func NewServiceScanAdapter(manager *Manager) *ServiceScanAdapter {
	return &ServiceScanAdapter{manager: manager}
}

func (a *ServiceScanAdapter) ScanServices(scanCtx, acquireCtx context.Context, channelType, channelID string, wait bool) ([]model.Service, error) {
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

// EPGGatherAdapter exposes schedule sources to EPG gathering. A channel
// served by a remote yields stored programs instead of live EIT updates.
type EPGGatherAdapter struct {
	// Legacy scheduled gathers may wait; continuous acquisition must not.
	WaitForTuner bool
	manager      *Manager
}

func NewEPGGatherAdapter(manager *Manager) *EPGGatherAdapter {
	return &EPGGatherAdapter{manager: manager, WaitForTuner: true}
}

func (a *EPGGatherAdapter) HasSession(channelType, channelID string) bool {
	return a.manager.HasSession(channelType, channelID)
}

// StoredEPG follows session construction: TLV always collects live signaling,
// while TS uses stored programs only when all enabled routes are remote.
func (a *EPGGatherAdapter) StoredEPG(channelType, channelID string) bool {
	return a.manager.sources.StoredEPG(channelType, channelID)
}

func (a *EPGGatherAdapter) OpenSchedule(ctx context.Context, channelType, channelID string) (schedule.Source, error) {
	if a.StoredEPG(channelType, channelID) {
		return schedule.StoredSource(func(ctx context.Context, nid, sid uint16) ([]model.Event, error) {
			return a.manager.sources.ListStoredPrograms(ctx, channelType, channelID, nid, sid)
		}), nil
	}
	session, err := a.manager.getOrCreate(ctx, channelType, channelID, a.WaitForTuner)
	if err != nil {
		return schedule.Source{}, err
	}
	source := session.ScheduleSource()
	if source.IsStored() {
		return source, nil
	}
	return schedule.LiveSource(func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
		return source.Collect(ensureUserContext(ctx, channelType, channelID), onSchedule, onPresentFollowing)
	}), nil
}

func (a *EPGGatherAdapter) CheckAvailable(ctx context.Context, channelType, channelID string) error {
	return a.manager.CheckAvailable(ensureUserContext(ctx, channelType, channelID), channelType, channelID)
}
