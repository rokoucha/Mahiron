package stream

import (
	"context"
	"log/slog"
	"sync"

	"github.com/21S1298001/mahiron/internal/program"
)

type knownServiceProgramUpdater struct {
	inner  ProgramUpdater
	loader ServiceLister

	mu     sync.Mutex
	known  map[serviceKey]struct{}
	loaded bool
}

type serviceKey struct {
	networkID uint16
	serviceID uint16
}

func newKnownServiceProgramUpdater(inner ProgramUpdater, loader ServiceLister) ProgramUpdater {
	return &knownServiceProgramUpdater{inner: inner, loader: loader}
}

func (u *knownServiceProgramUpdater) UpsertPrograms(ctx context.Context, programs []*program.Program) error {
	if len(programs) == 0 {
		return nil
	}
	if err := u.ensureLoaded(ctx); err != nil {
		return err
	}
	filtered, unknown := u.filter(programs)
	if unknown {
		if err := u.refresh(ctx); err != nil {
			return err
		}
		filtered, _ = u.filter(programs)
	}
	if len(filtered) == 0 {
		return nil
	}
	return u.inner.UpsertPrograms(ctx, filtered)
}

func (u *knownServiceProgramUpdater) ensureLoaded(ctx context.Context) error {
	u.mu.Lock()
	loaded := u.loaded
	u.mu.Unlock()
	if loaded {
		return nil
	}
	return u.refresh(ctx)
}

func (u *knownServiceProgramUpdater) refresh(ctx context.Context) error {
	services, err := u.loader.GetServices(ctx)
	if err != nil {
		return err
	}
	known := make(map[serviceKey]struct{}, len(services))
	for _, svc := range services {
		if svc == nil {
			continue
		}
		known[serviceKey{networkID: svc.NetworkId, serviceID: svc.ServiceId}] = struct{}{}
	}
	u.mu.Lock()
	u.known = known
	u.loaded = true
	u.mu.Unlock()
	return nil
}

func (u *knownServiceProgramUpdater) filter(programs []*program.Program) ([]*program.Program, bool) {
	u.mu.Lock()
	known := u.known
	u.mu.Unlock()

	filtered := make([]*program.Program, 0, len(programs))
	unknown := false
	for _, item := range programs {
		if item == nil {
			continue
		}
		key := serviceKey{networkID: item.NetworkID, serviceID: item.ServiceID}
		if _, ok := known[key]; ok {
			filtered = append(filtered, item)
			continue
		}
		unknown = true
		slog.Debug("ignoring remote program event for unknown service", "networkId", item.NetworkID, "serviceId", item.ServiceID, "programId", item.ID)
	}
	return filtered, unknown
}
