package epggather

import (
	"context"
	"log/slog"
	"sync"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

// ServiceLister loads the known services used to filter remote program events.
type ServiceLister interface {
	GetServices(context.Context) ([]*service.Service, error)
}

type knownServiceProgramUpdater struct {
	inner  EventWriter
	loader ServiceLister

	mu     sync.Mutex
	known  map[serviceKey]struct{}
	loaded bool
}

type serviceKey struct {
	networkID uint16
	serviceID uint16
}

// NewKnownServiceProgramUpdater stores only the programs of scanned
// services. A remote server pushes the programs of all its services, which
// would otherwise add services this server never scanned. An unknown
// service reloads the service list once, so that a fresh scan is picked up.
func NewKnownServiceProgramUpdater(inner EventWriter, loader ServiceLister) EventWriter {
	return &knownServiceProgramUpdater{inner: inner, loader: loader}
}

func (u *knownServiceProgramUpdater) UpsertEvents(ctx context.Context, programs []model.Event) error {
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
	return u.inner.UpsertEvents(ctx, filtered)
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
		known[serviceKey{networkID: svc.Key.NetworkID, serviceID: svc.Key.ServiceID}] = struct{}{}
	}
	u.mu.Lock()
	u.known = known
	u.loaded = true
	u.mu.Unlock()
	return nil
}

func (u *knownServiceProgramUpdater) filter(programs []model.Event) ([]model.Event, bool) {
	u.mu.Lock()
	known := u.known
	u.mu.Unlock()

	filtered := make([]model.Event, 0, len(programs))
	unknown := false
	for _, item := range programs {
		key := serviceKey{networkID: item.Key.NetworkID, serviceID: item.Key.ServiceID}
		if _, ok := known[key]; ok {
			filtered = append(filtered, item)
			continue
		}
		unknown = true
		slog.Debug("ignoring remote program event for unknown service", "networkId", item.Key.NetworkID, "serviceId", item.Key.ServiceID, "eventId", item.EventID)
	}
	return filtered, unknown
}
