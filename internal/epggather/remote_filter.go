package epggather

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

// ServiceLister loads the known services used to filter remote program events.
type ServiceLister interface {
	GetServices(context.Context) ([]*service.Service, error)
}

type serviceKey struct {
	networkID uint16
	serviceID uint16
}

type RemoteEventWriter interface {
	EventWriter
	RemovePrograms(context.Context, []int64) error
}

// RemoteProgramFilter stores the program events a remote server pushes
// only for the services whose EPG that remote serves. A remote pushes the
// programs of all its services: they would add services this server never
// scanned, and a remote that no route of the channel points to would
// overwrite the programs read from the routed remote or from local EIT.
// The service list is reloaded after remoteProgramFilterTTL, so that a scan
// that adds a service or moves it to another channel is picked up without
// reloading for every event of a service this server never scanned.
type RemoteProgramFilter struct {
	inner    RemoteEventWriter
	loader   ServiceLister
	channels config.ChannelsConfig

	mu sync.Mutex
	// remotes maps the scanned services served by a remote to that remote.
	remotes  map[serviceKey]string
	loadedAt time.Time
}

const remoteProgramFilterTTL = time.Minute

func NewRemoteProgramFilter(inner RemoteEventWriter, loader ServiceLister, channels config.ChannelsConfig) *RemoteProgramFilter {
	return &RemoteProgramFilter{inner: inner, loader: loader, channels: channels}
}

// ForRemote returns the writer for the events of the named remote.
func (f *RemoteProgramFilter) ForRemote(remote string) RemoteEventWriter {
	return &remoteProgramWriter{filter: f, remote: remote}
}

type remoteProgramWriter struct {
	filter *RemoteProgramFilter
	remote string
}

func (w *remoteProgramWriter) UpsertEvents(ctx context.Context, programs []model.Event) error {
	if len(programs) == 0 {
		return nil
	}
	filtered, err := w.filter.accept(ctx, w.remote, programs)
	if err != nil || len(filtered) == 0 {
		return err
	}
	return w.filter.inner.UpsertEvents(ctx, filtered)
}

func (w *remoteProgramWriter) RemovePrograms(ctx context.Context, ids []int64) error {
	events := make([]model.Event, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		key := model.ServiceKey{NetworkID: uint16(id / 10000000000), ServiceID: uint16(id / 100000 % 100000)}
		eventID := uint16(id % 100000)
		if model.ProgramID(key, eventID) != id {
			continue
		}
		events = append(events, model.Event{Key: key, EventID: eventID})
	}
	if len(events) == 0 {
		return nil
	}
	filtered, err := w.filter.accept(ctx, w.remote, events)
	if err != nil || len(filtered) == 0 {
		return err
	}
	acceptedIDs := make([]int64, 0, len(filtered))
	for _, e := range filtered {
		acceptedIDs = append(acceptedIDs, model.ProgramID(e.Key, e.EventID))
	}
	return w.filter.inner.RemovePrograms(ctx, acceptedIDs)
}

func (f *RemoteProgramFilter) accept(ctx context.Context, remote string, programs []model.Event) ([]model.Event, error) {
	remotes, err := f.load(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]model.Event, 0, len(programs))
	for _, item := range programs {
		if remote != "" && remotes[serviceKey{networkID: item.Key.NetworkID, serviceID: item.Key.ServiceID}] == remote {
			filtered = append(filtered, item)
			continue
		}
		slog.Debug("ignoring remote program event for service not served by the remote", "remote", remote, "networkId", item.Key.NetworkID, "serviceId", item.Key.ServiceID, "eventId", item.EventID)
	}
	return filtered, nil
}

func (f *RemoteProgramFilter) load(ctx context.Context) (map[serviceKey]string, error) {
	f.mu.Lock()
	remotes := f.remotes
	fresh := !f.loadedAt.IsZero() && time.Since(f.loadedAt) < remoteProgramFilterTTL
	f.mu.Unlock()
	if fresh {
		return remotes, nil
	}
	return f.refresh(ctx)
}

func (f *RemoteProgramFilter) refresh(ctx context.Context) (map[serviceKey]string, error) {
	services, err := f.loader.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	channelRemotes := make(map[Candidate]string, len(f.channels))
	for _, ch := range f.channels {
		c := Candidate{Type: ch.Type, Channel: ch.Channel}
		if _, ok := channelRemotes[c]; ok || config.IsChannelDisabled(ch) {
			// The stream pool also uses the first enabled match.
			continue
		}
		channelRemotes[c] = ch.StoredEPGRemote()
	}
	// A service belongs to the channel it was last scanned on.
	remotes := make(map[serviceKey]string, len(services))
	for _, svc := range services {
		if svc == nil {
			continue
		}
		if remote := channelRemotes[Candidate{Type: svc.ChannelType, Channel: svc.ChannelId}]; remote != "" {
			remotes[serviceKey{networkID: svc.Key.NetworkID, serviceID: svc.Key.ServiceID}] = remote
		}
	}
	f.mu.Lock()
	f.remotes = remotes
	f.loadedAt = time.Now()
	f.mu.Unlock()
	return remotes, nil
}
