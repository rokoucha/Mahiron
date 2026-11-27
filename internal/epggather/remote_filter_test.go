package epggather

import (
	"context"
	"slices"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

func remoteService(nid, sid uint16, channelType, channel string) *service.Service {
	return &service.Service{
		Service:     model.Service{Key: model.ServiceKey{NetworkID: nid, ServiceID: sid}},
		ChannelType: channelType,
		ChannelId:   channel,
	}
}

func remoteChannel(channelType, channel string, routes ...config.ChannelRouteConfig) config.ChannelConfig {
	return config.ChannelConfig{Type: channelType, Channel: channel, Routes: routes}
}

func remoteRoute(remote string, priority int) config.ChannelRouteConfig {
	return config.ChannelRouteConfig{Remote: remote, Type: "GR", Channel: "27", Priority: &priority}
}

var remoteFilterChannels = config.ChannelsConfig{remoteChannel("GR", "27", remoteRoute("living", 0))}

// A remote pushes the programs of services this server never scanned; they
// are dropped without reloading the service list for every batch.
func TestRemoteProgramFilterDropsUnknownServicesWithoutReload(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{services: []*service.Service{remoteService(4, 101, "GR", "27")}}
	updater := NewRemoteProgramFilter(inner, lister, remoteFilterChannels).ForRemote("living")

	for range 2 {
		err := updater.UpsertEvents(t.Context(), []model.Event{
			{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}, EventID: 1},
			{Key: model.ServiceKey{NetworkID: 4, ServiceID: 102}, EventID: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got, want := len(inner.programs), 2; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	for _, p := range inner.programs {
		if p.Key.ServiceID != 101 {
			t.Fatalf("program = %#v, want known service", p)
		}
	}
	if got, want := lister.calls, 1; got != want {
		t.Fatalf("service list calls = %d, want %d", got, want)
	}
}

// A remote that no route points to must not overwrite the programs of a
// channel served by another remote, nor make a known service reload.
func TestRemoteProgramFilterAcceptsOnlyStoredEPGRemote(t *testing.T) {
	no := false
	yes := true
	disabled := remoteRoute("disabled", -1)
	disabled.IsDisabled = &yes
	tlv := remoteChannel("BS", "BS15", config.ChannelRouteConfig{Remote: "living", Type: "BS", Channel: "BS15"})
	tlv.Transport = config.TransportTLV
	channels := config.ChannelsConfig{
		remoteChannel("GR", "27", remoteRoute("living", 0), disabled),
		remoteChannel("GR", "26", remoteRoute("backup", 1), remoteRoute("primary", 0)),
		remoteChannel("GR", "25", config.ChannelRouteConfig{Type: "GR", Channel: "25", IsDisabled: &no}, remoteRoute("living", 1)),
		tlv,
	}
	services := []*service.Service{
		remoteService(1, 1, "GR", "27"),
		remoteService(1, 2, "GR", "26"),
		remoteService(1, 3, "GR", "25"),
		remoteService(1, 4, "BS", "BS15"),
	}
	cases := []struct {
		remote string
		want   []uint16
	}{
		{remote: "living", want: []uint16{1}},
		{remote: "primary", want: []uint16{2}},
		{remote: "backup"},
		{remote: "disabled"},
		{remote: "other"},
	}
	for _, tc := range cases {
		t.Run(tc.remote, func(t *testing.T) {
			inner := &recordingProgramUpdater{}
			lister := &recordingServiceLister{services: services}
			updater := NewRemoteProgramFilter(inner, lister, channels).ForRemote(tc.remote)
			var events []model.Event
			var ids []int64
			for _, svc := range services {
				events = append(events, model.Event{Key: svc.Key, EventID: 1})
				ids = append(ids, model.ProgramID(svc.Key, 1))
			}
			if err := updater.UpsertEvents(t.Context(), events); err != nil {
				t.Fatal(err)
			}
			if err := updater.RemovePrograms(t.Context(), ids); err != nil {
				t.Fatal(err)
			}
			var upserted []uint16
			for _, p := range inner.programs {
				upserted = append(upserted, p.Key.ServiceID)
			}
			var removed []uint16
			for _, id := range inner.removed {
				removed = append(removed, uint16(id/100000%100000))
			}
			if !slices.Equal(upserted, tc.want) || !slices.Equal(removed, tc.want) {
				t.Fatalf("upserted = %v, removed = %v, want %v", upserted, removed, tc.want)
			}
			if lister.calls != 1 {
				t.Fatalf("service list calls = %d, want 1 (known services must not reload)", lister.calls)
			}
		})
	}
}

// A rescan may add a service or move it to a channel served by another remote.
func TestRemoteProgramFilterReloadsAfterTTL(t *testing.T) {
	channels := config.ChannelsConfig{
		remoteChannel("GR", "27", remoteRoute("living", 0)),
		remoteChannel("GR", "26", remoteRoute("bedroom", 0)),
	}
	lister := &recordingServiceLister{
		services:        []*service.Service{remoteService(1, 1, "GR", "27")},
		refreshServices: []*service.Service{remoteService(1, 1, "GR", "26")},
	}
	inner := &recordingProgramUpdater{}
	filter := NewRemoteProgramFilter(inner, lister, channels)
	event := []model.Event{{Key: model.ServiceKey{NetworkID: 1, ServiceID: 1}, EventID: 1}}
	if err := filter.ForRemote("bedroom").UpsertEvents(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if len(inner.programs) != 0 {
		t.Fatalf("upserted = %d before the reload, want 0", len(inner.programs))
	}
	filter.loadedAt = filter.loadedAt.Add(-remoteProgramFilterTTL)
	if err := filter.ForRemote("bedroom").UpsertEvents(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if len(inner.programs) != 1 || lister.calls != 2 {
		t.Fatalf("upserted = %d, service list calls = %d, want 1 and 2", len(inner.programs), lister.calls)
	}
}

type recordingProgramUpdater struct {
	removed  []int64
	programs []model.Event
}

func (u *recordingProgramUpdater) UpsertEvents(_ context.Context, programs []model.Event) error {
	u.programs = append(u.programs, programs...)
	return nil
}

type recordingServiceLister struct {
	calls           int
	services        []*service.Service
	refreshServices []*service.Service
}

func (l *recordingServiceLister) GetServices(context.Context) ([]*service.Service, error) {
	l.calls++
	if l.calls > 1 && l.refreshServices != nil {
		return l.refreshServices, nil
	}
	return l.services, nil
}

func (u *recordingProgramUpdater) RemovePrograms(_ context.Context, ids []int64) error {
	u.removed = append(u.removed, ids...)
	return nil
}

func TestRemoteRemovalsFilterUnknownServices(t *testing.T) {
	inner := &recordingProgramUpdater{}
	loader := &recordingServiceLister{services: []*service.Service{remoteService(1, 2, "GR", "27")}}
	updater := NewRemoteProgramFilter(inner, loader, remoteFilterChannels).ForRemote("living")
	known := model.ProgramID(model.ServiceKey{NetworkID: 1, ServiceID: 2}, 1)
	unknown := model.ProgramID(model.ServiceKey{NetworkID: 2, ServiceID: 2}, 1)
	if err := updater.RemovePrograms(t.Context(), []int64{known, unknown, -1, 999999999999999999}); err != nil {
		t.Fatal(err)
	}
	if len(inner.removed) != 1 || inner.removed[0] != known {
		t.Fatalf("removed=%v", inner.removed)
	}
}
