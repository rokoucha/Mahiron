package epggather

import (
	"fmt"
	"github.com/21S1298001/mahiron/ts"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/google/go-cmp/cmp"
)

func TestGroupsUseStreamIdentityRegardlessOfChannelType(t *testing.T) {
	for _, nid := range []uint16{4, 6, 7, 11, 12, 32736, 65534} {
		t.Run(fmt.Sprint(nid), func(t *testing.T) {
			services := []*service.Service{
				{Service: model.Service{Key: model.ServiceKey{NetworkID: nid, StreamID: 100, ServiceID: 1}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A"},
				{Service: model.Service{Key: model.ServiceKey{NetworkID: nid, StreamID: 101, ServiceID: 2}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "B"},
				{Service: model.Service{Key: model.ServiceKey{NetworkID: nid + 1, StreamID: 100, ServiceID: 3}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "C"},
			}
			channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "A"}, {Type: "CUSTOM", Channel: "B"}, {Type: "CUSTOM", Channel: "C"}, {Type: "CUSTOM", Channel: "unscanned"}}
			groups := groupServices(services, channels, func(string, string) bool { return false })
			if len(groups) != 3 {
				t.Fatalf("groups = %v, want 3", groups)
			}
			for _, svc := range services {
				group := groups[GroupKey{NetworkID: svc.Key.NetworkID, StreamID: svc.Key.StreamID}]
				want := &Group{Services: []model.ServiceKey{svc.Key}, Candidates: []Candidate{{Type: svc.ChannelType, Channel: svc.ChannelId}}}
				if diff := cmp.Diff(want, group); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestGroupsCombineAlternateChannelsAndTrackFreshnessPerStream(t *testing.T) {
	fresh := int64(200)
	old := int64(100)
	key := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 1}
	services := []*service.Service{
		{Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A"},
		{Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "OTHER", ChannelId: "alternate"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 2}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 3}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "B"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 4}}, ChannelType: "CUSTOM", ChannelId: "B"},
	}
	services[0].EPG.LastSuccessAt = &fresh
	services[1].EPG.LastSuccessAt = &fresh
	services[2].EPG.LastSuccessAt = &old
	services[3].EPG.LastSuccessAt = &fresh
	channels := config.ChannelsConfig{{Type: "OTHER", Channel: "alternate"}, {Type: "CUSTOM", Channel: "A"}, {Type: "CUSTOM", Channel: "B"}}
	groups := groupServices(services, channels, func(string, string) bool { return false })
	first := groups[GroupKey{NetworkID: 4, StreamID: 100}]
	if first.OldestSuccessAt != old || len(first.Services) != 2 {
		t.Fatalf("first = %#v", first)
	}
	if diff := cmp.Diff([]Candidate{{Type: "OTHER", Channel: "alternate"}, {Type: "CUSTOM", Channel: "A"}}, first.Candidates); diff != "" {
		t.Fatal(diff)
	}
	second := groups[GroupKey{NetworkID: 4, StreamID: 101}]
	if second.OldestSuccessAt != fresh || len(second.Services) != 1 {
		t.Fatalf("second = %#v", second)
	}
	services[2].EPG.LastSuccessAt = nil
	if got := groupServices(services, channels, func(string, string) bool { return false })[GroupKey{NetworkID: 4, StreamID: 100}].OldestSuccessAt; got != 0 {
		t.Fatalf("unseen freshness = %d", got)
	}
}

func TestGroupsKeepStoredEPGNetworkWideAndSeparateFromLive(t *testing.T) {
	services := []*service.Service{
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 1}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "remoteA"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 2}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "remoteB"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 102, ServiceID: 3}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "live"},
	}
	channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "remoteA"}, {Type: "CUSTOM", Channel: "remoteB"}, {Type: "CUSTOM", Channel: "live"}}
	groups := groupServices(services, channels, func(_, ch string) bool { return ch != "live" })
	if len(groups) != 2 {
		t.Fatalf("groups = %v", groups)
	}
	remote := groups[GroupKey{NetworkID: 4, Stored: true}]
	if remote == nil || len(remote.Services) != 2 || len(remote.Candidates) != 2 {
		t.Fatalf("remote = %#v", remote)
	}
	live := groups[GroupKey{NetworkID: 4, StreamID: 102}]
	if live == nil || len(live.Services) != 1 || len(live.Candidates) != 1 {
		t.Fatalf("live = %#v", live)
	}
}

func TestGroupsIgnoreDisabledChannelsAndPreferStoredCopy(t *testing.T) {
	disabled := true
	key := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 1}
	services := []*service.Service{
		{Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "local"},
		{Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "remote"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 2}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "disabled"},
	}
	channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "local"}, {Type: "CUSTOM", Channel: "remote"}, {Type: "CUSTOM", Channel: "disabled", IsDisabled: &disabled}}
	groups := groupServices(services, channels, func(_, ch string) bool { return ch == "remote" })
	if len(groups) != 1 {
		t.Fatalf("groups = %v", groups)
	}
	want := &Group{Services: []model.ServiceKey{key}, Candidates: []Candidate{{Type: "CUSTOM", Channel: "remote"}}}
	if diff := cmp.Diff(want, groups[GroupKey{NetworkID: 4, Stored: true}]); diff != "" {
		t.Fatal(diff)
	}
}

func TestGatherGroupReloadsTopology(t *testing.T) {
	key := GroupKey{NetworkID: 4, StreamID: 100}
	replacement := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 102}
	session := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, replacement, 10), testEIT(0x58, replacement, 10)}}
	store := channelServiceStore{remoteSyncServiceStore: newRemoteSyncServiceStore(), services: []*service.Service{
		{Service: model.Service{Key: replacement, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "new"},
	}}
	events := &collectProgramStore{}
	gatherer := NewGatherer(events, events, &store, staticEPGStreams{session: session}, config.ChannelsConfig{{Type: "CUSTOM", Channel: "new"}}, 20*time.Millisecond)
	if err := gatherer.GatherGroup(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if session.collectCalls != 1 {
		t.Fatalf("collection calls = %d", session.collectCalls)
	}
	store.services = nil
	if err := gatherer.GatherGroup(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if session.collectCalls != 1 {
		t.Fatal("collected removed stream")
	}
}

func TestGroupsRequireScannedServices(t *testing.T) {
	events := &collectProgramStore{}
	store := &staticEPGServiceStore{}
	gatherer := NewGatherer(events, events, store, staticEPGStreams{}, nil, time.Second)
	if _, err := gatherer.Groups(t.Context()); err == nil {
		t.Fatal("unscanned topology must remain retryable")
	}
	if err := gatherer.GatherGroup(t.Context(), GroupKey{NetworkID: 4, StreamID: 100}); err != nil {
		t.Fatalf("removed group should be skipped: %v", err)
	}
}

func TestUnknownLiveStreamsRemainSeparateUntilScanned(t *testing.T) {
	services := []*service.Service{
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 151}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A"},
		{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 161}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "B"},
	}
	channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "A"}, {Type: "CUSTOM", Channel: "B"}}
	groups := groupServices(services, channels, func(string, string) bool { return false })
	if len(groups) != 2 {
		t.Fatalf("unknown live streams combined: %v", groups)
	}
	for _, svc := range services {
		c := Candidate{Type: svc.ChannelType, Channel: svc.ChannelId}
		group := groups[GroupKey{NetworkID: 4, Channel: c}]
		if group == nil || !equalServiceKeys(group.Services, []model.ServiceKey{svc.Key}) {
			t.Fatalf("missing provisional group for %v", c)
		}
	}
	groups = groupServices(services, channels, func(string, string) bool { return true })
	if len(groups) != 1 || len(groups[GroupKey{NetworkID: 4, Stored: true}].Services) != 2 {
		t.Fatal("stored EPG must stay network-wide without stream IDs")
	}
}

func TestQualityWarningsDoNotBecomeGroupFailures(t *testing.T) {
	timestamp := func(value int64) *int64 { return &value }
	a := &service.Service{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 101}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A", EPG: service.EPGStatus{LastSuccessAt: timestamp(100), LastAttemptAt: timestamp(100), LastError: "low quality EITS"}}
	b := &service.Service{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 102}, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A", EPG: service.EPGStatus{LastSuccessAt: timestamp(300), LastAttemptAt: timestamp(300)}}
	groups := groupServices([]*service.Service{a, b}, config.ChannelsConfig{{Type: "CUSTOM", Channel: "A"}}, func(string, string) bool { return false })
	key := GroupKey{NetworkID: 4, StreamID: 100}
	if groups[key].LastFailureAt != 0 {
		t.Fatal("quality-only warning triggered retry")
	}
	a.EPG.LastAttemptAt = timestamp(200)
	a.EPG.LastError = "EPG reception incomplete"
	groups = groupServices([]*service.Service{a, b}, config.ChannelsConfig{{Type: "CUSTOM", Channel: "A"}}, func(string, string) bool { return false })
	if groups[key].LastFailureAt != 200 {
		t.Fatalf("failure timestamp smeared across services: %+v", groups[key])
	}
}
