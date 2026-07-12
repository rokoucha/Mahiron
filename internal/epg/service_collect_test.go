package epg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	servicepkg "github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/ts"
)

func TestCollectServiceSnapshotsRoutesEITSAndEITPF(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, ServiceKey{NetworkID: 4, ServiceID: 102}, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
		testEIT(ts.TableIDEITSStart, ServiceKey{NetworkID: 4, ServiceID: 102}, 20),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if session.collectCalls != 1 {
		t.Fatalf("CollectEIT calls = %d, want 1", session.collectCalls)
	}
	if got, want := store.eventIDs(), []uint16{1, 10, 20}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
	if got, want := store.sources, []string{"eitpf", "eits", "eits"}; !equalStrings(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsContinuesEITSAfterEITPFFailure(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	pfErr := errors.New("p/f upsert failed")
	store := &collectProgramStore{failEventID: 1, failErr: pfErr}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, key, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{1, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsWaitsForBasicBeforeUpsertingExtended(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart+8, key, 10),
		testEIT(ts.TableIDEITSStart, key, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsFlushesPartialEITSDuringCollection(t *testing.T) {
	previous := partialEITSFlushInterval
	partialEITSFlushInterval = 5 * time.Millisecond
	t.Cleanup(func() { partialEITSFlushInterval = previous })

	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	missing := ServiceKey{NetworkID: 4, ServiceID: 102}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, key, 10)}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key, missing}, 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{10, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want partial and final %v", got, want)
	}
}

func TestCollectServiceSnapshotsKeepsSameNetworkServicesOutsideExpected(t *testing.T) {
	expected := ServiceKey{NetworkID: 4, ServiceID: 151, TransportStreamID: 100}
	extra := ServiceKey{NetworkID: 4, ServiceID: 161, TransportStreamID: 101}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, expected, 10),
		testEIT(ts.TableIDEITSStart, extra, 20),
	}}

	result, err := CollectServiceSnapshots(context.Background(), store, status, session, []ServiceKey{expected}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if want := []ServiceKey{expected, extra}; !equalServiceKeys(result.Observed, want) {
		t.Fatalf("observed services = %v, want %v", result.Observed, want)
	}
	if got := store.eventIDs(); !containsEventIDs(got, []uint16{10, 20}) {
		t.Fatalf("upserted event IDs = %v, want expected and same-network extra events", got)
	}
	for _, key := range []ServiceKey{expected, extra} {
		statusKey := ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}
		if status.successes[statusKey] == 0 {
			t.Fatalf("service %d did not record success", key.ServiceID)
		}
	}
}

func TestCollectServiceSnapshotsStoresWarningForObservedServiceOutsideExpected(t *testing.T) {
	expected := ServiceKey{NetworkID: 4, ServiceID: 151, TransportStreamID: 100}
	extra := ServiceKey{NetworkID: 4, ServiceID: 161, TransportStreamID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, expected, 10),
		testSparseEIT(ts.TableIDEITSStart, extra, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session, []ServiceKey{expected}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	statusKey := ServiceKey{NetworkID: extra.NetworkID, ServiceID: extra.ServiceID}
	if status.successes[statusKey] == 0 {
		t.Fatal("extra service did not record success")
	}
	if got := status.errors[statusKey]; got != "low quality EITS: 10/10 programs missing titles" {
		t.Fatalf("extra service warning = %q", got)
	}
}

func TestCollectServiceSnapshotsDoesNotTreatExtendedOnlyAsObserved(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart+8, key, 10),
	}}

	_, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session, []ServiceKey{key}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if status.successes[key] != 0 {
		t.Fatal("extended-only service recorded success")
	}
	if got, want := status.errors[key], "service 101 EITS incomplete"; got != want {
		t.Fatalf("service error = %q, want %q", got, want)
	}
}

func TestCollectServiceSnapshotsRequiresMatchingTransportStreamID(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101, TransportStreamID: 100}
	wrongTS := ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID, TransportStreamID: 200}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, wrongTS, 10),
	}}

	result, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session, []ServiceKey{key}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if result == nil {
		t.Fatal("CollectServiceSnapshots result = nil")
	}
	if len(result.Observed) != 0 {
		t.Fatalf("observed services = %v, want none", result.Observed)
	}
	if !equalServiceKeys(result.Unobserved, []ServiceKey{key}) {
		t.Fatalf("unobserved services = %v, want [%v]", result.Unobserved, key)
	}
	if status.successes[ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}] != 0 {
		t.Fatal("TSID-mismatched service recorded success")
	}
}

func TestCollectServiceSnapshotsToleratesLateConcurrentObserve(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	session := &lateObserveEITSession{section: testEIT(ts.TableIDEITSStart, key, 10), done: make(chan struct{})}

	if _, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("late observe did not finish")
	}
}

func TestCollectServiceSnapshotsDoesNotDrainCollectorAfterParentCancel(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	ctx, cancel := context.WithCancel(context.Background())
	session := &stuckEITSession{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := CollectServiceSnapshots(ctx, &collectProgramStore{}, newRemoteSyncServiceStore(), session, []ServiceKey{key}, time.Second)
		done <- err
	}()
	<-session.started
	cancel()
	started := time.Now()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("CollectServiceSnapshots waited for collector drain after parent cancel")
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("CollectServiceSnapshots took %s after parent cancel, want no 2s drain", elapsed)
	}
	close(session.release)
}

func TestGatherNetworkTimesOutWhileWaitingForSession(t *testing.T) {
	streams := blockingEPGStreams{}
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	started := time.Now()
	err := gatherNetwork(
		context.Background(),
		&collectProgramStore{},
		newRemoteSyncServiceStore(),
		streams,
		key.NetworkID,
		[]Candidate{{Type: "GR", Channel: "27"}},
		[]ServiceKey{key},
		20*time.Millisecond,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gatherNetwork error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("gatherNetwork took %s, want session wait bounded by retrieval time", elapsed)
	}
}

func TestGatherNetworkMergesAfterCollectionTimeout(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &contextCheckingProgramStore{}
	streams := staticEPGStreams{session: &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, key, 10),
	}}}

	err := gatherNetwork(
		context.Background(),
		store,
		newRemoteSyncServiceStore(),
		streams,
		key.NetworkID,
		[]Candidate{{Type: "GR", Channel: "27"}},
		[]ServiceKey{key},
		20*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 {
		t.Fatalf("upsert calls = %d, want 1", store.calls)
	}
}

func TestGatherNetworkCarriesUnobservedServicesToNextCandidate(t *testing.T) {
	tsA := ServiceKey{NetworkID: 4, ServiceID: 101, TransportStreamID: 100}
	tsB := ServiceKey{NetworkID: 4, ServiceID: 161, TransportStreamID: 101}
	sessionA := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, tsA, 10),
	}}
	sessionB := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, tsB, 20),
	}}
	status := newRemoteSyncServiceStore()
	streams := keyedEPGStreams{sessions: map[Candidate]stream.Session{
		{Type: "BS", Channel: "BS01_0"}: sessionA,
		{Type: "BS", Channel: "BS01_1"}: sessionB,
	}}

	err := gatherNetwork(
		context.Background(),
		&collectProgramStore{},
		status,
		streams,
		tsA.NetworkID,
		[]Candidate{{Type: "BS", Channel: "BS01_0"}, {Type: "BS", Channel: "BS01_1"}},
		[]ServiceKey{tsA, tsB},
		20*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sessionA.collectCalls != 1 || sessionB.collectCalls != 1 {
		t.Fatalf("CollectEIT calls = %d/%d, want 1/1", sessionA.collectCalls, sessionB.collectCalls)
	}
	for _, key := range []ServiceKey{tsA, tsB} {
		statusKey := ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}
		if status.successes[statusKey] == 0 {
			t.Fatalf("service %d did not record success", key.ServiceID)
		}
		if got := status.errors[statusKey]; got != "" {
			t.Fatalf("service %d final error = %q, want cleared", key.ServiceID, got)
		}
	}
}

func TestBuildNetworkInputsFiltersServicesWithoutEITSchedule(t *testing.T) {
	store := &staticEPGServiceStore{services: []*servicepkg.Service{
		{NetworkId: 4, ServiceId: 101, EITScheduleFlag: true, ChannelType: "GR", ChannelId: "27"},
		{NetworkId: 4, ServiceId: 102, EITScheduleFlag: false, ChannelType: "GR", ChannelId: "27"},
		{NetworkId: 5, ServiceId: 201, EITScheduleFlag: true, ChannelType: "GR", ChannelId: "27"},
	}}
	channels := []config.ChannelConfig{{Type: "GR", Channel: "27"}}

	_, services, err := buildNetworkInputs(context.Background(), store, channels, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []ServiceKey{{NetworkID: 4, ServiceID: 101}}
	if len(services) != len(want) || services[0] != want[0] {
		t.Fatalf("network services = %v, want %v", services, want)
	}
}

func TestBuildNetworkInputsUsesAllConfiguredChannelsForNetworkType(t *testing.T) {
	store := &staticEPGServiceStore{services: []*servicepkg.Service{
		{NetworkId: 4, ServiceId: 151, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "BS01"},
	}}
	channels := []config.ChannelConfig{
		{Type: "USER_DEFINED", Channel: "BS01"},
		{Type: "USER_DEFINED", Channel: "BS03"},
		{Type: "GR", Channel: "27"},
	}

	candidates, _, err := buildNetworkInputs(context.Background(), store, channels, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{{Type: "USER_DEFINED", Channel: "BS01"}, {Type: "USER_DEFINED", Channel: "BS03"}}
	if !equalCandidates(candidates, want) {
		t.Fatalf("candidates = %v, want %v", candidates, want)
	}
}

func TestBuildNetworkInputsLimitsBroadTypeWhenMultipleNetworksExist(t *testing.T) {
	store := &staticEPGServiceStore{services: []*servicepkg.Service{
		{NetworkId: 6, ServiceId: 296, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS2"},
		{NetworkId: 7, ServiceId: 250, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS4"},
		{NetworkId: 7, ServiceId: 294, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS6"},
	}}
	channels := []config.ChannelConfig{
		{Type: "USER_DEFINED", Channel: "CS2"},
		{Type: "USER_DEFINED", Channel: "CS4"},
		{Type: "USER_DEFINED", Channel: "CS6"},
		{Type: "USER_DEFINED", Channel: "CS8"},
	}

	candidates, _, err := buildNetworkInputs(context.Background(), store, channels, 7)
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{{Type: "USER_DEFINED", Channel: "CS4"}, {Type: "USER_DEFINED", Channel: "CS6"}}
	if !equalCandidates(candidates, want) {
		t.Fatalf("candidates = %v, want %v", candidates, want)
	}
}

func TestBuildNetworkInputsDoesNotUseBroadCandidatesForTerrestrialNetwork(t *testing.T) {
	store := &staticEPGServiceStore{services: []*servicepkg.Service{
		{NetworkId: 32736, ServiceId: 101, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "27"},
	}}
	channels := []config.ChannelConfig{
		{Type: "USER_DEFINED", Channel: "27"},
		{Type: "USER_DEFINED", Channel: "28"},
	}

	candidates, _, err := buildNetworkInputs(context.Background(), store, channels, 32736)
	if err != nil {
		t.Fatal(err)
	}
	want := []Candidate{{Type: "USER_DEFINED", Channel: "27"}}
	if !equalCandidates(candidates, want) {
		t.Fatalf("candidates = %v, want %v", candidates, want)
	}
}

func TestGroupServicesByNetworkLimitsBroadTypeWhenMultipleNetworksExist(t *testing.T) {
	services := []*servicepkg.Service{
		{NetworkId: 6, ServiceId: 296, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS2"},
		{NetworkId: 7, ServiceId: 250, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS4"},
		{NetworkId: 7, ServiceId: 294, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "CS6"},
	}
	channels := []config.ChannelConfig{
		{Type: "USER_DEFINED", Channel: "CS2"},
		{Type: "USER_DEFINED", Channel: "CS4"},
		{Type: "USER_DEFINED", Channel: "CS6"},
		{Type: "USER_DEFINED", Channel: "CS8"},
	}

	groups := groupServicesByNetwork(services, channels)
	if !equalCandidates(groups[6].Candidates, []Candidate{{Type: "USER_DEFINED", Channel: "CS2"}}) {
		t.Fatalf("NID 6 candidates = %v", groups[6].Candidates)
	}
	wantNID7 := []Candidate{{Type: "USER_DEFINED", Channel: "CS4"}, {Type: "USER_DEFINED", Channel: "CS6"}}
	if !equalCandidates(groups[7].Candidates, wantNID7) {
		t.Fatalf("NID 7 candidates = %v, want %v", groups[7].Candidates, wantNID7)
	}
}

func TestGroupServicesByNetworkUsesBroadCandidatesOnlyForSatelliteNetwork(t *testing.T) {
	services := []*servicepkg.Service{
		{NetworkId: 4, ServiceId: 151, EITScheduleFlag: true, ChannelType: "USER_DEFINED", ChannelId: "BS01"},
		{NetworkId: 32736, ServiceId: 101, EITScheduleFlag: true, ChannelType: "LOCAL", ChannelId: "27"},
	}
	channels := []config.ChannelConfig{
		{Type: "USER_DEFINED", Channel: "BS01"},
		{Type: "USER_DEFINED", Channel: "BS03"},
		{Type: "LOCAL", Channel: "27"},
		{Type: "LOCAL", Channel: "28"},
	}

	groups := groupServicesByNetwork(services, channels)
	wantSatellite := []Candidate{{Type: "USER_DEFINED", Channel: "BS01"}, {Type: "USER_DEFINED", Channel: "BS03"}}
	if !equalCandidates(groups[4].Candidates, wantSatellite) {
		t.Fatalf("NID 4 candidates = %v, want %v", groups[4].Candidates, wantSatellite)
	}
	wantTerrestrial := []Candidate{{Type: "LOCAL", Channel: "27"}}
	if !equalCandidates(groups[32736].Candidates, wantTerrestrial) {
		t.Fatalf("NID 32736 candidates = %v, want %v", groups[32736].Candidates, wantTerrestrial)
	}
}

func TestCollectServiceSnapshotsDoesNotFailWhenSomeServicesUnobserved(t *testing.T) {
	observed := ServiceKey{NetworkID: 4, ServiceID: 101}
	missing := ServiceKey{NetworkID: 4, ServiceID: 102}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, observed, 10),
	}}

	result, err := CollectServiceSnapshots(context.Background(), store, status, session, []ServiceKey{observed, missing}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !equalServiceKeys(result.Observed, []ServiceKey{observed}) {
		t.Fatalf("observed services = %v, want [%v]", result.Observed, observed)
	}
	if !equalServiceKeys(result.Unobserved, []ServiceKey{missing}) {
		t.Fatalf("unobserved services = %v, want [%v]", result.Unobserved, missing)
	}
	if status.successes[observed] == 0 {
		t.Fatal("observed service did not record success")
	}
	if status.successes[missing] != 0 {
		t.Fatal("unobserved service recorded success")
	}
	if got, want := status.errors[missing], "service 102 EITS incomplete"; got != want {
		t.Fatalf("unobserved service error = %q, want %q", got, want)
	}
}

func TestCollectServiceSnapshotsFailsWhenNoServicesObserved(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{}

	_, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session, []ServiceKey{key}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if got, want := status.errors[key], "service 101 EITS incomplete"; got != want {
		t.Fatalf("service error = %q, want %q", got, want)
	}
}

func TestCollectServiceSnapshotsStoresLowQualityWarningWithoutFailing(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testSparseEIT(ts.TableIDEITSStart, key, 10),
	}}

	_, err := CollectServiceSnapshots(context.Background(), store, status, session, []ServiceKey{key}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if status.successes[key] == 0 {
		t.Fatal("service did not record success")
	}
	if got := status.errors[key]; got != "low quality EITS: 10/10 programs missing titles" {
		t.Fatalf("service warning = %q", got)
	}
	if got := len(store.calls); got != 1 {
		t.Fatalf("upsert calls = %d, want 1", got)
	}
	if got := len(store.calls[0]); got != 10 {
		t.Fatalf("upserted programs = %d, want 10", got)
	}
}

func TestCollectServiceSnapshotsUsesBroadcastClockForSuccessTimestamp(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	clock := time.Date(2026, 6, 29, 12, 34, 56, 0, time.FixedZone("JST", 9*60*60))
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITClockSession{sections: []clockedEIT{{
		eit:   testEIT(ts.TableIDEITSStart, key, 10),
		clock: clock,
	}}}

	if _, err := CollectServiceSnapshots(context.Background(), store, status, session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := status.successes[key], clock.UnixMilli(); got != want {
		t.Fatalf("success timestamp = %d, want TOT clock %d", got, want)
	}
	if session.collectCalls != 1 {
		t.Fatalf("CollectEITWithClock calls = %d, want 1", session.collectCalls)
	}
}

func TestServiceCleanupUsesCleanupMetricSource(t *testing.T) {
	store := &collectProgramStore{}
	service := NewService(store, newRemoteSyncServiceStore(), nil, nil, 1, time.Second)

	if err := service.Cleanup(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if store.deleteSource != "cleanup" {
		t.Fatalf("delete source = %q, want cleanup", store.deleteSource)
	}
}

type blockingEPGStreams struct{}

func (blockingEPGStreams) HasSession(string, string) bool { return false }

func (blockingEPGStreams) GetOrCreateWait(ctx context.Context, _, _ string) (stream.Session, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type staticEPGStreams struct {
	session stream.Session
}

func (staticEPGStreams) HasSession(string, string) bool { return false }

func (s staticEPGStreams) GetOrCreateWait(ctx context.Context, _, _ string) (stream.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.session, nil
}

type keyedEPGStreams struct {
	sessions map[Candidate]stream.Session
}

func (keyedEPGStreams) HasSession(string, string) bool { return false }

func (s keyedEPGStreams) GetOrCreateWait(ctx context.Context, typ, ch string) (stream.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session := s.sessions[Candidate{Type: typ, Channel: ch}]
	if session == nil {
		return nil, errors.New("missing test session")
	}
	return session, nil
}

type collectEITSession struct {
	stream.Session
	sections     []*ts.EIT
	collectCalls int
}

func (s *collectEITSession) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	s.collectCalls++
	for _, section := range s.sections {
		if err := observe(section); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type lateObserveEITSession struct {
	section *ts.EIT
	done    chan struct{}
}

func (s *lateObserveEITSession) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	if err := observe(s.section); err != nil {
		return err
	}
	go func() {
		defer close(s.done)
		time.Sleep(time.Millisecond)
		_ = observe(s.section)
	}()
	return nil
}

type stuckEITSession struct {
	started chan struct{}
	release chan struct{}
}

func (s *stuckEITSession) CollectEIT(context.Context, func(*ts.EIT) error) error {
	close(s.started)
	<-s.release
	return context.Canceled
}

type clockedEIT struct {
	eit   *ts.EIT
	clock time.Time
}

type collectEITClockSession struct {
	sections     []clockedEIT
	collectCalls int
}

func (s *collectEITClockSession) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	for _, section := range s.sections {
		if err := observe(section.eit); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *collectEITClockSession) CollectEITWithClock(ctx context.Context, observe func(*ts.EIT, time.Time) error) error {
	s.collectCalls++
	for _, section := range s.sections {
		if err := observe(section.eit, section.clock); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type collectProgramStore struct {
	calls        [][]*program.Program
	failEventID  uint16
	failErr      error
	sources      []string
	deleteSource string
}

type staticEPGServiceStore struct {
	services []*servicepkg.Service
}

func (s *staticEPGServiceStore) GetServices(context.Context) ([]*servicepkg.Service, error) {
	return s.services, nil
}

func (s *staticEPGServiceStore) SetEPGAttempt(context.Context, uint16, uint16, int64, string) error {
	return nil
}

func (s *staticEPGServiceStore) SetEPGSuccess(context.Context, uint16, uint16, int64) error {
	return nil
}

func (s *collectProgramStore) UpsertPrograms(ctx context.Context, programs []*program.Program) error {
	s.calls = append(s.calls, append([]*program.Program(nil), programs...))
	s.sources = append(s.sources, observability.EPGMetricSource(ctx))
	if len(programs) > 0 && programs[0].EventID == s.failEventID {
		return s.failErr
	}
	return nil
}

func (s *collectProgramStore) DeleteEndedBefore(ctx context.Context, _ int64) error {
	s.deleteSource = observability.EPGMetricSource(ctx)
	return nil
}

func (s *collectProgramStore) ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error {
	return nil
}

func (s *collectProgramStore) eventIDs() []uint16 {
	var ids []uint16
	for _, call := range s.calls {
		for _, item := range call {
			ids = append(ids, item.EventID)
		}
	}
	return ids
}

type contextCheckingProgramStore struct {
	calls int
}

func (s *contextCheckingProgramStore) UpsertPrograms(ctx context.Context, programs []*program.Program) error {
	s.calls++
	return ctx.Err()
}

func (s *contextCheckingProgramStore) DeleteEndedBefore(context.Context, int64) error {
	return nil
}

func (s *contextCheckingProgramStore) ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error {
	return nil
}

func testEIT(tableID byte, key ServiceKey, eventID uint16) *ts.EIT {
	return &ts.EIT{
		OriginalNetworkID:        key.NetworkID,
		TransportStreamID:        key.TransportStreamID,
		ServiceID:                key.ServiceID,
		TableID:                  tableID,
		SectionNumber:            0,
		LastSectionNumber:        0,
		SegmentLastSectionNumber: 0,
		Events: []ts.EITEvent{{
			EventID:   eventID,
			StartTime: time.Unix(int64(eventID), 0),
			Duration:  time.Minute,
		}},
	}
}

func testSparseEIT(tableID byte, key ServiceKey, count int) *ts.EIT {
	eit := &ts.EIT{
		OriginalNetworkID:        key.NetworkID,
		TransportStreamID:        key.TransportStreamID,
		ServiceID:                key.ServiceID,
		TableID:                  tableID,
		SectionNumber:            0,
		LastSectionNumber:        0,
		SegmentLastSectionNumber: 0,
	}
	for i := 0; i < count; i++ {
		eit.Events = append(eit.Events, ts.EITEvent{
			EventID:   uint16(100 + i),
			StartTime: time.Unix(int64(100+i), 0),
			Duration:  time.Minute,
		})
	}
	return eit
}

func equalEventIDs(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsEventIDs(got, want []uint16) bool {
	seen := make(map[uint16]int, len(got))
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		if seen[id] == 0 {
			return false
		}
		seen[id]--
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalCandidates(a, b []Candidate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalServiceKeys(a, b []ServiceKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
