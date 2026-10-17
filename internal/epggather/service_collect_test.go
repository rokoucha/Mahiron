package epggather

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	servicepkg "github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/go-cmp/cmp"
)

func TestCollectServiceSnapshotsRoutesEITSAndEITPF(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, model.ServiceKey{NetworkID: 4, ServiceID: 102}, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
		testEIT(ts.TableIDEITSStart, model.ServiceKey{NetworkID: 4, ServiceID: 102}, 20),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if session.collectCalls != 1 {
		t.Fatalf("CollectSchedule calls = %d, want 1", session.collectCalls)
	}
	if got, want := store.eventIDs(), []uint16{1, 10, 20}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
	if got, want := store.sources, []string{"eitpf", "eits", "eits"}; !equalStrings(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsContinuesEITSAfterEITPFFailure(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	pfErr := errors.New("p/f upsert failed")
	store := &collectProgramStore{failEventID: 1, failErr: pfErr}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, key, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{1, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsWaitsForBasicBeforeUpsertingExtended(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart+8, key, 10),
		testEIT(ts.TableIDEITSStart, key, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond); err != nil {
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

	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	missing := model.ServiceKey{NetworkID: 4, ServiceID: 102}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, key, 10)}}

	if _, err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key, missing}, 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{10, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want partial and final %v", got, want)
	}
}

func TestCollectServiceSnapshotsKeepsSameStreamServicesOutsideExpected(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, ServiceID: 151, StreamID: 100}
	extra := model.ServiceKey{NetworkID: 4, ServiceID: 161, StreamID: 100}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, expected, 10),
		testEIT(ts.TableIDEITSStart, extra, 20),
	}}

	result, err := CollectServiceSnapshots(context.Background(), store, status, session.CollectSchedule, []model.ServiceKey{expected}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if want := []model.ServiceKey{expected, extra}; !equalServiceKeys(result.Observed, want) {
		t.Fatalf("observed services = %v, want %v", result.Observed, want)
	}
	if got := store.eventIDs(); !containsEventIDs(got, []uint16{10, 20}) {
		t.Fatalf("upserted event IDs = %v, want expected and same-stream extra events", got)
	}
	for _, key := range []model.ServiceKey{expected, extra} {
		statusKey := model.ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}
		if status.successes[statusKey] == 0 {
			t.Fatalf("service %d did not record success", key.ServiceID)
		}
	}
}

func TestCollectServiceSnapshotsStoresWarningForObservedServiceOutsideExpected(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, ServiceID: 151, StreamID: 100}
	extra := model.ServiceKey{NetworkID: 4, ServiceID: 161, StreamID: 100}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, expected, 10),
		testSparseEIT(ts.TableIDEITSStart, extra, 10),
	}}

	if _, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session.CollectSchedule, []model.ServiceKey{expected}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	statusKey := model.ServiceKey{NetworkID: extra.NetworkID, ServiceID: extra.ServiceID}
	if status.successes[statusKey] == 0 {
		t.Fatal("extra service did not record success")
	}
	if got := status.errors[statusKey]; got != "low quality EITS: 10/10 programs missing titles" {
		t.Fatalf("extra service warning = %q", got)
	}
}

func TestCollectServiceSnapshotsDoesNotTreatExtendedOnlyAsObserved(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart+8, key, 10),
	}}

	_, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond)
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
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101, StreamID: 100}
	wrongTS := model.ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID, StreamID: 200}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, wrongTS, 10),
	}}

	result, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if result == nil {
		t.Fatal("CollectServiceSnapshots result = nil")
	}
	if len(result.Observed) != 0 {
		t.Fatalf("observed services = %v, want none", result.Observed)
	}
	if !equalServiceKeys(result.Unobserved, []model.ServiceKey{key}) {
		t.Fatalf("unobserved services = %v, want [%v]", result.Unobserved, key)
	}
	if status.successes[model.ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}] != 0 {
		t.Fatal("TSID-mismatched service recorded success")
	}
}

// A remote that omits transportStreamId leaves the expected stream ID at
// zero; the stream's real ID must still count as the expected service.
func TestCollectServiceSnapshotsMatchesObservedStreamToUnknownExpectedStream(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	observed := model.ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID, StreamID: 200}
	session := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, observed, 10)}}

	result, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !equalServiceKeys(result.Observed, []model.ServiceKey{key}) || len(result.Unobserved) != 0 {
		t.Fatalf("observed = %v, unobserved = %v, want [%v] and none", result.Observed, result.Unobserved, key)
	}
}

func TestCollectServiceSnapshotsToleratesLateConcurrentObserve(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	session := &lateObserveEITSession{section: testEIT(ts.TableIDEITSStart, key, 10), done: make(chan struct{})}

	if _, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("late observe did not finish")
	}
}

func TestCollectServiceSnapshotsDoesNotDrainCollectorAfterParentCancel(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	ctx, cancel := context.WithCancel(context.Background())
	session := &stuckEITSession{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := CollectServiceSnapshots(ctx, &collectProgramStore{}, newRemoteSyncServiceStore(), session.CollectSchedule, []model.ServiceKey{key}, time.Second)
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
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	started := time.Now()
	err := gatherNetwork(
		context.Background(),
		&collectProgramStore{},
		&collectProgramStore{},
		newRemoteSyncServiceStore(),
		streams,
		key.NetworkID,
		[]Candidate{{Type: "GR", Channel: "27"}},
		[]model.ServiceKey{key},
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
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &contextCheckingProgramStore{}
	streams := staticEPGStreams{session: &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, key, 10),
	}}}

	err := gatherNetwork(
		context.Background(),
		store,
		store,
		newRemoteSyncServiceStore(),
		streams,
		key.NetworkID,
		[]Candidate{{Type: "GR", Channel: "27"}},
		[]model.ServiceKey{key},
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
	tsA := model.ServiceKey{NetworkID: 4, ServiceID: 101, StreamID: 100}
	tsB := model.ServiceKey{NetworkID: 4, ServiceID: 161, StreamID: 101}
	sessionA := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, tsA, 10),
	}}
	sessionB := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, tsB, 20),
	}}
	status := newRemoteSyncServiceStore()
	streams := keyedEPGStreams{sessions: map[Candidate]scheduleSession{
		{Type: "BS", Channel: "BS01_0"}: sessionA,
		{Type: "BS", Channel: "BS01_1"}: sessionB,
	}}

	err := gatherNetwork(
		context.Background(),
		&collectProgramStore{},
		&collectProgramStore{},
		status,
		streams,
		tsA.NetworkID,
		[]Candidate{{Type: "BS", Channel: "BS01_0"}, {Type: "BS", Channel: "BS01_1"}},
		[]model.ServiceKey{tsA, tsB},
		20*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sessionA.collectCalls != 1 || sessionB.collectCalls != 1 {
		t.Fatalf("CollectSchedule calls = %d/%d, want 1/1", sessionA.collectCalls, sessionB.collectCalls)
	}
	for _, key := range []model.ServiceKey{tsA, tsB} {
		statusKey := model.ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID}
		if status.successes[statusKey] == 0 {
			t.Fatalf("service %d did not record success", key.ServiceID)
		}
		if got := status.errors[statusKey]; got != "" {
			t.Fatalf("service %d final error = %q, want cleared", key.ServiceID, got)
		}
	}
}

func TestCollectServiceSnapshotsDoesNotFailWhenSomeServicesUnobserved(t *testing.T) {
	observed := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	missing := model.ServiceKey{NetworkID: 4, ServiceID: 102}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, observed, 10),
	}}

	result, err := CollectServiceSnapshots(context.Background(), store, status, session.CollectSchedule, []model.ServiceKey{observed, missing}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !equalServiceKeys(result.Observed, []model.ServiceKey{observed}) {
		t.Fatalf("observed services = %v, want [%v]", result.Observed, observed)
	}
	if !equalServiceKeys(result.Unobserved, []model.ServiceKey{missing}) {
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
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{}

	_, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if got, want := status.errors[key], "service 101 EITS incomplete"; got != want {
		t.Fatalf("service error = %q, want %q", got, want)
	}
}

func TestCollectServiceSnapshotsAbortsEarlyOnDeadStream(t *testing.T) {
	previous := eitsDeadStreamTimeout
	eitsDeadStreamTimeout = 10 * time.Millisecond
	t.Cleanup(func() { eitsDeadStreamTimeout = previous })

	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{}

	started := time.Now()
	_, err := CollectServiceSnapshots(context.Background(), &collectProgramStore{}, status, session.CollectSchedule, []model.ServiceKey{key}, time.Minute)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want incomplete service error")
	}
	if got, want := status.errors[key], "service 101 EITS incomplete"; got != want {
		t.Fatalf("service error = %q, want %q", got, want)
	}
	if elapsed >= time.Minute {
		t.Fatalf("collection took %s, want it to abort well before the retrievalTime", elapsed)
	}
}

func TestCollectServiceSnapshotsStoresLowQualityWarningWithoutFailing(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testSparseEIT(ts.TableIDEITSStart, key, 10),
	}}

	_, err := CollectServiceSnapshots(context.Background(), store, status, session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond)
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
	key := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	clock := time.Date(2026, 6, 29, 12, 34, 56, 0, time.FixedZone("JST", 9*60*60))
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITClockSession{sections: []clockedEIT{{
		eit:   testEIT(ts.TableIDEITSStart, key, 10),
		clock: clock,
	}}}

	if _, err := CollectServiceSnapshots(context.Background(), store, status, session.CollectSchedule, []model.ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := status.successes[key], clock.UnixMilli(); got != want {
		t.Fatalf("success timestamp = %d, want TOT clock %d", got, want)
	}
	if session.collectCalls != 1 {
		t.Fatalf("CollectSchedule calls = %d, want 1", session.collectCalls)
	}
}

type scheduleSession interface {
	CollectSchedule(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error
}

type blockingEPGStreams struct{}

func (blockingEPGStreams) HasSession(string, string) bool { return false }

func (blockingEPGStreams) StoredEPG(string, string) bool { return false }

func (blockingEPGStreams) OpenSchedule(ctx context.Context, _, _ string) (CollectSchedule, ListStoredPrograms, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

type staticEPGStreams struct {
	session scheduleSession
}

func (staticEPGStreams) HasSession(string, string) bool { return false }

func (staticEPGStreams) StoredEPG(string, string) bool { return false }

func (s staticEPGStreams) OpenSchedule(ctx context.Context, _, _ string) (CollectSchedule, ListStoredPrograms, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.session.CollectSchedule, nil, nil
}

type keyedEPGStreams struct {
	sessions map[Candidate]scheduleSession
}

func (keyedEPGStreams) HasSession(string, string) bool { return false }

func (keyedEPGStreams) StoredEPG(string, string) bool { return false }

func (s keyedEPGStreams) OpenSchedule(ctx context.Context, typ, ch string) (CollectSchedule, ListStoredPrograms, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	session := s.sessions[Candidate{Type: typ, Channel: ch}]
	if session == nil {
		return nil, nil, errors.New("missing test session")
	}
	return session.CollectSchedule, nil, nil
}

// eitFeeder passes TS EIT sections through the session-side schedule
// collector, as channel.Session.CollectSchedule does.
func eitFeeder(onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) func(*ts.EIT, time.Time) error {
	collector := schedule.NewCollector(isdb.ScheduleTS)
	return func(eit *ts.EIT, clock time.Time) error {
		if clock.IsZero() {
			clock = time.Now()
		}
		return collector.Observe(channel.ScheduleSection(eit), clock, onSchedule, onPresentFollowing)
	}
}

type collectEITSession struct {
	sections     []*ts.EIT
	collectCalls int
}

func (s *collectEITSession) CollectSchedule(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
	s.collectCalls++
	observe := eitFeeder(onSchedule, onPresentFollowing)
	for _, section := range s.sections {
		if err := observe(section, time.Time{}); err != nil {
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

func (s *lateObserveEITSession) CollectSchedule(_ context.Context, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
	observe := eitFeeder(onSchedule, onPresentFollowing)
	if err := observe(s.section, time.Time{}); err != nil {
		return err
	}
	go func() {
		defer close(s.done)
		time.Sleep(time.Millisecond)
		late := *s.section
		late.VersionNumber++
		_ = observe(&late, time.Time{})
	}()
	return nil
}

type stuckEITSession struct {
	started chan struct{}
	release chan struct{}
}

func (s *stuckEITSession) CollectSchedule(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error {
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

func (s *collectEITClockSession) CollectSchedule(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
	s.collectCalls++
	observe := eitFeeder(onSchedule, onPresentFollowing)
	for _, section := range s.sections {
		if err := observe(section.eit, section.clock); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type collectProgramStore struct {
	calls       [][]*program.Program
	failEventID uint16
	failErr     error
	sources     []string
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

func (s *collectProgramStore) UpsertEvents(ctx context.Context, events []model.Event) error {
	programs := make([]*program.Program, len(events))
	for i := range events {
		programs[i] = program.FromEvent(events[i])
	}
	return s.UpsertPrograms(ctx, programs)
}

func (s *collectProgramStore) UpsertPrograms(ctx context.Context, programs []*program.Program) error {
	s.calls = append(s.calls, append([]*program.Program(nil), programs...))
	s.sources = append(s.sources, observability.EPGMetricSource(ctx))
	if len(programs) > 0 && programs[0].EventID == s.failEventID {
		return s.failErr
	}
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

func (s *contextCheckingProgramStore) UpsertEvents(ctx context.Context, _ []model.Event) error {
	s.calls++
	return ctx.Err()
}

func (s *contextCheckingProgramStore) DeleteEndedBefore(context.Context, int64) error {
	return nil
}

func (s *contextCheckingProgramStore) ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error {
	return nil
}

func testEIT(tableID byte, key model.ServiceKey, eventID uint16) *ts.EIT {
	return &ts.EIT{
		OriginalNetworkID:        key.NetworkID,
		TransportStreamID:        key.StreamID,
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

func testSparseEIT(tableID byte, key model.ServiceKey, count int) *ts.EIT {
	eit := &ts.EIT{
		OriginalNetworkID:        key.NetworkID,
		TransportStreamID:        key.StreamID,
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

func equalServiceKeys(a, b []model.ServiceKey) bool {
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

// channelServiceStore lists services on channels, as scans store them.
type channelServiceStore struct {
	*remoteSyncServiceStore
	services []*servicepkg.Service
}

func (s channelServiceStore) GetServices(context.Context) ([]*servicepkg.Service, error) {
	return s.services, nil
}

func TestGatherGroupCollectsEachTLVStreamIndependently(t *testing.T) {
	a := model.ServiceKey{NetworkID: 11, StreamID: 0xB110, ServiceID: 101}
	b := model.ServiceKey{NetworkID: 11, StreamID: 0xB0E0, ServiceID: 102}
	sessionA := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, a, 10)}}
	sessionB := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, b, 20)}}
	store := channelServiceStore{remoteSyncServiceStore: newRemoteSyncServiceStore(), services: []*servicepkg.Service{
		{Service: model.Service{Key: a, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "A"},
		{Service: model.Service{Key: b, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "B"},
	}}
	streams := keyedEPGStreams{sessions: map[Candidate]scheduleSession{
		{Type: "CUSTOM", Channel: "A"}: sessionA,
		{Type: "CUSTOM", Channel: "B"}: sessionB,
	}}
	events := &collectProgramStore{}
	gatherer := NewGatherer(events, events, store, streams, config.ChannelsConfig{
		{Type: "CUSTOM", Channel: "A", Transport: config.TransportTLV},
		{Type: "CUSTOM", Channel: "B", Transport: config.TransportTLV},
		{Type: "CUSTOM", Channel: "unscanned", Transport: config.TransportTLV},
	}, 20*time.Millisecond)
	if err := gatherer.GatherGroup(t.Context(), GroupKey{NetworkID: 11, StreamID: a.StreamID}); err != nil {
		t.Fatal(err)
	}
	if sessionA.collectCalls != 1 || sessionB.collectCalls != 0 {
		t.Fatalf("collection calls = %d/%d", sessionA.collectCalls, sessionB.collectCalls)
	}
	if err := gatherer.GatherGroup(t.Context(), GroupKey{NetworkID: 11, StreamID: b.StreamID}); err != nil {
		t.Fatal(err)
	}
	if sessionB.collectCalls != 1 {
		t.Fatalf("second collection calls = %d", sessionB.collectCalls)
	}
}

func TestCollectServiceSnapshotsStoresOtherStreamsWithoutUpdatingFreshness(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}
	other := model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 161}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	otherStatus := model.ServiceKey{NetworkID: other.NetworkID, ServiceID: other.ServiceID}
	status.attempts[otherStatus] = 123
	status.successes[otherStatus] = 100
	status.errors[otherStatus] = "previous failure"
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart, expected, 10),
		testEIT(ts.TableIDEITSStart+0x10, other, 20),
	}}
	result, err := CollectServiceSnapshots(t.Context(), store, status, session.CollectSchedule, []model.ServiceKey{expected}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !equalServiceKeys(result.Observed, []model.ServiceKey{expected}) {
		t.Fatalf("observed = %v", result.Observed)
	}
	if !containsEventIDs(store.eventIDs(), []uint16{20}) {
		t.Fatal("did not persist another stream's basic EPG")
	}
	if status.attempts[otherStatus] != 123 || status.successes[otherStatus] != 100 || status.errors[otherStatus] != "previous failure" {
		t.Fatalf("changed another stream's status: attempts=%v successes=%v errors=%v", status.attempts, status.successes, status.errors)
	}
}

func TestOtherStreamBasicEPGDoesNotCompleteMissingTarget(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}
	other := model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 161}
	foreign := model.ServiceKey{NetworkID: 7, StreamID: 101, ServiceID: 162}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITSStart+0x10, other, 20),
		testEIT(ts.TableIDEITSStart, foreign, 30),
	}}
	result, err := CollectServiceSnapshots(t.Context(), store, status, session.CollectSchedule, []model.ServiceKey{expected}, 20*time.Millisecond)
	if err == nil {
		t.Fatal("other stream completed the missing target")
	}
	if len(result.Observed) != 0 || !equalServiceKeys(result.Unobserved, []model.ServiceKey{expected}) {
		t.Fatalf("result = %#v", result)
	}
	if !containsEventIDs(store.eventIDs(), []uint16{20}) {
		t.Fatal("discarded other stream's basic EPG")
	}
	if containsEventIDs(store.eventIDs(), []uint16{30}) {
		t.Fatal("stored another network's EPG")
	}
	otherStatus := model.ServiceKey{NetworkID: other.NetworkID, ServiceID: other.ServiceID}
	if _, ok := status.attempts[otherStatus]; ok {
		t.Fatal("recorded attempt for another stream")
	}
	if _, ok := status.successes[otherStatus]; ok {
		t.Fatal("recorded success for another stream")
	}
}

func TestOtherStreamUpdatesDoNotDelayStableCompletion(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}
	other := model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 161}
	index := newExpectedServiceIndex([]model.ServiceKey{expected})
	schedule := newCollectionSchedule([]model.ServiceKey{expected})
	schedule.lastProgress = time.Now().Add(-time.Minute)
	update := model.ScheduleUpdate{Service: other, BasicObserved: true}
	schedule.observe(update, index.matchesCollectionStream(other))
	if !schedule.stableFor(time.Second) {
		t.Fatal("other stream reset target stability")
	}
	if shouldStopEITSCollection(schedule, []model.ServiceKey{expected}) {
		t.Fatal("other stream completed missing target")
	}
}

func TestOtherStreamBasicUpdateKeepsDetailGroupDue(t *testing.T) {
	ctx := t.Context()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	first := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}
	second := model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 161}
	channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "A"}, {Type: "CUSTOM", Channel: "B"}}
	services := servicepkg.NewManager(servicepkg.NewSQLiteStore(database), channels)
	for i, key := range []model.ServiceKey{first, second} {
		if err := services.ReplaceChannelServices(ctx, "CUSTOM", channels[i].Channel, []*servicepkg.Service{
			{Id: fmt.Sprint(key.MirakurunID()), Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: channels[i].Channel},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := services.SetEPGSuccess(ctx, 4, second.ServiceID, 100); err != nil {
		t.Fatal(err)
	}
	if err := services.SetEPGAttempt(ctx, 4, second.ServiceID, 123, "previous failure"); err != nil {
		t.Fatal(err)
	}
	programs := program.NewManager(program.NewSQLiteStore(database))
	priorStart := int64(1000)
	prior := model.Event{EITRevision: &model.EITRevision{TableID: 0x60}, Key: second, EventID: 20, StartAt: &priorStart, Name: "stored title", Extended: []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "出演者", Text: "stored details"}}}}}
	if err := programs.UpsertEvents(ctx, []model.Event{prior}); err != nil {
		t.Fatal(err)
	}
	sessionA := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, first, 10), testEIT(ts.TableIDEITSStart+0x10, second, 20)}}
	sessionB := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, second, 20)}}
	streams := keyedEPGStreams{sessions: map[Candidate]scheduleSession{{Type: "CUSTOM", Channel: "A"}: sessionA, {Type: "CUSTOM", Channel: "B"}: sessionB}}
	gatherer := NewGatherer(programs, programs, services, streams, channels, 20*time.Millisecond)
	if err := gatherer.GatherGroup(ctx, GroupKey{NetworkID: 4, StreamID: 100}); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := programs.Get(ctx, model.ProgramID(second, 20))
	if err != nil || !ok {
		t.Fatalf("get program: ok=%v err=%v", ok, err)
	}
	if stored.StartAt == nil || *stored.StartAt != 20000 {
		t.Fatalf("basic update was not stored: %#v", stored.StartAt)
	}
	if diff := cmp.Diff(prior.Extended, stored.Extended); diff != "" {
		t.Fatalf("basic update lost stored details: %s", diff)
	}
	records, err := services.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range records {
		if svc.Key != second {
			continue
		}
		if svc.EPG.LastAttemptAt == nil || *svc.EPG.LastAttemptAt != 123 || svc.EPG.LastSuccessAt == nil || *svc.EPG.LastSuccessAt != 100 || svc.EPG.LastError != "previous failure" {
			t.Fatalf("other stream's status changed: %#v", svc.EPG)
		}
	}
	groups, err := gatherer.Groups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := GroupKey{NetworkID: 4, StreamID: 101}
	if groups[key].OldestSuccessAt != 100 {
		t.Fatalf("detail group freshness = %d", groups[key].OldestSuccessAt)
	}
	if err := gatherer.GatherGroup(ctx, key); err != nil {
		t.Fatal(err)
	}
	if sessionB.collectCalls != 1 {
		t.Fatal("detail group did not collect its own stream")
	}
}

func TestUnknownTargetStreamDoesNotMarkOtherServicesFresh(t *testing.T) {
	expected := model.ServiceKey{NetworkID: 4, ServiceID: 151}
	observed := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}
	other := model.ServiceKey{NetworkID: 4, StreamID: 101, ServiceID: 161}
	store := &collectProgramStore{}
	status := newRemoteSyncServiceStore()
	session := &collectEITSession{sections: []*ts.EIT{testEIT(ts.TableIDEITSStart, observed, 10), testEIT(ts.TableIDEITSStart+0x10, other, 20)}}
	result, err := CollectServiceSnapshots(t.Context(), store, status, session.CollectSchedule, []model.ServiceKey{expected}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !equalServiceKeys(result.Observed, []model.ServiceKey{expected}) {
		t.Fatalf("observed = %v", result.Observed)
	}
	if !containsEventIDs(store.eventIDs(), []uint16{20}) {
		t.Fatal("other stream's basic EPG was discarded")
	}
	otherStatus := model.ServiceKey{NetworkID: other.NetworkID, ServiceID: other.ServiceID}
	if _, ok := status.attempts[otherStatus]; ok {
		t.Fatal("recorded attempt for unknown stream's peer")
	}
	if _, ok := status.successes[otherStatus]; ok {
		t.Fatal("recorded success for unknown stream's peer")
	}
	index := newExpectedServiceIndex([]model.ServiceKey{expected})
	if index.matchesCollectionStream(other) {
		t.Fatal("unknown stream matched unrelated service")
	}
}

func (s *collectProgramStore) UpsertSchedule(ctx context.Context, snapshot model.ScheduleSnapshot) error {
	return s.UpsertEvents(ctx, snapshot.Events)
}

func (s *contextCheckingProgramStore) UpsertSchedule(ctx context.Context, snapshot model.ScheduleSnapshot) error {
	return s.UpsertEvents(ctx, snapshot.Events)
}
