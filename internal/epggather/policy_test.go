package epggather

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

type receptionStore struct {
	*remoteSyncServiceStore
	reception service.EPGReception
}

func (s *receptionStore) SetEPGReception(_ context.Context, _, _ uint16, reception service.EPGReception) error {
	s.reception = reception
	return nil
}

func TestUnknownExtendedBudgets(t *testing.T) {
	for _, test := range []struct {
		name                                                  string
		probe, prior, observed, required, basic, receiveError bool
		wantYield, wantPartial                                bool
	}{
		{name: "unknown yields", basic: true, wantYield: true, wantPartial: true},
		{name: "long probe", basic: true, probe: true, wantPartial: true},
		{name: "previously observed", basic: true, prior: true},
		{name: "observed now", basic: true, observed: true},
		{name: "explicitly required", basic: true, required: true},
		{name: "basic incomplete"},
		{name: "receiver failed", basic: true, receiveError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := model.ServiceKey{NetworkID: 65534, StreamID: 1, ServiceID: 119}
			option := CollectOptions{UnknownExtendedIdle: 5 * time.Millisecond, ProbeExtended: test.probe, PreviouslyExtended: map[model.ServiceKey]bool{key: test.prior}}
			collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
				if err := onSchedule(model.ScheduleUpdate{Service: key, BasicObserved: true, BasicComplete: test.basic, ExtendedObserved: test.observed, ExtendedRequired: test.required, ObservedAt: time.Now().UnixMilli(), Events: func() []model.Event { return nil }}); err != nil {
					return err
				}
				if test.receiveError {
					return errors.New("receiver failed")
				}
				<-ctx.Done()
				return ctx.Err()
			}
			status := &receptionStore{remoteSyncServiceStore: newRemoteSyncServiceStore()}
			result, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, status, collect, []model.ServiceKey{key}, 200*time.Millisecond, option)
			if err != nil {
				t.Fatal(err)
			}
			if got := result.EndReason == "unknown_extended_idle"; got != test.wantYield {
				t.Fatalf("end=%s", result.EndReason)
			}
			if got := len(result.DeferredExtended) == 1; got != test.wantPartial {
				t.Fatalf("partial=%v result=%+v", got, result)
			}
			if status.reception.ExtendedProbed != (test.probe && test.basic && !test.receiveError) {
				t.Fatalf("probe recorded=%+v", status.reception)
			}
			if len(status.successes) != 0 {
				t.Fatal("unknown/incomplete reception advanced full success")
			}
		})
	}
}

func TestPartialReceptionDeadlinesSurviveDatabaseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epg.db")
	database, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}
	store := service.NewSQLiteStore(database)
	key := model.ServiceKey{NetworkID: 65534, StreamID: 10, ServiceID: 119}
	svc := &service.Service{Id: "catv", Service: model.Service{Key: key, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "1"}
	if err = store.ReplaceChannelServices(t.Context(), "CUSTOM", "1", []*service.Service{svc}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err = store.SetEPGAttempt(t.Context(), key.NetworkID, key.ServiceID, now, "unknown extended"); err != nil {
		t.Fatal(err)
	}
	if err = store.SetEPGReception(t.Context(), key.NetworkID, key.ServiceID, service.EPGReception{UpdatedAt: now, BasicComplete: true, ExtendedProbed: true, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store = service.NewSQLiteStore(database)
	services, err := store.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, failed, _, err := store.EPGSummary(t.Context(), 1000, now)
	if err != nil || failed != 0 {
		t.Fatalf("planned partial counted as failure: %d %v", failed, err)
	}
	if err = store.SetEPGAttempt(t.Context(), key.NetworkID, key.ServiceID, now+1, "real receiver failure"); err != nil {
		t.Fatal(err)
	}
	_, failed, _, err = store.EPGSummary(t.Context(), 1000, now+1)
	if err != nil || failed != 1 {
		t.Fatalf("real failure was hidden: %d %v", failed, err)
	}
	// Restore the expected partial outcome before checking its deadlines.
	if err = store.SetEPGAttempt(t.Context(), key.NetworkID, key.ServiceID, now, "unknown extended"); err != nil {
		t.Fatal(err)
	}
	policy := ReceptionPolicy{RefreshInterval: 30 * time.Minute, ExtendedProbeInterval: 6 * time.Hour, UnknownExtendedIdle: 30 * time.Second}
	channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "1"}}
	group := groupServices(services, channels, func(string, string) bool { return false }, policy)[GroupKey{NetworkID: key.NetworkID, StreamID: key.StreamID}]
	if group.NextDueAt != now+int64((30*time.Minute)/time.Millisecond) || group.LastFailureAt != 0 || group.OldestSuccessAt != 0 {
		t.Fatalf("group=%+v", group)
	}
	gatherer := &Gatherer{policy: policy}
	option := gatherer.collectOptions(services, group)
	if option.ProbeExtended {
		t.Fatal("restart forgot the recent long probe")
	}
	services[0].EPG.ExtendedProbeAt = new(now - int64((6*time.Hour)/time.Millisecond))
	if !gatherer.collectOptions(services, group).ProbeExtended {
		t.Fatal("long probe deadline missed")
	}
	if err = store.SetEPGReception(t.Context(), key.NetworkID, key.ServiceID, service.EPGReception{UpdatedAt: now, ExtendedObserved: true}); err != nil {
		t.Fatal(err)
	}
	if err = store.SetEPGReception(t.Context(), key.NetworkID, key.ServiceID, service.EPGReception{UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	services, err = store.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !services[0].EPG.ExtendedObserved {
		t.Fatal("later basic-only run erased observed extension evidence")
	}
}

func TestUnknownExtendedRequiresEveryTargetAndIgnoresOtherStreams(t *testing.T) {
	key := model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 1}
	missing := model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 2}
	other := model.ServiceKey{NetworkID: 1, StreamID: 20, ServiceID: 1}
	schedule := newCollectionSchedule([]model.ServiceKey{key, missing})
	option := CollectOptions{UnknownExtendedIdle: time.Second}
	schedule.observe(model.ScheduleUpdate{Service: key, BasicComplete: true}, true)
	schedule.observe(model.ScheduleUpdate{Service: other, BasicComplete: true, ExtendedComplete: true}, false)
	if option.canYield(schedule, []model.ServiceKey{key, missing}) {
		t.Fatal("missing target ignored")
	}
	schedule.observe(model.ScheduleUpdate{Service: missing, BasicComplete: true, ExtendedComplete: true}, true)
	if !option.canYield(schedule, []model.ServiceKey{key, missing}) {
		t.Fatal("complete peer prevented partial yield")
	}
	schedule.observe(model.ScheduleUpdate{Service: key, BasicComplete: false}, true)
	if option.canYield(schedule, []model.ServiceKey{key, missing}) {
		t.Fatal("coverage reset still yielded")
	}
}

func TestUnknownExtendedFailureDoesNotAdvanceReceptionEvidence(t *testing.T) {
	key := model.ServiceKey{NetworkID: 65534, StreamID: 10, ServiceID: 119}
	status := &receptionStore{remoteSyncServiceStore: newRemoteSyncServiceStore()}
	writer := &failingScheduleWriter{collectProgramStore: &collectProgramStore{}}
	collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		if err := onSchedule(model.ScheduleUpdate{Service: key, BasicObserved: true, BasicComplete: true, Events: func() []model.Event { return nil }}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	result, err := CollectServiceSnapshots(t.Context(), writer, status, collect, []model.ServiceKey{key}, 100*time.Millisecond, CollectOptions{UnknownExtendedIdle: 5 * time.Millisecond})
	if err == nil || len(result.DeferredExtended) != 0 || status.reception.BasicComplete || status.reception.ExtendedProbed {
		t.Fatalf("failed save advanced state: result=%+v reception=%+v err=%v", result, status.reception, err)
	}
}

type failingScheduleWriter struct{ *collectProgramStore }

func (*failingScheduleWriter) UpsertSchedule(context.Context, model.ScheduleSnapshot) error {
	return errors.New("save failed")
}

func TestExtendedOnlyObservationIsRemembered(t *testing.T) {
	key := model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 1}
	status := &receptionStore{remoteSyncServiceStore: newRemoteSyncServiceStore()}
	collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		if err := onSchedule(model.ScheduleUpdate{Service: key, ExtendedObserved: true}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	result, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, status, collect, []model.ServiceKey{key}, 20*time.Millisecond, CollectOptions{UnknownExtendedIdle: time.Millisecond})
	if err == nil || !status.reception.ExtendedObserved || status.reception.BasicComplete || len(result.DeferredExtended) > 0 {
		t.Fatalf("extended evidence lost: %+v %+v %v", result, status.reception, err)
	}
}

func TestFallbackPreservesEarlierPartialOutcome(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "basic only", true: "new extension evidence"}[observed], func(t *testing.T) {
			database, err := db.OpenInMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = database.Close() }()
			store := service.NewSQLiteStore(database)
			a := model.ServiceKey{NetworkID: 65534, StreamID: 10, ServiceID: 1}
			b := model.ServiceKey{NetworkID: 65534, StreamID: 10, ServiceID: 2}
			channels := config.ChannelsConfig{{Type: "CUSTOM", Channel: "1"}}
			if err = store.ReplaceChannelServices(t.Context(), "CUSTOM", "1", []*service.Service{{Id: "a", Service: model.Service{Key: a, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "1"}, {Id: "b", Service: model.Service{Key: b, EITSchedule: true}, ChannelType: "CUSTOM", ChannelId: "1"}}); err != nil {
				t.Fatal(err)
			}
			manager := service.NewManager(store, channels)
			now := time.Now().UnixMilli()
			update := func(key model.ServiceKey, complete bool, at int64) model.ScheduleUpdate {
				return model.ScheduleUpdate{Service: key, BasicObserved: true, BasicComplete: true, ExtendedComplete: complete, ObservedAt: at, Events: func() []model.Event { return nil }}
			}
			first := func(ctx context.Context, on func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
				if err := on(update(a, false, now)); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			}
			option := CollectOptions{UnknownExtendedIdle: time.Millisecond, ProbeExtended: true}
			if _, err = CollectServiceSnapshots(t.Context(), &collectProgramStore{}, manager, first, []model.ServiceKey{a, b}, 10*time.Millisecond, option); err != nil {
				t.Fatal(err)
			}
			second := func(ctx context.Context, on func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
				extra := update(a, false, now+1000)
				extra.ExtendedObserved = observed
				if err := on(extra); err != nil {
					return err
				}
				if err := on(update(b, true, now+1000)); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			}
			if _, err = CollectServiceSnapshots(t.Context(), &collectProgramStore{}, manager, second, []model.ServiceKey{b}, 10*time.Millisecond, option); err != nil {
				t.Fatal(err)
			}
			svc, err := store.GetByNetworkServiceID(t.Context(), a.NetworkID, a.ServiceID)
			if err != nil {
				t.Fatal(err)
			}
			if svc.EPG.PartialAt == nil || *svc.EPG.PartialAt != now || *svc.EPG.LastAttemptAt != now || *svc.EPG.BasicUpdatedAt != now || *svc.EPG.ExtendedProbeAt != now || svc.EPG.ExtendedObserved != observed {
				t.Fatalf("earlier outcome overwritten: %+v", svc.EPG)
			}
			_, failed, _, err := store.EPGSummary(t.Context(), 1000, now+1000)
			if err != nil || failed != 0 {
				t.Fatalf("fallback made a false failure: %d %v", failed, err)
			}
		})
	}
}

func TestReceptionMetadataFailureStillRecordsFullSuccess(t *testing.T) {
	key := model.ServiceKey{NetworkID: 1, ServiceID: 1}
	status := &failingReceptionStore{remoteSyncServiceStore: newRemoteSyncServiceStore()}
	collect := func(_ context.Context, on func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		return on(model.ScheduleUpdate{Service: key, BasicObserved: true, BasicComplete: true, ExtendedComplete: true, ObservedAt: 1000, Events: func() []model.Event { return nil }})
	}
	_, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, status, collect, []model.ServiceKey{key}, time.Second)
	if err == nil || status.successes[key] != 1000 {
		t.Fatalf("metadata failure suppressed full success: %v %+v", err, status.successes)
	}
}

type failingReceptionStore struct{ *remoteSyncServiceStore }

func (*failingReceptionStore) SetEPGReception(context.Context, uint16, uint16, service.EPGReception) error {
	return errors.New("metadata write failed")
}
