package epggather

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type resultTestStreams struct {
	collect    CollectSchedule
	openError  error
	firstError error
	opened     []string
}

func (*resultTestStreams) HasSession(string, string) bool                       { return false }
func (*resultTestStreams) CheckAvailable(context.Context, string, string) error { return nil }
func (*resultTestStreams) StoredEPG(string, string) bool                        { return false }
func (s *resultTestStreams) OpenSchedule(_ context.Context, _, channel string) (schedule.Source, error) {
	s.opened = append(s.opened, channel)
	if len(s.opened) == 1 && s.firstError != nil {
		return schedule.Source{}, s.firstError
	}
	return schedule.LiveSource(s.collect), s.openError
}

type failingSuccessStore struct {
	ServiceStore
	err error
}

func (s failingSuccessStore) SetEPGSuccess(context.Context, uint16, uint16, int64) error {
	return s.err
}

// These cases deliberately preserve the different finalization paths: a
// successful candidate, a following iteration with nothing left, and exhaustion.
func TestGatherNetworkFinalization(t *testing.T) {
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	saveErr := errors.New("success status failed")
	for _, tc := range []struct {
		name                                             string
		candidates                                       int
		complete                                         bool
		openErr                                          error
		saveErr                                          error
		option                                           CollectOptions
		wantErr                                          error
		kind                                             string
		observed, remaining, incomplete, warnings, items int
	}{
		{name: "complete", candidates: 2, complete: true, kind: "epg_gather", observed: 1, items: 1},
		{name: "incomplete", candidates: 2, wantErr: ErrIncomplete, kind: "epg_gather", observed: 1, incomplete: 1, warnings: 1, items: 1},
		{name: "planned partial", candidates: 2, option: CollectOptions{UnknownExtendedIdle: time.Second}, wantErr: ErrPartial, kind: "epg_partial", observed: 1, incomplete: 1, warnings: 1, items: 1},
		{name: "status failure then early finish", candidates: 2, complete: true, saveErr: saveErr, wantErr: saveErr, kind: "epg_gather", observed: 1, warnings: 1, items: 1},
		{name: "status failure on last candidate", candidates: 1, complete: true, saveErr: saveErr, wantErr: saveErr, kind: "epg_gather", observed: 1, warnings: 1, items: 1},
		{name: "unavailable", candidates: 2, openErr: tuner.ErrTunerUnavailable, wantErr: ErrDeferred, kind: "epg_gather", remaining: 1, warnings: 2, items: 3},
		{name: "no candidates", wantErr: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				streams := &resultTestStreams{openError: tc.openErr, collect: func(ctx context.Context, update func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
					u := telemetryUpdate(key, tc.complete)
					if tc.option.UnknownExtendedIdle > 0 {
						u.ExtendedObserved = false
					}
					if err := update(u); err != nil {
						return err
					}
					<-ctx.Done()
					return ctx.Err()
				}}
				var status ServiceStore = newRemoteSyncServiceStore()
				if tc.saveErr != nil {
					status = failingSuccessStore{status, tc.saveErr}
				}
				candidates := []Candidate{}
				for i := 0; i < tc.candidates; i++ {
					candidates = append(candidates, Candidate{Type: "BS4K", Channel: string(rune('A' + i))})
				}
				recorder := &collectionResultRecorder{}
				store := &collectProgramStore{}
				err := gatherNetwork(run.WithReporter(t.Context(), recorder), store, store, status, streams, 11, candidates, []model.ServiceKey{key}, 10*time.Millisecond, tc.option)
				if tc.candidates == 0 {
					if err == nil || recorder.result.Kind != "" {
						t.Fatalf("err=%v report=%+v", err, recorder.result)
					}
					return
				}
				if tc.wantErr == nil && err != nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v want %v", err, tc.wantErr)
				}
				report := recorder.result
				counts := map[string]int{"candidates": tc.candidates, "services": 1, "observedServices": tc.observed, "remainingServices": tc.remaining, "incompleteServices": tc.incomplete, "programs": tc.observed}
				if report.Kind != tc.kind || !reflect.DeepEqual(report.Counts, counts) || len(report.Warnings) != tc.warnings || len(report.Items) != tc.items {
					t.Fatalf("report=%+v want kind=%s counts=%v warnings=%d items=%d", report, tc.kind, counts, tc.warnings, tc.items)
				}
				wantOpened := 1
				if tc.openErr != nil {
					wantOpened = tc.candidates
				}
				if len(streams.opened) != wantOpened {
					t.Fatalf("opened=%v", streams.opened)
				}
			})
		})
	}
}

// Keep legacy test setup concise while production uses Gatherer's dependencies.
func gatherNetwork(ctx context.Context, events ScheduleWriter, programs ProgramStore, services ServiceStore, streams StreamManager, networkID uint16, candidates []Candidate, keys []model.ServiceKey, retrievalTime time.Duration, options ...CollectOptions) error {
	var option CollectOptions
	if len(options) > 0 {
		option = options[0]
	}
	g := NewGatherer(events, programs, services, streams, nil, retrievalTime)
	return g.gatherNetwork(ctx, networkID, &Group{Candidates: candidates, Services: keys}, option)
}

func TestGatherNetworkFallbackPreservesEarlierErrorOnlyForPartial(t *testing.T) {
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	firstErr := errors.New("first candidate failed")
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				streams := &resultTestStreams{firstError: firstErr, collect: func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
					u := telemetryUpdate(key, !partial)
					u.ExtendedObserved = !partial
					if err := onSchedule(u); err != nil {
						return err
					}
					<-ctx.Done()
					return ctx.Err()
				}}
				recorder := &collectionResultRecorder{}
				store := &collectProgramStore{}
				err := gatherNetwork(run.WithReporter(t.Context(), recorder), store, store, newRemoteSyncServiceStore(), streams, 11, []Candidate{{Channel: "A"}, {Channel: "B"}}, []model.ServiceKey{key}, 10*time.Millisecond, CollectOptions{UnknownExtendedIdle: time.Second})
				if partial {
					if !errors.Is(err, firstErr) || !errors.Is(err, ErrIncomplete) || errors.Is(err, ErrPartial) {
						t.Fatalf("err=%v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				report := recorder.result
				wantWarnings := 1
				if partial {
					wantWarnings++
				}
				if report.Kind != "epg_gather" || report.Counts["observedServices"] != 1 || len(report.Items) != 2 || len(report.Warnings) != wantWarnings {
					t.Fatalf("report=%+v", report)
				}
			})
		})
	}
}

func TestGatherNetworkCancellationDoesNotReport(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	streams := &resultTestStreams{collect: func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error {
		cancel()
		return context.Canceled
	}}
	recorder := &collectionResultRecorder{}
	store := &collectProgramStore{}
	key := model.ServiceKey{NetworkID: 11, ServiceID: 102}
	err := gatherNetwork(run.WithReporter(ctx, recorder), store, store, newRemoteSyncServiceStore(), streams, 11, []Candidate{{Channel: "A"}}, []model.ServiceKey{key}, time.Second)
	if !errors.Is(err, context.Canceled) || recorder.result.Kind != "" {
		t.Fatalf("err=%v report=%+v", err, recorder.result)
	}
}
