package defs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type availableEPG struct {
	*scheduledEPG
	unavailable map[epggather.GroupKey]bool
}

func (s *availableEPG) CheckGroup(_ context.Context, key epggather.GroupKey) error {
	if s.unavailable[key] {
		return tuner.ErrTunerUnavailable
	}
	return nil
}

func controllerManager(t *testing.T) *job.Manager {
	t.Helper()
	mgr, err := job.NewManager(job.Config{MaxConcurrentJobs: 1, GroupLimits: map[string]int{EPGLiveGroup: 1, EPGStoredGroup: 1}, IndependentGroups: map[string]bool{EPGStoredGroup: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	return mgr
}

func TestContinuousEPGSkipsUnavailableAndRunsStoredIndependently(t *testing.T) {
	mgr := controllerManager(t)
	unavailable := epggather.GroupKey{NetworkID: 1, StreamID: 1}
	live := epggather.GroupKey{NetworkID: 2, StreamID: 2}
	stored := epggather.GroupKey{NetworkID: 3, Stored: true}
	started := make(chan epggather.GroupKey, 2)
	service := &availableEPG{scheduledEPG: &scheduledEPG{groups: map[epggather.GroupKey]*epggather.Group{}, gather: func(ctx context.Context, key epggather.GroupKey) error {
		started <- key
		<-ctx.Done()
		return ctx.Err()
	}}, unavailable: map[epggather.GroupKey]bool{unavailable: true}}
	for _, key := range []epggather.GroupKey{unavailable, live, stored} {
		service.groups[key] = &epggather.Group{Services: []model.ServiceKey{{NetworkID: key.NetworkID, ServiceID: 1}}}
	}
	controller := NewEPGController(mgr, service, 30*time.Minute, 1)
	states := map[epggather.GroupKey]*epgDeadline{}
	controller.reconcile(t.Context(), states, time.Now())
	seen := map[epggather.GroupKey]bool{}
	for range 2 {
		select {
		case key := <-started:
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatal("live/stored blocked each other")
		}
	}
	if !seen[live] || !seen[stored] {
		t.Fatalf("started=%v", seen)
	}
	if len(mgr.GetJobs()) != 2 || states[unavailable].id != "" {
		t.Fatal("unavailable target occupied a job")
	}
}

func TestContinuousEPGDeadlineAndPartialBackoff(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			mgr := controllerManager(t)
			key := epggather.GroupKey{NetworkID: 4, Stored: true}
			now := time.Now()
			service := &scheduledEPG{groups: map[epggather.GroupKey]*epggather.Group{key: {OldestSuccessAt: now.Add(-time.Hour).UnixMilli(), Services: []model.ServiceKey{{NetworkID: 4, ServiceID: 1}}}}, gather: func(context.Context, epggather.GroupKey) error {
				if partial {
					return epggather.ErrIncomplete
				}
				return nil
			}}
			c := NewEPGController(mgr, service, 30*time.Minute, 1)
			states := map[epggather.GroupKey]*epgDeadline{}
			c.reconcile(t.Context(), states, now)
			id := states[key].id
			if id == "" {
				t.Fatal("due target not queued")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			item, err := mgr.Wait(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if item.HasFailed != partial {
				t.Fatalf("partial=%v result=%+v", partial, item)
			}
			if !partial {
				service.groups[key].OldestSuccessAt = item.FinishedAt.UnixMilli()
			}
			c.reconcile(t.Context(), states, now.Add(time.Second))
			want := time.UnixMilli(service.groups[key].OldestSuccessAt).Add(30 * time.Minute)
			if partial {
				want = now.Add(time.Second + time.Minute)
			}
			if partial && !states[key].next.Equal(want) {
				t.Fatalf("deadline=%v want %v", states[key].next, want)
			}
			c.reconcile(t.Context(), states, want.Add(-time.Millisecond))
			if states[key].id != "" {
				t.Fatal("retried before deadline")
			}
			c.reconcile(t.Context(), states, want)
			if states[key].id == "" {
				t.Fatal("deadline did not dispatch")
			}
		})
	}
}

func TestContinuousEPGRechecksImmediatelyAfterCompletion(t *testing.T) {
	mgr := controllerManager(t)
	service := &completingEPG{groups: map[epggather.GroupKey]*epggather.Group{}, started: make(chan struct{}, 2)}
	for _, nid := range []uint16{1, 2} {
		service.groups[epggather.GroupKey{NetworkID: nid, Stored: true}] = &epggather.Group{Services: []model.ServiceKey{{NetworkID: nid, ServiceID: 1}}}
	}
	started := service.started
	c := NewEPGController(mgr, service, time.Hour, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("completion notification missed")
		}
	}
	cancel()
	<-done
}

func TestContinuousEPGDefersAcquisitionRaceWithoutFailure(t *testing.T) {
	mgr := controllerManager(t)
	key := epggather.GroupKey{NetworkID: 4, Stored: true}
	service := &scheduledEPG{groups: map[epggather.GroupKey]*epggather.Group{key: {Services: []model.ServiceKey{{NetworkID: 4, ServiceID: 1}}}}, gather: func(context.Context, epggather.GroupKey) error {
		return errors.Join(epggather.ErrDeferred, tuner.ErrTunerUnavailable)
	}}
	c := NewEPGController(mgr, service, time.Hour, 1)
	states := map[epggather.GroupKey]*epgDeadline{}
	now := time.Now()
	c.reconcile(t.Context(), states, now)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := mgr.Wait(ctx, states[key].id)
	if err != nil {
		t.Fatal(err)
	}
	if result.HasFailed || result.Result == nil || result.Result.Kind != "epg_deferred" {
		t.Fatalf("result=%+v", result)
	}
	c.reconcile(t.Context(), states, now)
	if !states[key].next.Equal(now.Add(5 * time.Second)) {
		t.Fatal("unavailable tuner got failure backoff")
	}
}

func TestDeferredBackoffIsBounded(t *testing.T) {
	state := &epgDeadline{}
	for _, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute} {
		if got := state.deferDelay(); got != want {
			t.Fatalf("delay=%s want %s", got, want)
		}
	}
}

func TestIncompleteScheduledGatherIsNotRetried(t *testing.T) {
	if epggather.RetryableError(epggather.ErrIncomplete) {
		t.Fatal("partial acquisition retries inside cron batch")
	}
	if !epggather.RetryableError(errors.New("receive failed")) {
		t.Fatal("actual failure cannot retry")
	}
}

type completingEPG struct {
	mu      sync.Mutex
	groups  map[epggather.GroupKey]*epggather.Group
	started chan struct{}
}

func (s *completingEPG) Groups(context.Context) (map[epggather.GroupKey]*epggather.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make(map[epggather.GroupKey]*epggather.Group)
	for key, value := range s.groups {
		copy := *value
		groups[key] = &copy
	}
	return groups, nil
}
func (s *completingEPG) GatherGroup(ctx context.Context, key epggather.GroupKey) error {
	s.mu.Lock()
	s.groups[key].OldestSuccessAt = time.Now().UnixMilli()
	s.mu.Unlock()
	select {
	case s.started <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func TestContinuousEPGRetryDoesNotStarveEarlierFreshnessDeadline(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(map[bool]string{false: "running controller", true: "restart"}[persisted], func(t *testing.T) {
			mgr := controllerManager(t)
			now := time.Now()
			retry := epggather.GroupKey{NetworkID: 1, StreamID: 1}
			overdue := epggather.GroupKey{NetworkID: 2, StreamID: 2}
			started := make(chan epggather.GroupKey, 1)
			service := &scheduledEPG{groups: map[epggather.GroupKey]*epggather.Group{
				retry:   {OldestSuccessAt: now.Add(-2 * time.Hour).UnixMilli()},
				overdue: {OldestSuccessAt: now.Add(-40 * time.Minute).UnixMilli()},
			}, gather: func(ctx context.Context, key epggather.GroupKey) error {
				started <- key
				<-ctx.Done()
				return ctx.Err()
			}}
			states := map[epggather.GroupKey]*epgDeadline{}
			if persisted {
				service.groups[retry].LastFailureAt = now.Add(-2 * time.Minute).UnixMilli()
			} else {
				states[retry] = &epgDeadline{failures: 2, next: now.Add(-time.Minute)}
			}
			c := NewEPGController(mgr, service, 30*time.Minute, 1)
			c.reconcile(t.Context(), states, now)
			select {
			case key := <-started:
				if key != overdue {
					t.Fatalf("started %v before earlier freshness deadline %v", key, overdue)
				}
			case <-time.After(time.Second):
				t.Fatal("overdue stream not started")
			}
		})
	}
}
