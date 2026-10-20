package defs

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/tuner"
)

const (
	EPGLiveGroup   = "epg-live"
	EPGStoredGroup = "epg-stored"
)

// EPGController owns deadlines and retries; each acquisition remains a finite job.
type EPGController struct {
	EPGGatherer
	jobs            *job.Manager
	interval        time.Duration
	liveLimit       int
	refresh         chan struct{}
	forceAll        bool
	cleaner         ProgramCleaner
	retentionDays   int
	nextCleanup     time.Time
	resourceChanges func() <-chan struct{}
}

type epgDeadline struct {
	id          string
	next        time.Time
	failures    int
	forced      bool
	lastFailure int64
	lastSuccess int64
	deferred    bool
	deferrals   int
}

func NewEPGController(jobs *job.Manager, service EPGGatherer, interval time.Duration, liveLimit int) *EPGController {
	return &EPGController{EPGGatherer: service, jobs: jobs, interval: interval, liveLimit: liveLimit, refresh: make(chan struct{}, 1)}
}

func (c *EPGController) Refresh() {
	select {
	case c.refresh <- struct{}{}:
	default:
	}
}

func (c *EPGController) Run(ctx context.Context) {
	states := make(map[epggather.GroupKey]*epgDeadline)
	timer := time.NewTimer(0)
	defer timer.Stop()
	changed := c.jobs.Changes()
	var resources <-chan struct{}
	if c.resourceChanges != nil {
		resources = c.resourceChanges()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.refresh:
			c.forceAll = true
			for _, state := range states {
				state.forced = true
				state.next = time.Time{}
			}
		case <-changed:
		case <-resources:
			for _, state := range states {
				if state.deferred {
					state.deferrals = 0
					state.next = time.Time{}
				}
			}
		case <-timer.C:
		}
		changed = c.jobs.Changes()
		if c.resourceChanges != nil {
			resources = c.resourceChanges()
		}
		delay := c.reconcile(ctx, states, time.Now())
		timer.Stop()
		timer.Reset(delay)
	}
}

func (c *EPGController) reconcile(ctx context.Context, states map[epggather.GroupKey]*epgDeadline, now time.Time) time.Duration {
	// This also recovers missed external tuner/service notifications without
	// polling idle controllers at the refresh frequency.
	next := now.Add(time.Minute)
	if c.cleaner != nil && !c.nextCleanup.After(now) {
		_, err := c.jobs.EnqueueDefinition(job.JobDefinition{Key: "epg-cleanup", Name: "EPG Cleanup", Handler: func(ctx context.Context) error { return c.cleaner.DeleteExpired(ctx, time.Now(), c.retentionDays) }})
		if err == nil {
			c.nextCleanup = now.Add(time.Hour)
		}
	}
	// Read executions before their persisted success times so a just-finished
	// job cannot be paired with an older service snapshot and queued again.
	jobs := make(map[string]*job.Job)
	for _, item := range c.jobs.GetJobs() {
		jobs[item.ID] = item
	}
	groups, err := c.Groups(ctx)
	if err != nil {
		slog.Debug("EPG reconciliation postponed", "err", err)
		return time.Minute
	}
	live, stored := 0, 0
	for key, state := range states {
		if state.id != "" {
			item := jobs[state.id]
			if item != nil && item.Status != job.StatusFinished {
				if key.Stored {
					stored++
				} else {
					live++
				}
				continue
			}
			state.id = ""
			if item != nil && item.Result != nil && item.Result.Kind == "epg_deferred" {
				state.next = now.Add(state.deferDelay())
				state.deferred = true
				state.failures = 0
			} else if item == nil || item.HasFailed || item.HasAborted {
				state.deferred = false
				state.deferrals = 0
				state.failures++
				delay := min(time.Minute<<min(state.failures-1, 20), max(c.interval, time.Minute))
				if item != nil && item.HasAborted {
					delay = c.interval
				}
				state.next = now.Add(delay)
			} else {
				state.failures = 0
				state.deferred = false
				state.deferrals = 0
				state.next = time.Time{}
			}
		}
		if groups[key] == nil {
			delete(states, key)
		}
	}
	keys := make([]epggather.GroupKey, 0, len(groups))
	deadlines := make(map[epggather.GroupKey]time.Time, len(groups))
	for key, group := range groups {
		state := states[key]
		if state == nil {
			state = &epgDeadline{lastSuccess: group.OldestSuccessAt}
			states[key] = state
			if group.LastFailureAt > 0 {
				state.next = time.UnixMilli(group.LastFailureAt).Add(time.Minute)
			}
		}
		if state.id == "" {
			if group.OldestSuccessAt < state.lastSuccess {
				state.next = time.Time{}
				state.failures = 0
			}
			if group.LastFailureAt > state.lastFailure {
				state.failures = max(1, state.failures)
				state.next = maxTime(state.next, time.UnixMilli(group.LastFailureAt).Add(time.Minute))
			}
		}
		state.lastFailure = group.LastFailureAt
		state.lastSuccess = group.OldestSuccessAt
		if c.forceAll {
			state.forced = true
			state.next = time.Time{}
		}
		due := time.UnixMilli(group.OldestSuccessAt).Add(c.interval)
		if state.forced || state.failures > 0 || state.deferred {
			due = time.Time{}
		}
		deadlines[key] = maxTime(due, state.next)
		keys = append(keys, key)
	}
	// Retry deadlines must compete with freshness deadlines; an old success
	// alone would let repeatedly incomplete streams starve overdue peers.
	slices.SortFunc(keys, func(a, b epggather.GroupKey) int {
		if order := deadlines[a].Compare(deadlines[b]); order != 0 {
			return order
		}
		return compareGroupKeys(a, b)
	})
	for _, key := range keys {
		state, group := states[key], groups[key]
		if state.id != "" {
			continue
		}
		due := deadlines[key]
		if due.After(now) {
			if due.Before(next) {
				next = due
			}
			continue
		}
		if key.Stored && stored >= 1 || !key.Stored && live >= c.liveLimit {
			continue
		}
		if checker, ok := c.EPGGatherer.(interface {
			CheckGroup(context.Context, epggather.GroupKey) error
		}); ok {
			if err := checker.CheckGroup(ctx, key); err != nil {
				delay := time.Minute
				if errors.Is(err, tuner.ErrTunerUnavailable) {
					state.deferred = true
					delay = state.deferDelay()
				}
				state.next = now.Add(delay)
				if state.next.Before(next) {
					next = state.next
				}
				continue
			}
		}
		def := epgGatherDefinition(c.EPGGatherer, key, group)
		handler := def.Handler
		def.Handler = func(ctx context.Context) error {
			err := handler(ctx)
			if errors.Is(err, epggather.ErrDeferred) {
				run.Set(ctx, run.Result{Kind: "epg_deferred", Summary: "waiting for an available tuner"})
				return nil
			}
			return err
		}
		def.RetryDelays = nil
		def.RetryIf = nil
		if key.Stored {
			def.ConcurrencyGroup = EPGStoredGroup
		} else {
			def.ConcurrencyGroup = EPGLiveGroup
		}
		id, err := c.jobs.EnqueueDefinition(def)
		if err != nil {
			continue
		}
		state.id = id
		state.deferred = false
		state.forced = false
		if key.Stored {
			stored++
		} else {
			live++
		}
	}
	c.forceAll = false
	return max(time.Millisecond, time.Until(next))
}

func (c *EPGController) SetCleaner(cleaner ProgramCleaner, retentionDays int) {
	c.cleaner = cleaner
	c.retentionDays = retentionDays
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (c *EPGController) SetResourceChanges(changes func() <-chan struct{}) {
	c.resourceChanges = changes
}

func (s *epgDeadline) deferDelay() time.Duration {
	s.deferrals++
	return min(5*time.Second<<min(s.deferrals-1, 4), time.Minute)
}
