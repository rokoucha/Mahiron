package job

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestSharedJobsOverlapAndExcludeWriter(t *testing.T) {
	mgr, err := NewManager(Config{MaxConcurrentJobs: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mgr.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	started := make(chan string, 4)
	enqueue := func(key string, shared bool) string {
		def := JobDefinition{Key: key, Handler: func(ctx context.Context) error { started <- key; <-ctx.Done(); return ctx.Err() }}
		if shared {
			def.SharedKeys = []string{"topology"}
		} else {
			def.ExclusiveKeys = []string{"topology"}
		}
		id, err := mgr.EnqueueDefinition(def)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	next := func(want string) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("started %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not start", want)
		}
	}
	first := enqueue("first-reader", true)
	next("first-reader")
	second := enqueue("second-reader", true)
	next("second-reader")
	writer := enqueue("writer", false)
	if err := mgr.Abort(first); err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, first)
	for _, item := range mgr.GetJobs() {
		if item.ID == writer && item.Status != StatusQueued {
			t.Fatalf("writer started while a reader held the key: %s", item.Status)
		}
	}
	if err := mgr.Abort(second); err != nil {
		t.Fatal(err)
	}
	next("writer")
	third := enqueue("third-reader", true)
	for _, item := range mgr.GetJobs() {
		if item.ID == third && item.Status != StatusQueued {
			t.Fatalf("reader started while writer held the key: %s", item.Status)
		}
	}
	if err := mgr.Abort(writer); err != nil {
		t.Fatal(err)
	}
	next("third-reader")
}

func TestQueuePriorityKeepsFIFOForTies(t *testing.T) {
	mgr := newTestManager(t)
	t.Cleanup(func() {
		if err := mgr.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	started := make(chan string, 4)
	blocker, err := mgr.EnqueueDefinition(JobDefinition{Key: "blocker", Handler: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, def := range []JobDefinition{{Key: "newer", Priority: -20}, {Key: "older", Priority: -10}, {Key: "same-age", Priority: -20}, {Key: "unseen"}} {
		def.Handler = func(context.Context) error { started <- def.Key; return nil }
		id, err := mgr.EnqueueDefinition(def)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := mgr.Abort(blocker); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		waitJob(t, mgr, id)
	}
	var got []string
	for range ids {
		got = append(got, <-started)
	}
	if diff := cmp.Diff([]string{"unseen", "older", "newer", "same-age"}, got); diff != "" {
		t.Fatal(diff)
	}
}

func TestQueuedWriterBlocksNewReadersButNotItsDependencies(t *testing.T) {
	mgr, err := NewManager(Config{MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	started := make(chan string, 5)
	handler := func(key string) func(context.Context) error {
		return func(ctx context.Context) error { started <- key; <-ctx.Done(); return ctx.Err() }
	}
	reader, err := mgr.EnqueueDefinition(JobDefinition{Key: "reader", SharedKeys: []string{"topology"}, Handler: handler("reader")})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	writer, err := mgr.EnqueueDefinition(JobDefinition{Key: "writer", ExclusiveKeys: []string{"topology"}, Handler: handler("writer")})
	if err != nil {
		t.Fatal(err)
	}
	late, err := mgr.EnqueueDefinition(JobDefinition{Key: "late", SharedKeys: []string{"topology"}, Handler: handler("late")})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range mgr.GetJobs() {
		if item.ID == late && item.Status != StatusQueued {
			t.Fatal("reader overtook waiting writer")
		}
	}
	_ = mgr.Abort(reader)
	select {
	case got := <-started:
		if got != "writer" {
			t.Fatalf("started %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("writer starved")
	}
	_ = mgr.Abort(writer)
	select {
	case got := <-started:
		if got != "late" {
			t.Fatalf("started %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not resume")
	}
	_ = mgr.Abort(late)
	waitJob(t, mgr, late)
	// An unresolved writer cannot block its own shared-key dependency.
	mgr.mu.Lock()
	dependency := &Job{Key: "dep", definition: &JobDefinition{SharedKeys: []string{"topology"}}}
	waiting := &Job{Key: "waiting", definition: &JobDefinition{ExclusiveKeys: []string{"topology"}, DependsOn: []string{"dep"}}}
	mgr.activeKeys["dep"] = true
	mgr.queue = append(mgr.queue, waiting)
	blocked := mgr.blockedByWriterLocked(dependency)
	mgr.queue = nil
	delete(mgr.activeKeys, "dep")
	mgr.mu.Unlock()
	if blocked {
		t.Fatal("writer blocked its dependency")
	}
}

func TestOnlyIndependentGroupsBypassSharedConcurrencyLimit(t *testing.T) {
	mgr, err := NewManager(Config{MaxConcurrentJobs: 1, GroupLimits: map[string]int{"live": 1, "api": 1}, IndependentGroups: map[string]bool{"api": true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	started := make(chan string, 3)
	handler := func(key string) func(context.Context) error {
		return func(ctx context.Context) error { started <- key; <-ctx.Done(); return ctx.Err() }
	}
	ordinary, err := mgr.EnqueueDefinition(JobDefinition{Key: "ordinary", Handler: handler("ordinary")})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	live, err := mgr.EnqueueDefinition(JobDefinition{Key: "live", ConcurrencyGroup: "live", Handler: handler("live")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.EnqueueDefinition(JobDefinition{Key: "api", ConcurrencyGroup: "api", Handler: handler("api")})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-started:
		if got != "api" {
			t.Fatalf("shared limit bypassed by %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("independent API blocked")
	}
	for _, item := range mgr.GetJobs() {
		if item.ID == live && item.Status != StatusQueued {
			t.Fatal("live bypassed common limit")
		}
	}
	_ = mgr.Abort(ordinary)
	select {
	case got := <-started:
		if got != "live" {
			t.Fatalf("started %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("live did not resume")
	}
}

func TestUnconfiguredConcurrencyGroupIsRejected(t *testing.T) {
	mgr, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	_, err = mgr.EnqueueDefinition(JobDefinition{Key: "unknown", ConcurrencyGroup: "unknown", Handler: func(context.Context) error { return nil }})
	if err != ErrInvalidDefinition {
		t.Fatalf("unknown group queued forever: %v", err)
	}
}
