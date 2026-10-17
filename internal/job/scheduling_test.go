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
