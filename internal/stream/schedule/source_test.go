package schedule

import (
	"context"
	"errors"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
)

func TestSourceRejectsUnsupportedOperations(t *testing.T) {
	live := LiveSource(func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error {
		return nil
	})
	stored := StoredSource(func(context.Context, uint16, uint16) ([]model.Event, error) { return nil, nil })
	for _, s := range []Source{{}, LiveSource(nil), StoredSource(nil), stored} {
		if err := s.Collect(t.Context(), nil, nil); err == nil {
			t.Fatal("unsupported collection succeeded")
		}
	}
	for _, s := range []Source{{}, LiveSource(nil), StoredSource(nil), live} {
		if _, err := s.List(t.Context(), 1, 2); err == nil {
			t.Fatal("unsupported listing succeeded")
		}
	}
}

func TestSourceForwardsCallbacksAndErrors(t *testing.T) {
	want := errors.New("callback failure")
	live := LiveSource(func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, onPF func(model.PresentFollowing) error) error {
		if ctx != t.Context() {
			t.Fatal("context changed")
		}
		if err := onSchedule(model.ScheduleUpdate{}); err != nil {
			return err
		}
		return onPF(model.PresentFollowing{})
	})
	called := false
	err := live.Collect(t.Context(), func(model.ScheduleUpdate) error { called = true; return nil }, func(model.PresentFollowing) error { return want })
	if !called || !errors.Is(err, want) || live.IsStored() {
		t.Fatalf("called=%v stored=%v err=%v", called, live.IsStored(), err)
	}
	stored := StoredSource(func(ctx context.Context, nid, sid uint16) ([]model.Event, error) {
		if ctx != t.Context() || nid != 4 || sid != 101 {
			t.Fatal("listing arguments changed")
		}
		return nil, want
	})
	if _, err := stored.List(t.Context(), 4, 101); !stored.IsStored() || !errors.Is(err, want) {
		t.Fatalf("stored=%v err=%v", stored.IsStored(), err)
	}
}
