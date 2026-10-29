package program

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
)

func TestRemoteSnapshotRemovesUnknownBeforeWindowAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	pub := &fakeProgramEventPublisher{}
	m.events = pub
	key := model.ServiceKey{NetworkID: 1, ServiceID: 2}
	makeEvent := func(id uint16, start int64, duration *int) *Program {
		return FromEvent(model.Event{Key: key, EventID: id, StartAt: &start, DurationMS: duration})
	}
	old := makeEvent(1, 100, nil)
	history := makeEvent(2, 100, testPtr(10))
	overlap := makeEvent(3, 150, testPtr(100))
	other := makeEvent(4, 100, nil)
	other.Key.NetworkID = 2
	other.ID = ProgramID(2, 2, 4)
	// Seed the store without overlap reconciliation: these are pre-existing rows.
	if err := m.store.UpsertAll(ctx, []*Program{old, history, overlap, other}); err != nil {
		t.Fatal(err)
	}
	next := makeEvent(5, 200, testPtr(100))
	if err := m.ReplaceServicePrograms(ctx, 1, 2, 200, []*Program{next}); err != nil {
		t.Fatal(err)
	}
	m.flushEvents()
	for _, p := range []*Program{old, overlap} {
		if _, ok, err := m.Get(ctx, p.ID); err != nil || ok {
			t.Fatalf("stale %d retained: %v", p.ID, err)
		}
	}
	for _, p := range []*Program{history, other, next} {
		if _, ok, err := m.Get(ctx, p.ID); err != nil || !ok {
			t.Fatalf("expected %d missing: %v", p.ID, err)
		}
	}
	removed := map[int64]bool{}
	for _, e := range pub.events {
		if e.isRemove {
			removed[e.removeID] = true
		}
	}
	if !removed[old.ID] || !removed[overlap.ID] || len(removed) != 2 {
		t.Fatalf("remove notifications=%v", removed)
	}
}

func TestRemoteRemovalFailureAndDuplicateNotification(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	pub := &fakeProgramEventPublisher{}
	m.events = pub
	p := FromEvent(model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1})
	if err := m.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}
	m.flushEvents()
	pub.events = nil
	db := m.store.(*sqliteStore).write
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_remove BEFORE DELETE ON programs BEGIN SELECT RAISE(ABORT, 'test failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := m.RemovePrograms(ctx, []int64{p.ID}); err == nil {
		t.Fatal("delete unexpectedly succeeded")
	}
	m.flushEvents()
	if len(pub.events) != 0 {
		t.Fatal("published failed removal")
	}
	if _, ok, err := m.Get(ctx, p.ID); err != nil || !ok {
		t.Fatal("failed removal lost row")
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_remove`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := m.RemovePrograms(ctx, []int64{p.ID}); err != nil {
			t.Fatal(err)
		}
	}
	m.flushEvents()
	if len(pub.events) != 1 || !pub.events[0].isRemove {
		t.Fatalf("notifications=%+v", pub.events)
	}
}
