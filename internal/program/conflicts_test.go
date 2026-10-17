package program

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/model"
)

func TestUpsertRemovesConflicts(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		start                          int64
		duration                       int
		present, following, newPresent bool
		removed                        bool
	}{
		{"replacement", 100, 100, false, false, false, true},
		{"partial overlap", 150, 100, false, false, false, true},
		{"contains", 50, 300, false, false, false, true},
		{"adjacent before", 0, 100, false, false, false, false},
		{"adjacent after", 200, 100, false, false, false, false},
		{"unknown duration", 100, 0, false, false, false, false},
		{"protect present", 100, 100, true, false, false, false},
		{"protect following", 100, 100, false, true, false, false},
		{"present replaces following", 100, 100, false, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := newTestManager(t)
			pub := &fakeProgramEventPublisher{}
			m.events = pub
			old := FromEvent(model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](100), DurationMS: testPtr(100), EITPresent: tc.present, EITFollowing: tc.following})
			if err := m.UpsertPrograms(ctx, []*Program{old}); err != nil {
				t.Fatal(err)
			}
			m.flushEvents()
			pub.events = nil
			next := FromEvent(model.Event{Key: old.Key, EventID: 2, StartAt: &tc.start, DurationMS: &tc.duration, EITPresent: tc.newPresent})
			if err := m.UpsertPrograms(ctx, []*Program{next}); err != nil {
				t.Fatal(err)
			}
			m.flushEvents()
			_, found, err := m.Get(ctx, old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if found == tc.removed {
				t.Fatalf("old found=%v, removed=%v", found, tc.removed)
			}
			removed := false
			for _, e := range pub.events {
				if e.isRemove && e.removeID == old.ID {
					removed = true
				}
			}
			if removed != tc.removed {
				t.Fatalf("remove event=%v", removed)
			}
		})
	}
}

func TestUpsertConflictBatchAndTimeChange(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	makeProgram := func(id uint16, start int64) *Program {
		return FromEvent(model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: id, StartAt: &start, DurationMS: testPtr(100)})
	}
	a, b := makeProgram(1, 100), makeProgram(2, 100)
	if err := m.UpsertPrograms(ctx, []*Program{a, b}); err != nil {
		t.Fatal(err)
	}
	list, err := m.List(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != b.ID {
		t.Fatalf("batch result=%v", list)
	}
	a = makeProgram(1, 300)
	if err := m.UpsertPrograms(ctx, []*Program{a}); err != nil {
		t.Fatal(err)
	}
	b = makeProgram(2, 300)
	if err := m.UpsertPrograms(ctx, []*Program{b}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := m.Get(ctx, a.ID); err != nil || found {
		t.Fatalf("time change left conflict: found=%v err=%v", found, err)
	}
}

func TestConflictRemovalRollsBackWithFailedWrite(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := NewSQLiteStore(database)
	m := NewManager(store)
	old := FromEvent(model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](100), DurationMS: testPtr(100)})
	if err := m.UpsertPrograms(ctx, []*Program{old}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Write.ExecContext(ctx, `CREATE TRIGGER reject_program BEFORE INSERT ON programs BEGIN SELECT RAISE(ABORT, 'test failure'); END`); err != nil {
		t.Fatal(err)
	}
	replacement := *old
	replacement.ID = ProgramID(1, 2, 2)
	replacement.EventID = 2
	if err := m.UpsertPrograms(ctx, []*Program{&replacement}); err == nil {
		t.Fatal("write unexpectedly succeeded")
	}
	if _, found, err := m.Get(ctx, old.ID); err != nil || !found {
		t.Fatalf("old program lost: found=%v err=%v", found, err)
	}
	if _, found, err := m.Get(ctx, replacement.ID); err != nil || found {
		t.Fatalf("replacement stored: found=%v err=%v", found, err)
	}
}

func TestConflictsStayWithinService(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	var programs []*Program
	for _, key := range []model.ServiceKey{{NetworkID: 1, ServiceID: 2}, {NetworkID: 2, ServiceID: 2}, {NetworkID: 1, ServiceID: 3}} {
		programs = append(programs, FromEvent(model.Event{Key: key, EventID: 1, StartAt: testPtr[int64](100), DurationMS: testPtr(100)}))
	}
	if err := m.UpsertPrograms(ctx, programs); err != nil {
		t.Fatal(err)
	}
	replacement := *programs[0]
	replacement.ID = ProgramID(1, 2, 2)
	replacement.EventID = 2
	if err := m.UpsertPrograms(ctx, []*Program{&replacement}); err != nil {
		t.Fatal(err)
	}
	list, err := m.List(ctx, Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("program count=%d, want 3", len(list))
	}
}
