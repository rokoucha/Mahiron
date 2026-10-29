package program

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
	"testing"
)

func TestPresentFollowingUndefinedDurationLifecycle(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	pub := &fakeProgramEventPublisher{}
	m.events = pub
	key := model.ServiceKey{NetworkID: 32736, ServiceID: 1024}
	event := func(id uint16, start int64, duration *int, present bool) model.Event {
		return model.Event{Key: key, EventID: id, StartAt: &start, DurationMS: duration, EITPresent: present, EITFollowing: !present, Name: "ニュース"}
	}
	update := func(events ...model.Event) {
		t.Helper()
		if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: key, Events: events}); err != nil {
			t.Fatal(err)
		}
		m.flushEvents()
	}
	get := func(id uint16) *Program {
		t.Helper()
		p, ok, err := m.Get(ctx, ProgramID(key.NetworkID, key.ServiceID, id))
		if err != nil || !ok {
			t.Fatalf("event %d missing: %v", id, err)
		}
		return p
	}
	a := event(6473, 1000, nil, true)
	update(a)
	if get(6473).DurationMS != nil {
		t.Fatal("undefined end invented")
	}
	// Schedule timing must not replace the more reliable p/f timing.
	stale := a
	stale.DurationMS = testPtr(500)
	stale.Name = "old schedule"
	stale.EITPresent = false
	stale.EITRevision = &model.EITRevision{TableID: 0x50, Reception: 1}
	if err := m.UpsertEvents(ctx, []model.Event{stale}); err != nil {
		t.Fatal(err)
	}
	if p := get(6473); p.DurationMS != nil || p.Name != "ニュース" {
		t.Fatalf("schedule replaced p/f: %+v", p)
	}
	// A program can move to following with its original start time during a cut-in.
	a.EITPresent = false
	a.EITFollowing = true
	b := event(64762, 2000, testPtr(1000), true)
	update(b, a)
	if p := get(6473); !p.EITFollowing || p.EITPresent || p.DurationMS != nil {
		t.Fatalf("suspended event=%+v", p)
	}
	a.EITFollowing = false
	a.EITPresent = true
	update(a)
	if !get(6473).EITPresent {
		t.Fatal("suspended event did not resume")
	}
	if p := get(64762); p.EITPresent || p.EITFollowing {
		t.Fatal("historical event retained active flags")
	}
	// A same-ID confirmation replaces undefined duration without retiring the row.
	a.DurationMS = testPtr(3000)
	update(a)
	if get(6473).DurationOrZero() != 3000 {
		t.Fatal("duration confirmation lost")
	}
	a.DurationMS = nil
	update(a)
	pub.events = nil
	update(b)
	if _, ok, err := m.Get(ctx, ProgramID(key.NetworkID, key.ServiceID, 6473)); err != nil || ok {
		t.Fatalf("ended undefined event retained: found=%v err=%v", ok, err)
	}
	removed := false
	for _, e := range pub.events {
		if e.isRemove && e.removeID == ProgramID(key.NetworkID, key.ServiceID, 6473) {
			removed = true
		}
	}
	if !removed {
		t.Fatal("no remove notification")
	}
	// Empty p/f retires active flags but retains known historical timing.
	update()
	if p := get(64762); p.EITPresent || p.EITFollowing || p.DurationOrZero() != 1000 {
		t.Fatalf("history=%+v", p)
	}
}

func TestPresentFollowingRetirementIsAtomic(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	key := model.ServiceKey{NetworkID: 1, ServiceID: 2}
	old := model.Event{Key: key, EventID: 1, StartAt: testPtr[int64](100), EITPresent: true}
	if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: key, Events: []model.Event{old}}); err != nil {
		t.Fatal(err)
	}
	store := m.store.(*sqliteStore)
	if _, err := store.write.ExecContext(ctx, `CREATE TRIGGER reject_pf BEFORE INSERT ON programs BEGIN SELECT RAISE(ABORT, 'test failure'); END`); err != nil {
		t.Fatal(err)
	}
	next := old
	next.EventID = 2
	if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: key, Events: []model.Event{next}}); err == nil {
		t.Fatal("write unexpectedly succeeded")
	}
	if _, ok, err := m.Get(ctx, ProgramID(1, 2, 1)); err != nil || !ok {
		t.Fatalf("old event lost: %v", err)
	}
}

func TestPresentFollowingSuspensionAndUnstartedFollowing(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	key := model.ServiceKey{NetworkID: 1, ServiceID: 2}
	a := model.Event{Key: key, EventID: 1, StartAt: testPtr[int64](100), DurationMS: testPtr(1000), EITPresent: true}
	b := a
	b.EventID = 2
	b.StartAt = testPtr[int64](200)
	b.DurationMS = testPtr(100)
	update := func(events ...model.Event) {
		t.Helper()
		if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: key, Events: events}); err != nil {
			t.Fatal(err)
		}
	}
	update(a)
	a.EITPresent = false
	a.EITFollowing = true
	update(b, a)
	if p, ok, err := m.Get(ctx, ProgramID(1, 2, 1)); err != nil || !ok || !p.EITSuspended {
		t.Fatalf("known-duration suspended event lost: %+v %v", p, err)
	}
	a.DurationMS = nil
	update(b, a)
	// Suspension survives serialization, and disappearance now ends the event.
	update(b)
	if _, ok, err := m.Get(ctx, ProgramID(1, 2, 1)); err != nil || ok {
		t.Fatalf("ended suspended event retained: %v", err)
	}
	// Disappearance of a future following event alone does not prove cancellation.
	next := a
	next.EventID = 3
	next.StartAt = testPtr[int64](1000)
	update(b, next)
	update(b)
	if p, ok, err := m.Get(ctx, ProgramID(1, 2, 3)); err != nil || !ok || p.EITFollowing || p.EITSuspended {
		t.Fatalf("unstarted following incorrectly cancelled: %+v %v", p, err)
	}
}

func TestEmptyPresentFollowingRetiresOnlyItsService(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t)
	a := model.ServiceKey{NetworkID: 1, ServiceID: 2}
	b := a
	b.NetworkID = 2
	for _, key := range []model.ServiceKey{a, b} {
		e := model.Event{Key: key, EventID: 1, StartAt: testPtr[int64](100), EITPresent: true}
		if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: key, Events: []model.Event{e}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.UpsertPresentFollowing(ctx, model.PresentFollowingSnapshot{Service: a}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(ctx, ProgramID(1, 2, 1)); err != nil || ok {
		t.Fatalf("empty p/f left an undefined present: %v", err)
	}
	if _, ok, err := m.Get(ctx, ProgramID(2, 2, 1)); err != nil || !ok {
		t.Fatalf("another service was changed: %v", err)
	}
}
