package program

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
)

type publishedProgramEvent struct {
	typ      string
	program  *Program
	removeID int64
	isRemove bool
}

type fakeProgramEventPublisher struct {
	events []publishedProgramEvent
}

func (p *fakeProgramEventPublisher) PublishProgramEvent(typ string, program *Program) {
	p.events = append(p.events, publishedProgramEvent{typ: typ, program: program})
}

func (p *fakeProgramEventPublisher) PublishProgramRemove(typ string, id int64) {
	p.events = append(p.events, publishedProgramEvent{typ: typ, removeID: id, isRemove: true})
}

func TestProgramManagerPublishesCreateUpdateAndRemoveEvents(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	publisher := &fakeProgramEventPublisher{}
	manager := NewManager(NewSQLiteStore(database), publisher)

	p := &Program{ID: ProgramID(1, 101, 1), NetworkID: 1, ServiceID: 101, EventID: 1, Name: "first"}
	if err := manager.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	p.Name = "updated"
	if err := manager.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	if err := manager.ReplaceServicePrograms(ctx, 1, 101, 0, nil); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	events := publisher.events
	if got, want := len(events), 3; got != want {
		t.Fatalf("events length = %d, want %d: %#v", got, want, events)
	}
	if events[0].typ != eventTypeCreate || events[1].typ != eventTypeUpdate || events[2].typ != eventTypeRemove {
		t.Fatalf("event types = %s/%s/%s", events[0].typ, events[1].typ, events[2].typ)
	}
	if !events[2].isRemove || events[2].removeID != p.ID {
		t.Fatalf("remove payload = %#v, want id %d", events[2], p.ID)
	}
}

func TestProgramManagerPublishesMergedSparseUpdateEvent(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	publisher := &fakeProgramEventPublisher{}
	manager := NewManager(NewSQLiteStore(database), publisher)

	id := ProgramID(1, 101, 1)
	if err := manager.UpsertPrograms(ctx, []*Program{{
		ID:          id,
		NetworkID:   1,
		ServiceID:   101,
		EventID:     1,
		StartAt:     1000,
		Duration:    1000,
		Name:        "existing title",
		Description: "existing description",
		Genres:      []Genre{{Lv1: 0, Lv2: 1}},
	}}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	if err := manager.UpsertPrograms(ctx, []*Program{{
		ID:        id,
		NetworkID: 1,
		ServiceID: 101,
		EventID:   1,
		StartAt:   2000,
		Duration:  2000,
	}}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	events := publisher.events
	if got, want := len(events), 2; got != want {
		t.Fatalf("events length = %d, want %d: %#v", got, want, events)
	}
	update := events[1]
	if update.typ != eventTypeUpdate {
		t.Fatalf("event type = %s, want %s", update.typ, eventTypeUpdate)
	}
	if update.program == nil {
		t.Fatalf("update payload = nil, want program")
	}
	if got, want := update.program.Name, "existing title"; got != want {
		t.Fatalf("update payload name = %v, want %q", got, want)
	}
	if got, want := update.program.StartAt, int64(2000); got != want {
		t.Fatalf("update payload startAt = %v, want %d", got, want)
	}
}

// Genres omission in the Mirakurun shape is pinned by
// TestProgramContractOmitsEmptyKeys via internal/mirakurun.
