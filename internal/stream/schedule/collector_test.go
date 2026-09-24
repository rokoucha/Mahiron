package schedule

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
)

var testService = model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101}

// testNow is JST midnight, so that no segment of today counts as elapsed.
var testNow = time.Date(2026, 9, 24, 15, 0, 0, 0, time.UTC)

func section(tableID, number, last, version uint8, events ...model.Event) Section {
	return Section{
		TableID: tableID,
		Header: isdb.SectionHeader{
			TableIDExtension:   testService.ServiceID,
			Version:            version,
			SectionNumber:      number,
			LastSectionNumber:  last,
			CurrentNext:        true,
			LastTableID:        tableID,
			SegmentLastSection: number,
		},
		Service: testService,
		Events:  events,
	}
}

func event(id uint16, name string) model.Event {
	start := int64(id) * 1000
	return model.Event{Key: testService, EventID: id, StartAt: &start, Name: name}
}

func extendedEvent(id uint16, item string) model.Event {
	return model.Event{Key: testService, EventID: id, Extended: []model.ExtendedBlock{{
		Language: "jpn",
		Items:    []model.ExtendedItem{{Name: item, Text: item + " text"}},
	}}}
}

// observe feeds the sections and returns the last schedule update.
func observe(t *testing.T, c *Collector, sections ...Section) model.ScheduleUpdate {
	t.Helper()
	var last model.ScheduleUpdate
	for _, s := range sections {
		if err := c.Observe(s, testNow, func(update model.ScheduleUpdate) error {
			last = update
			return nil
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	return last
}

func eventIDs(events []model.Event) []uint16 {
	ids := make([]uint16, len(events))
	for i, e := range events {
		ids[i] = e.EventID
	}
	return ids
}

func equalIDs(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCollectorCompletesAndOrdersEvents(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	update := observe(t, c,
		section(0x50, 0, 0, 1, event(3, "three"), event(1, "one")),
	)
	if !update.BasicObserved || !update.BasicComplete || !update.ExtendedComplete {
		t.Fatalf("update = %+v, want observed and complete", update)
	}
	if got := eventIDs(update.Events()); !equalIDs(got, []uint16{1, 3}) {
		t.Fatalf("events = %v, want [1 3]", got)
	}
}

func TestCollectorVersionReplacesOnlyItsSection(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	observe(t, c,
		section(0x50, 0, 8, 1, event(1, "one")),
		section(0x50, 8, 8, 1, event(2, "two")),
	)
	update := observe(t, c, section(0x50, 8, 8, 2))
	if got := eventIDs(update.Events()); !equalIDs(got, []uint16{1}) {
		t.Fatalf("events = %v, want the emptied section's event removed", got)
	}
}

func TestCollectorDuplicateSectionMakesNoProgress(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	s := section(0x50, 0, 0, 1, event(1, "one"))
	observe(t, c, s)
	calls := 0
	if err := c.Observe(s, testNow, func(model.ScheduleUpdate) error {
		calls++
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("duplicate section reported %d updates, want none", calls)
	}
}

func TestCollectorMergesExtendedIntoBasicEvents(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	extendedOnly := observe(t, c, section(0x58, 0, 0, 1, extendedEvent(1, "cast"), extendedEvent(9, "orphan")))
	if extendedOnly.BasicObserved {
		t.Fatal("extended tables alone must not make the service observed")
	}
	if got := extendedOnly.Events(); len(got) != 0 {
		t.Fatalf("events = %+v, want none before a basic table", got)
	}
	update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
	events := update.Events()
	if got := eventIDs(events); !equalIDs(got, []uint16{1}) {
		t.Fatalf("events = %v, want only the basic table's event", got)
	}
	if len(events[0].Extended) != 1 || events[0].Extended[0].Items[0].Name != "cast" || events[0].Name != "one" {
		t.Fatalf("event = %+v, want name and extended description", events[0])
	}
}

func TestCollectorEventsFollowNewSections(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	first := observe(t, c, section(0x50, 0, 8, 1, event(1, "one")))
	if got := eventIDs(first.Events()); !equalIDs(got, []uint16{1}) {
		t.Fatalf("events = %v, want [1]", got)
	}
	observe(t, c, section(0x50, 8, 8, 1, event(2, "two")))
	// An earlier update's Events reflects sections that arrived since.
	if got := eventIDs(first.Events()); !equalIDs(got, []uint16{1, 2}) {
		t.Fatalf("events = %v, want [1 2]", got)
	}
}

func TestCollectorEventsAreCopies(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
	events := update.Events()
	events[0].Name = "changed"
	if got := update.Events()[0].Name; got != "one" {
		t.Fatalf("name = %q, want the collector's events unchanged", got)
	}
}

func TestCollectorPresentFollowing(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	var got model.PresentFollowing
	onPF := func(pf model.PresentFollowing) error {
		got = pf
		return nil
	}
	for _, s := range []Section{section(0x4E, 0, 1, 1, event(1, "now")), section(0x4E, 1, 1, 1, event(2, "next"))} {
		if err := c.Observe(s, testNow, nil, onPF); err != nil {
			t.Fatal(err)
		}
	}
	if got.Present == nil || got.Present.EventID != 1 || got.Following == nil || got.Following.EventID != 2 {
		t.Fatalf("present/following = %+v", got)
	}
}

func TestCollectorMMTTables(t *testing.T) {
	c := NewCollector(isdb.ScheduleMMT)
	if update := observe(t, c, section(0x50, 0, 0, 1, event(1, "ts"))); update.Service != (model.ServiceKey{}) {
		t.Fatalf("TS table reported %+v on an MMT collector", update)
	}
	update := observe(t, c, section(0x8C, 0, 0, 1, event(1, "mmt")))
	if !update.BasicComplete || len(update.Events()) != 1 {
		t.Fatalf("update = %+v, want the MH-EIT schedule", update)
	}
}
