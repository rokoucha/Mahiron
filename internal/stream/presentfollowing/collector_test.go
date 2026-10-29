package presentfollowing

import (
	"github.com/21S1298001/mahiron/internal/model"
	"testing"
)

func TestCollectorWaitsForCompleteVersion(t *testing.T) {
	var c Collector
	key := model.ServiceKey{NetworkID: 1, StreamID: 2, ServiceID: 3}
	start := int64(100)
	old := model.Event{Key: key, EventID: 1, StartAt: &start}
	next := old
	next.EventID = 2
	if got := c.Observe(key, 0x4e, 31, 0, 1, []model.Event{old}); got != nil {
		t.Fatal("incomplete snapshot published")
	}
	if got := c.Observe(key, 0x4e, 0, 1, 1, []model.Event{old}); got != nil {
		t.Fatal("versions mixed at wraparound")
	}
	got := c.Observe(key, 0x4e, 0, 0, 1, []model.Event{next})
	if got == nil || got.ObservedAt <= 0 || len(got.Events) != 2 || !got.Events[0].EITPresent || !got.Events[1].EITFollowing || got.Events[1].EventID != 1 {
		t.Fatalf("interruption snapshot=%+v", got)
	}
	// Re-observation must return the complete snapshot so failed writes can retry.
	if c.Observe(key, 0x4e, 0, 0, 1, []model.Event{next}) == nil {
		t.Fatal("retry suppressed")
	}
	if c.Observe(key, 0x4e, 1, 0, 1, nil) != nil {
		t.Fatal("retired before following arrived")
	}
	got = c.Observe(key, 0x4e, 1, 1, 1, []model.Event{{EventID: 65535}})
	if got == nil || len(got.Events) != 0 {
		t.Fatalf("empty/undefined slots=%+v", got)
	}
}

func TestCollectorIsolatesServicesAndTables(t *testing.T) {
	var c Collector
	a := model.ServiceKey{NetworkID: 1, ServiceID: 2}
	b := a
	b.NetworkID = 2
	c.Observe(a, 0x4e, 1, 0, 1, nil)
	if c.Observe(b, 0x4e, 1, 1, 1, nil) != nil || c.Observe(a, 0x4f, 1, 1, 1, nil) != nil {
		t.Fatal("mixed unrelated subtables")
	}
	if c.Observe(a, 0x4e, 1, 2, 2, nil) != nil {
		t.Fatal("accepted invalid p/f section")
	}
}
