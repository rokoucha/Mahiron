// Package presentfollowing assembles complete p/f subtables before retiring events.
package presentfollowing

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
)

type Updater interface {
	UpsertPresentFollowing(context.Context, model.PresentFollowingSnapshot) error
}

type key struct {
	service model.ServiceKey
	tableID byte
}

type subtable struct {
	version byte
	seen    [2]bool
	events  [2][]model.Event
}

type Collector struct{ tables map[key]*subtable }

// Observe waits for both sections of the same version, including empty sections.
// A present event may move to following during an interruption (TR-B14 19.3).
func (c *Collector) Observe(service model.ServiceKey, tableID, version, section, last byte, events []model.Event) *model.PresentFollowingSnapshot {
	if section > 1 || last != 1 {
		return nil
	}
	if c.tables == nil {
		c.tables = make(map[key]*subtable)
	}
	k := key{service, tableID}
	t := c.tables[k]
	if t == nil || t.version != version {
		t = &subtable{version: version}
		c.tables[k] = t
	}
	t.seen[section] = true
	t.events[section] = nil
	for _, e := range events {
		// An undefined following event has no meaningful event_id (TR-B14 19.1).
		if e.StartAt == nil && e.DurationMS == nil {
			continue
		}
		e.EITPresent = section == 0
		e.EITFollowing = section == 1
		t.events[section] = append(t.events[section], e)
	}
	if !t.seen[0] || !t.seen[1] {
		return nil
	}
	all := append([]model.Event{}, t.events[0]...)
	all = append(all, t.events[1]...)
	return &model.PresentFollowingSnapshot{Service: service, Events: all}
}
