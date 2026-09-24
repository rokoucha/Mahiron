// Package schedule assembles decoded EIT sections into the schedule and
// present/following updates that sessions hand to EPG gathering. TS and MMT
// sessions share it: they decode their sections into model events, and this
// package tracks reception with isdb.ScheduleTracker, whose table layout is
// the same for both systems.
package schedule

import (
	"sort"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
)

// Section is one decoded EIT section.
type Section struct {
	TableID uint8
	Header  isdb.SectionHeader
	Service model.ServiceKey
	Events  []model.Event
}

// Collector keeps the EIT reception state of one collection. It is safe for
// concurrent use: the session goroutine observes sections while the
// receiver evaluates the events of earlier updates.
type Collector struct {
	system isdb.ScheduleSystem

	mu       sync.Mutex
	services map[model.ServiceKey]*serviceSchedule
	pf       map[model.ServiceKey]*model.PresentFollowing
}

type serviceSchedule struct {
	tracker *isdb.ScheduleTracker[[]model.Event]
	// events caches the assembled events until a section arrives.
	events []model.Event
	stale  bool
}

func NewCollector(system isdb.ScheduleSystem) *Collector {
	return &Collector{
		system:   system,
		services: make(map[model.ServiceKey]*serviceSchedule),
		pf:       make(map[model.ServiceKey]*model.PresentFollowing),
	}
}

// Observe records a section. A schedule section that makes progress calls
// onSchedule, and a present/following section calls onPresentFollowing.
// now is the broadcast clock, which decides the elapsed segments of today.
func (c *Collector) Observe(section Section, now time.Time, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
	switch c.classify(section.TableID) {
	case isdb.EITKindPresentFollowing:
		pf, ok := c.observePresentFollowing(section)
		if !ok || onPresentFollowing == nil {
			return nil
		}
		return onPresentFollowing(pf)
	case isdb.EITKindScheduleBasic, isdb.EITKindScheduleExtended:
		update, ok := c.observeSchedule(section, now)
		if !ok || onSchedule == nil {
			return nil
		}
		return onSchedule(update)
	default:
		return nil
	}
}

func (c *Collector) classify(tableID uint8) isdb.EITKind {
	if c.system == isdb.ScheduleMMT {
		return isdb.ClassifyMMTEITTableID(tableID)
	}
	return isdb.ClassifyTSEITTableID(tableID)
}

// observePresentFollowing keeps section 0 as the present event and section 1
// as the following one.
func (c *Collector) observePresentFollowing(section Section) (model.PresentFollowing, bool) {
	if section.Header.SectionNumber > 1 {
		return model.PresentFollowing{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	pf := c.pf[section.Service]
	if pf == nil {
		pf = &model.PresentFollowing{Service: section.Service}
		c.pf[section.Service] = pf
	}
	var event *model.Event
	if len(section.Events) > 0 {
		e := section.Events[0]
		event = &e
	}
	if section.Header.SectionNumber == 0 {
		pf.Present = event
	} else {
		pf.Following = event
	}
	return *pf, true
}

func (c *Collector) observeSchedule(section Section, now time.Time) (model.ScheduleUpdate, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	schedule := c.services[section.Service]
	if schedule == nil {
		schedule = &serviceSchedule{tracker: isdb.NewScheduleTracker[[]model.Event](c.system)}
		c.services[section.Service] = schedule
	}
	if !schedule.tracker.Observe(section.TableID, section.Header, section.Events, now) {
		return model.ScheduleUpdate{}, false
	}
	schedule.stale = true
	return model.ScheduleUpdate{
		Service:          section.Service,
		BasicObserved:    schedule.tracker.HasBasic(),
		BasicComplete:    schedule.tracker.BasicComplete(),
		ExtendedComplete: schedule.tracker.ExtendedComplete(),
		ObservedAt:       now.UnixMilli(),
		Events:           func() []model.Event { return c.events(schedule) },
		Diagnosis:        func() string { return c.diagnosis(schedule) },
	}, true
}

func (c *Collector) events(schedule *serviceSchedule) []model.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	if schedule.stale {
		schedule.events = assemble(schedule.tracker.Sections())
		schedule.stale = false
	}
	return append([]model.Event(nil), schedule.events...)
}

func (c *Collector) diagnosis(schedule *serviceSchedule) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return schedule.tracker.Diagnosis()
}

// assemble merges the received sections into one event per event ID, in
// event ID order. Basic tables create the events and update their timing;
// extended tables only add descriptions to events a basic table created.
func assemble(sections []isdb.ScheduleSection[[]model.Event]) []model.Event {
	events := make(map[uint16]*model.Event)
	extended := make(map[uint16][]model.Event)
	for _, section := range sections {
		for _, e := range section.Payload {
			if !section.Basic {
				extended[e.EventID] = append(extended[e.EventID], e)
				continue
			}
			if dst := events[e.EventID]; dst != nil {
				merge(dst, e, true)
			} else {
				clone := cloneEvent(e)
				events[e.EventID] = &clone
			}
		}
	}
	ids := make([]int, 0, len(events))
	for id := range events {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	out := make([]model.Event, 0, len(ids))
	for _, id := range ids {
		dst := events[uint16(id)]
		for _, e := range extended[uint16(id)] {
			merge(dst, e, false)
		}
		out = append(out, *dst)
	}
	return out
}

// cloneEvent copies the extended blocks, the only part merge modifies in
// place; the other slices are replaced, never appended to.
func cloneEvent(e model.Event) model.Event {
	if len(e.Extended) > 0 {
		blocks := make([]model.ExtendedBlock, len(e.Extended))
		for i, block := range e.Extended {
			block.Items = append([]model.ExtendedItem(nil), block.Items...)
			blocks[i] = block
		}
		e.Extended = blocks
	}
	return e
}

// merge fills what dst lacks from src. updateTiming also takes src's start
// time and duration, which basic tables carry.
func merge(dst *model.Event, src model.Event, updateTiming bool) {
	if updateTiming && src.StartAt != nil {
		dst.StartAt = src.StartAt
	}
	if updateTiming && src.DurationMS != nil {
		dst.DurationMS = src.DurationMS
	}
	dst.FreeCA = src.FreeCA
	if dst.RunningStatus == 0 {
		dst.RunningStatus = src.RunningStatus
	}
	if dst.Language == "" {
		dst.Language = src.Language
	}
	if dst.Name == "" {
		dst.Name = src.Name
	}
	if dst.Description == "" {
		dst.Description = src.Description
	}
	if len(dst.Genres) == 0 {
		dst.Genres = src.Genres
	}
	if len(dst.Videos) == 0 {
		dst.Videos = src.Videos
	}
	if len(dst.Audios) == 0 {
		dst.Audios = src.Audios
	}
	for _, block := range src.Extended {
		mergeExtended(dst, block)
	}
	if len(dst.Related) == 0 {
		dst.Related = src.Related
	}
	if len(dst.Parental) == 0 {
		dst.Parental = src.Parental
	}
	if dst.Series == nil {
		dst.Series = src.Series
	}
}

// mergeExtended adds a language block's items that dst lacks, and fills
// items whose text dst has empty.
func mergeExtended(dst *model.Event, src model.ExtendedBlock) {
	for i := range dst.Extended {
		block := &dst.Extended[i]
		if block.Language != src.Language {
			continue
		}
	items:
		for _, item := range src.Items {
			for j := range block.Items {
				if block.Items[j].Name == item.Name {
					if block.Items[j].Text == "" {
						block.Items[j].Text = item.Text
					}
					continue items
				}
			}
			block.Items = append(block.Items, item)
		}
		if block.Body == "" {
			block.Body = src.Body
		}
		return
	}
	src.Items = append([]model.ExtendedItem(nil), src.Items...)
	dst.Extended = append(dst.Extended, src)
}
