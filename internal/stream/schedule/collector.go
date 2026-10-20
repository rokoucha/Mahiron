// Package schedule assembles decoded EIT sections into the schedule and
// present/following updates that sessions hand to EPG gathering. TS and MMT
// sessions share it: they decode their sections into model events, and this
// package tracks reception with isdb.ScheduleTracker, whose table layout is
// the same for both systems.
package schedule

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
)

// Section is one decoded EIT section.
type Section struct {
	TableID      uint8
	Header       isdb.SectionHeader
	Service      model.ServiceKey
	Events       []model.Event
	DecodeEvents func() []model.Event
}

// Collector keeps the EIT reception state of one collection. It is safe for
// concurrent use: the session goroutine observes sections while the
// receiver evaluates the events of earlier updates.
type Collector struct {
	system isdb.ScheduleSystem
	source string

	mu                    sync.Mutex
	services              map[model.ServiceKey]*serviceSchedule
	pf                    map[model.ServiceKey]*model.PresentFollowing
	operations            map[operationService]bool
	actualServices        map[operationService]byte
	terrestrialParameters map[uint16][]operationDescriptor
}

type serviceSchedule struct {
	current      model.ScheduleUpdate
	tracker      *isdb.ScheduleTracker[[]model.Event]
	revisions    map[uint8]model.EITRevision
	seenVersions map[uint8]uint32
	// events caches the assembled events until a section arrives.
	events []model.Event
	stale  bool
}

var receptionSequence atomic.Uint64

func NewCollector(system isdb.ScheduleSystem, source ...string) *Collector {
	origin := ""
	if len(source) > 0 {
		origin = source[0]
	}
	return &Collector{
		system:   system,
		source:   origin,
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
		if section.DecodeEvents != nil {
			section.Events = section.DecodeEvents()
		}
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
		schedule = &serviceSchedule{tracker: isdb.NewScheduleTracker[[]model.Event](c.system), revisions: make(map[uint8]model.EITRevision), seenVersions: make(map[uint8]uint32)}
		c.services[section.Service] = schedule
	}
	if !section.Header.CurrentNext || section.Header.Version > 31 {
		return model.ScheduleUpdate{}, false
	}
	if previous, ok := schedule.revisions[section.TableID]; ok && previous.Version != section.Header.Version {
		mask := schedule.seenVersions[section.TableID]
		if mask&(uint32(1)<<section.Header.Version) != 0 {
			// A retired version is ambiguous until a full cycle has been observed.
			// Keep the current version rather than label a replay as new.
			if mask != ^uint32(0) {
				return model.ScheduleUpdate{}, false
			}
			mask = uint32(1) << previous.Version
		}
		schedule.seenVersions[section.TableID] = mask
	}
	schedule.seenVersions[section.TableID] |= uint32(1) << section.Header.Version
	key := section.TableID
	previous, hadPrevious := schedule.revisions[key]
	revision, ok := schedule.revisions[key]
	if !ok || revision.Version != section.Header.Version {
		revision = model.EITRevision{Source: c.source, TableID: section.TableID, Version: section.Header.Version, Reception: receptionSequence.Add(1)}
		schedule.revisions[key] = revision
	}
	resets := schedule.tracker.CoverageResets()
	if !schedule.tracker.ObserveLazy(section.TableID, section.Header, func() []model.Event {
		events := section.Events
		if section.DecodeEvents != nil {
			events = section.DecodeEvents()
		}
		events = append([]model.Event(nil), events...)
		for i := range events {
			if c.classify(section.TableID) == isdb.EITKindScheduleBasic {
				events[i].EITRevision = &revision
			} else {
				events[i].EITExtendedRevision = &revision
			}
		}
		return events
	}, now) {
		return model.ScheduleUpdate{}, false
	}
	schedule.stale = true
	update := model.ScheduleUpdate{
		Service: section.Service,
		Revisions: func() []model.EITRevision {
			c.mu.Lock()
			defer c.mu.Unlock()
			revisions := make([]model.EITRevision, 0, len(schedule.revisions))
			for _, revision := range schedule.revisions {
				revisions = append(revisions, revision)
			}
			return revisions
		},
		Reception: &model.ScheduleReception{
			TableID: section.TableID, Version: section.Header.Version,
			PreviousVersion: previous.Version, VersionChanged: hadPrevious && previous.Version != section.Header.Version,
			CoverageReset: schedule.tracker.CoverageResets() != resets,
			Section:       section.Header.SectionNumber, LastSection: section.Header.LastSectionNumber,
			SegmentLastSection: section.Header.SegmentLastSection, LastTableID: section.Header.LastTableID,
		},
		BasicObserved:    schedule.tracker.HasBasic(),
		ExtendedObserved: schedule.tracker.HasExtended(),
		BasicComplete:    schedule.tracker.BasicComplete(),
		ExtendedComplete: schedule.tracker.ExtendedComplete() && (schedule.tracker.HasExtended() || c.extendedNotTransmitted(section.Service)),
		ObservedAt:       now.UnixMilli(),
		Events:           func() []model.Event { return c.events(schedule) },
		Diagnosis:        func() string { return c.diagnosis(schedule) },
	}
	schedule.current = update
	return update, true
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
	if !schedule.tracker.HasExtended() && !c.extendedNotTransmitted(schedule.current.Service) {
		return schedule.tracker.Diagnosis() + "; extended transmission unknown (awaiting EIT or BIT parameters)"
	}
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
				if e.EITRevision != nil && dst.EITRevision != nil && !e.EITRevision.PreferTo(*dst.EITRevision) {
					continue
				}
				newer := cloneEvent(e)
				merge(&newer, *dst, false)
				newer.FreeCA = e.FreeCA
				events[e.EventID] = &newer
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
	if src.EITExtendedRevision != nil {
		if previous := dst.EITExtendedRevision; previous != nil {
			if !src.EITExtendedRevision.PreferTo(*previous) {
				return
			}
			if src.EITExtendedRevision.Reception > previous.Reception {
				dst.Extended = nil
			}
		}
		dst.EITExtendedRevision = src.EITExtendedRevision
	}

	if updateTiming && src.EITRevision != nil {
		dst.EITRevision = src.EITRevision
	}
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

func (c *Collector) extendedNotTransmitted(key model.ServiceKey) bool {
	required, known := c.operations[operationService{key.NetworkID, key.ServiceID}]
	return known && !required
}
