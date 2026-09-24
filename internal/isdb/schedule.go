package isdb

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ScheduleSystem selects the EIT schedule table layout. TS follows TR-B15
// and MMT follows TR-B39; both use the same 8-tables x 32-segments x
// 8-sections frame.
type ScheduleSystem int

const (
	// ScheduleTS covers table IDs 0x50-0x6F (actual and other streams).
	ScheduleTS ScheduleSystem = iota
	// ScheduleMMT covers table IDs 0x8C-0x9B (own stream only).
	ScheduleMMT
)

// scheduleBaseOf maps a schedule table ID to its ready-group base, the
// table index within the group (0-7) and whether it carries basic (short
// event, content, component) rather than extended descriptors.
func scheduleBaseOf(system ScheduleSystem, tableID uint8) (base uint8, index uint8, basic bool, ok bool) {
	switch system {
	case ScheduleMMT:
		switch {
		case tableID >= 0x8C && tableID <= 0x93:
			return 0x8C, tableID - 0x8C, true, true
		case tableID >= 0x94 && tableID <= 0x9B:
			return 0x94, tableID - 0x94, false, true
		default:
			return 0, 0, false, false
		}
	default:
		switch {
		case tableID >= 0x50 && tableID <= 0x57:
			return 0x50, tableID - 0x50, true, true
		case tableID >= 0x58 && tableID <= 0x5F:
			return 0x58, tableID - 0x58, false, true
		case tableID >= 0x60 && tableID <= 0x67:
			return 0x60, tableID - 0x60, true, true
		case tableID >= 0x68 && tableID <= 0x6F:
			return 0x68, tableID - 0x68, false, true
		default:
			return 0, 0, false, false
		}
	}
}

// ScheduleTracker keeps one service's EIT schedule reception state: which
// sections arrived, the newest payload per section (a re-sent section
// version replaces the old payload, which is how event removals and moves
// are reflected) and whether the basic and extended tables are complete.
// P is the opaque per-section payload; the tracker never interprets it.
type ScheduleTracker[P any] struct {
	system ScheduleSystem
	tables map[uint8]*scheduleTable[P]
	groups map[uint8]*scheduleReadyGroup
	latest time.Time
}

type scheduleTable[P any] struct {
	version     uint8
	hasVersion  bool
	lastSection uint8
	segmentLast map[uint8]uint8
	sections    map[uint8]struct{}
	payloads    map[uint8]P
	versions    map[uint8]uint8
}

type scheduleReadyGroup struct {
	base        uint8
	basic       bool
	lastFlagsID int
	flags       [8]scheduleReadyFlag
}

// scheduleReadyFlag covers one table: 32 segments of 3 hours (8 days), each
// a byte of 8 section bits.
type scheduleReadyFlag struct {
	observed bool
	version  uint8
	flag     [32]byte
	ignore   [32]byte
}

// NewScheduleTracker creates an empty tracker for one service.
func NewScheduleTracker[P any](system ScheduleSystem) *ScheduleTracker[P] {
	return &ScheduleTracker[P]{
		system: system,
		tables: make(map[uint8]*scheduleTable[P]),
		groups: make(map[uint8]*scheduleReadyGroup),
	}
}

// Observe records one section with its decoded payload. Sections outside
// the schedule tables are ignored. It reports whether reception made
// progress (a new or replaced section, or newly completed coverage),
// which drives collection stop decisions.
func (t *ScheduleTracker[P]) Observe(tableID uint8, header SectionHeader, payload P, now time.Time) bool {
	base, _, _, ok := scheduleBaseOf(t.system, tableID)
	if !ok {
		return false
	}
	table := t.tables[tableID]
	if table == nil {
		table = &scheduleTable[P]{
			segmentLast: make(map[uint8]uint8),
			sections:    make(map[uint8]struct{}),
			payloads:    make(map[uint8]P),
			versions:    make(map[uint8]uint8),
		}
		t.tables[tableID] = table
	}
	previous, existed := table.versions[header.SectionNumber]
	changed := !existed || previous != header.Version
	table.sections[header.SectionNumber] = struct{}{}
	table.payloads[header.SectionNumber] = payload
	table.versions[header.SectionNumber] = header.Version

	table.version = header.Version
	table.hasVersion = true
	table.lastSection = header.LastSectionNumber
	table.segmentLast[header.SectionNumber/8] = header.SegmentLastSection
	readyChanged := t.observeReady(tableID, base, header, now)

	if changed || readyChanged {
		t.latest = now
	}
	return changed || readyChanged
}

func (t *ScheduleTracker[P]) observeReady(tableID, base uint8, header SectionHeader, now time.Time) bool {
	_, index, basic, _ := scheduleBaseOf(t.system, tableID)
	// The last-table ID belongs to the same 8-table group; its index is
	// the table offset from the group base. (For 8-aligned TS bases this
	// is identical to LastTableID & 0x07.)
	lastBase, lastIndex, _, lastOK := scheduleBaseOf(t.system, header.LastTableID)
	lastFlagsID := int(header.LastTableID & 0x07)
	if lastOK && lastBase == base {
		lastFlagsID = int(lastIndex)
	}
	group := t.groups[base]
	if group == nil {
		group = &scheduleReadyGroup{base: base, basic: basic, lastFlagsID: -1}
		for i := range group.flags {
			for j := range group.flags[i].ignore {
				group.flags[i].ignore[j] = 0xff
			}
		}
		t.groups[base] = group
	}

	flagsID := int(index)
	target := &group.flags[flagsID]
	changed := false
	if group.lastFlagsID != lastFlagsID || (target.observed && target.version != header.Version) {
		group.reset(lastFlagsID)
		changed = true
	}
	group.lastFlagsID = lastFlagsID

	if !target.observed || target.version != header.Version {
		target.observed = true
		target.version = header.Version
		changed = true
	}

	if flagsID == 0 {
		currentSegment := currentJSTSegment(now)
		for i := 0; i < currentSegment; i++ {
			if target.ignore[i] != 0xff {
				target.ignore[i] = 0xff
				changed = true
			}
		}
	}

	lastSegment := int(header.LastSectionNumber >> 3)
	for i := lastSegment + 1; i < len(target.ignore); i++ {
		if target.ignore[i] != 0xff {
			target.ignore[i] = 0xff
			changed = true
		}
	}
	segmentNumber := int(header.SectionNumber >> 3)
	sectionNumber := uint(header.SectionNumber & 0x07)
	segmentLastSection := int(header.SegmentLastSection & 0x07)
	for i := segmentLastSection + 1; i < 8; i++ {
		mask := byte(1 << uint(i))
		if target.ignore[segmentNumber]&mask == 0 {
			target.ignore[segmentNumber] |= mask
			changed = true
		}
	}
	mask := byte(1 << sectionNumber)
	if target.flag[segmentNumber]&mask == 0 {
		target.flag[segmentNumber] |= mask
		changed = true
	}
	return changed
}

func (g *scheduleReadyGroup) reset(lastFlagsID int) {
	for i := range g.flags {
		g.flags[i].observed = false
		g.flags[i].version = 0
		for j := range g.flags[i].flag {
			g.flags[i].flag[j] = 0x00
			if i <= lastFlagsID {
				g.flags[i].ignore[j] = 0x00
			} else {
				g.flags[i].ignore[j] = 0xff
			}
		}
	}
}

func currentJSTSegment(now time.Time) int {
	const segmentDurationMillis = int64(3 * time.Hour / time.Millisecond)
	const jstOffsetMillis = int64(9 * time.Hour / time.Millisecond)
	return int(((now.UnixMilli() + jstOffsetMillis) / segmentDurationMillis) & 0x07)
}

// BasicComplete reports whether every observed basic group is fully covered
// and at least one basic group was observed.
func (t *ScheduleTracker[P]) BasicComplete() bool {
	hasBasic := false
	for _, group := range t.groups {
		if group == nil || !group.basic {
			continue
		}
		hasBasic = true
		if !group.ready() {
			return false
		}
	}
	return hasBasic
}

// ExtendedComplete reports whether every observed extended group is fully
// covered. Services without extended tables report true.
func (t *ScheduleTracker[P]) ExtendedComplete() bool {
	for _, group := range t.groups {
		if group == nil || group.basic {
			continue
		}
		if !group.ready() {
			return false
		}
	}
	return true
}

func (g *scheduleReadyGroup) ready() bool {
	if g == nil {
		return false
	}
	for i := range g.flags {
		for j := range g.flags[i].flag {
			if g.flags[i].flag[j]|g.flags[i].ignore[j] != 0xff {
				return false
			}
		}
	}
	return true
}

// StableFor reports whether reception made no progress for duration.
func (t *ScheduleTracker[P]) StableFor(now time.Time, duration time.Duration) bool {
	return !t.latest.IsZero() && now.Sub(t.latest) >= duration
}

// ScheduleSection is one received section's payload and whether it came from
// a basic (short event, content, component) or an extended table.
type ScheduleSection[P any] struct {
	Basic   bool
	Payload P
}

// Sections returns the current per-section payloads in table/section order.
func (t *ScheduleTracker[P]) Sections() []ScheduleSection[P] {
	tableIDs := make([]int, 0, len(t.tables))
	for id := range t.tables {
		tableIDs = append(tableIDs, int(id))
	}
	sort.Ints(tableIDs)
	var out []ScheduleSection[P]
	for _, id := range tableIDs {
		_, _, basic, _ := scheduleBaseOf(t.system, uint8(id))
		table := t.tables[uint8(id)]
		sections := make([]int, 0, len(table.payloads))
		for section := range table.payloads {
			sections = append(sections, int(section))
		}
		sort.Ints(sections)
		for _, section := range sections {
			out = append(out, ScheduleSection[P]{Basic: basic, Payload: table.payloads[uint8(section)]})
		}
	}
	return out
}

// HasBasic reports whether any basic schedule table was received. A service
// with only extended tables has no event names yet and does not count as
// observed.
func (t *ScheduleTracker[P]) HasBasic() bool {
	for id := range t.tables {
		if _, _, basic, _ := scheduleBaseOf(t.system, id); basic {
			return true
		}
	}
	return false
}

// Diagnosis summarizes missing tables and sections for log-only output.
func (t *ScheduleTracker[P]) Diagnosis() string {
	var missing []string
	for base, group := range t.groups {
		for i := 0; i <= group.lastFlagsID; i++ {
			tableID := base + uint8(i)
			if _, ok := t.tables[tableID]; !ok {
				missing = append(missing, fmt.Sprintf("table %#02x", tableID))
				continue
			}
			for _, section := range missingSections(group, uint8(i)) {
				missing = append(missing, fmt.Sprintf("table %#02x section %d", tableID, section))
			}
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		return "complete"
	}
	return "missing " + strings.Join(missing, ", ")
}

func missingSections(group *scheduleReadyGroup, flagsID uint8) []int {
	if group == nil {
		return nil
	}
	flag := group.flags[flagsID]
	var missing []int
	for segment := 0; segment < len(flag.flag); segment++ {
		needed := ^(flag.flag[segment] | flag.ignore[segment])
		for section := 0; section < 8; section++ {
			if needed&(1<<uint(section)) != 0 {
				missing = append(missing, segment*8+section)
			}
		}
	}
	return missing
}
