package epg

import (
	"log/slog"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
)

type ServiceKey struct {
	NetworkID         uint16
	ServiceID         uint16
	TransportStreamID uint16
}

type Snapshot struct {
	services     map[ServiceKey]*snapshotService
	lastProgress time.Time
}

type snapshotService struct {
	tables      map[uint8]*snapshotTable
	programs    map[int64]*program.Program
	lastTableID map[uint8]uint8
	readyGroups map[uint8]*snapshotReadyGroup
}

type snapshotTable struct {
	version         uint8
	hasVersion      bool
	lastSection     uint8
	segmentLast     map[uint8]uint8
	sections        map[uint8]struct{}
	sectionPrograms map[uint8][]*program.Program
	sectionVersions map[uint8]uint8
}

type snapshotReadyGroup struct {
	base        uint8
	lastFlagsID int
	flags       [8]snapshotReadyFlag
}

type snapshotReadyFlag struct {
	observed bool
	version  uint8
	flag     [32]byte
	ignore   [32]byte
}

type SnapshotReport struct {
	ObservedTables  int                   `json:"observedTables"`
	MissingTableIDs []int                 `json:"missingTableIds,omitempty"`
	Tables          []SnapshotTableReport `json:"tables,omitempty"`
}

type SnapshotTableReport struct {
	TableID            int   `json:"tableId"`
	Version            int   `json:"version"`
	LastSection        int   `json:"lastSection"`
	ObservedSections   int   `json:"observedSections"`
	MissingSections    []int `json:"missingSections,omitempty"`
	MissingSegmentInfo []int `json:"missingSegmentInfo,omitempty"`
	Complete           bool  `json:"complete"`
}

func NewSnapshot() *Snapshot {
	return &Snapshot{services: make(map[ServiceKey]*snapshotService)}
}

func (s *Snapshot) Observe(section *EITSection, now time.Time) bool {
	if section == nil {
		return false
	}
	if section.TableID < 0x50 || section.TableID > 0x6f {
		slog.Warn("ignoring EITS section for unsupported table",
			"networkId", section.OriginalNetworkID,
			"serviceId", section.ServiceID,
			"tableId", section.TableID)
		return false
	}
	key := ServiceKey{NetworkID: section.OriginalNetworkID, ServiceID: section.ServiceID, TransportStreamID: section.TransportStreamID}
	service := s.services[key]
	if service == nil {
		service = &snapshotService{
			tables:      make(map[uint8]*snapshotTable),
			programs:    make(map[int64]*program.Program),
			lastTableID: make(map[uint8]uint8),
			readyGroups: make(map[uint8]*snapshotReadyGroup),
		}
		s.services[key] = service
	}
	table := service.tables[section.TableID]
	if table == nil {
		table = &snapshotTable{
			segmentLast:     make(map[uint8]uint8),
			sections:        make(map[uint8]struct{}),
			sectionPrograms: make(map[uint8][]*program.Program),
			sectionVersions: make(map[uint8]uint8),
		}
		service.tables[section.TableID] = table
	}

	previousVersion, existed := table.sectionVersions[section.SectionNumber]
	changed := !existed || previousVersion != section.VersionNumber
	table.sections[section.SectionNumber] = struct{}{}
	table.sectionPrograms[section.SectionNumber] = section.Programs()
	table.sectionVersions[section.SectionNumber] = section.VersionNumber
	rebuildServicePrograms(service)

	table.version = section.VersionNumber
	table.hasVersion = true
	table.lastSection = section.LastSectionNumber
	table.segmentLast[section.SectionNumber/8] = section.SegmentLastSectionNumber
	service.lastTableID[section.TableID&0xf8] = section.LastTableID
	readyChanged := service.observeReady(section, now)

	if changed || readyChanged {
		s.lastProgress = now
	}
	return changed || readyChanged
}

func (s *snapshotService) observeReady(section *EITSection, now time.Time) bool {
	base := section.TableID & 0xf8
	group := s.readyGroups[base]
	if group == nil {
		group = &snapshotReadyGroup{base: base, lastFlagsID: -1}
		for i := range group.flags {
			for j := range group.flags[i].ignore {
				group.flags[i].ignore[j] = 0xff
			}
		}
		s.readyGroups[base] = group
	}

	flagsID := int(section.TableID & 0x07)
	lastFlagsID := int(section.LastTableID & 0x07)
	target := &group.flags[flagsID]
	changed := false
	if group.lastFlagsID != lastFlagsID || (target.observed && target.version != section.VersionNumber) {
		group.reset(lastFlagsID)
		changed = true
	}
	group.lastFlagsID = lastFlagsID

	if !target.observed || target.version != section.VersionNumber {
		target.observed = true
		target.version = section.VersionNumber
		changed = true
	}

	if flagsID == 0 && isCurrentScheduleBase(base) {
		currentSegment := currentJSTSegment(now)
		for i := 0; i < currentSegment; i++ {
			if target.ignore[i] != 0xff {
				target.ignore[i] = 0xff
				changed = true
			}
		}
	}

	lastSegment := int(section.LastSectionNumber >> 3)
	for i := lastSegment + 1; i < len(target.ignore); i++ {
		if target.ignore[i] != 0xff {
			target.ignore[i] = 0xff
			changed = true
		}
	}

	segmentNumber := int(section.SectionNumber >> 3)
	sectionNumber := uint(section.SectionNumber & 0x07)
	segmentLastSection := int(section.SegmentLastSectionNumber & 0x07)
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

func (g *snapshotReadyGroup) reset(lastFlagsID int) {
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

func isCurrentScheduleBase(base uint8) bool {
	return base == 0x50 || base == 0x58 || base == 0x60 || base == 0x68
}

func isScheduleBasic(tableID uint8) bool {
	return (tableID >= 0x50 && tableID <= 0x57) || (tableID >= 0x60 && tableID <= 0x67)
}

func isScheduleExtended(tableID uint8) bool {
	return (tableID >= 0x58 && tableID <= 0x5f) || (tableID >= 0x68 && tableID <= 0x6f)
}

func currentJSTSegment(now time.Time) int {
	const segmentDurationMillis = int64(3 * time.Hour / time.Millisecond)
	const jstOffsetMillis = int64(9 * time.Hour / time.Millisecond)
	return int(((now.UnixMilli() + jstOffsetMillis) / segmentDurationMillis) & 0x07)
}

func (s *Snapshot) ServiceReady(key ServiceKey) bool {
	service := s.services[key]
	if service == nil || len(service.readyGroups) == 0 {
		return false
	}
	hasBasic := false
	for _, group := range service.readyGroups {
		if group == nil || !isScheduleBasic(group.base) {
			continue
		}
		hasBasic = true
		if !group.ready() {
			return false
		}
	}
	return hasBasic
}

func (g *snapshotReadyGroup) ready() bool {
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

func (s *Snapshot) CompletionReport(key ServiceKey) SnapshotReport {
	service := s.services[key]
	if service == nil {
		return SnapshotReport{}
	}

	tableIDs := make([]int, 0, len(service.tables))
	for tableID := range service.tables {
		tableIDs = append(tableIDs, int(tableID))
	}
	sort.Ints(tableIDs)

	report := SnapshotReport{ObservedTables: len(tableIDs)}
	readyGroups := make(map[uint8]struct{})
	for _, id := range tableIDs {
		tableID := uint8(id)
		table := service.tables[tableID]
		base := tableID & 0xf8
		readyGroups[base] = struct{}{}

		tableReport := SnapshotTableReport{
			TableID:          id,
			Version:          int(table.version),
			LastSection:      int(table.lastSection),
			ObservedSections: len(table.sections),
			Complete:         readyTableComplete(service.readyGroups[base], tableID),
		}
		tableReport.MissingSections = readyMissingSections(service.readyGroups[base], tableID)
		report.Tables = append(report.Tables, tableReport)
	}

	for base := range readyGroups {
		group := service.readyGroups[base]
		if group == nil {
			continue
		}
		for i := 0; i <= group.lastFlagsID; i++ {
			tableID := base + uint8(i)
			if _, ok := service.tables[tableID]; !ok {
				report.MissingTableIDs = append(report.MissingTableIDs, int(tableID))
			}
		}
	}
	sort.Ints(report.MissingTableIDs)
	return report
}

func readyTableComplete(group *snapshotReadyGroup, tableID uint8) bool {
	if group == nil {
		return false
	}
	flag := group.flags[tableID&0x07]
	for i := range flag.flag {
		if flag.flag[i]|flag.ignore[i] != 0xff {
			return false
		}
	}
	return true
}

func readyMissingSections(group *snapshotReadyGroup, tableID uint8) []int {
	if group == nil {
		return nil
	}
	flag := group.flags[tableID&0x07]
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

func (s *Snapshot) AllReady(expected []ServiceKey) bool {
	if len(expected) == 0 {
		return false
	}
	for _, key := range expected {
		if !s.ServiceReady(key) {
			return false
		}
	}
	return true
}

func (s *Snapshot) ObservedExtendedReady(expected []ServiceKey) bool {
	for _, key := range expected {
		service := s.services[key]
		if service == nil {
			continue
		}
		for _, group := range service.readyGroups {
			if group == nil || !isScheduleExtended(group.base) {
				continue
			}
			if !group.ready() {
				return false
			}
		}
	}
	return true
}

func (s *Snapshot) StableFor(now time.Time, duration time.Duration) bool {
	return !s.lastProgress.IsZero() && now.Sub(s.lastProgress) >= duration
}

func (s *Snapshot) Programs(key ServiceKey) []*program.Program {
	service := s.services[key]
	if service == nil {
		return nil
	}
	result := make([]*program.Program, 0, len(service.programs))
	for _, id := range sortedProgramIDs(service.programs) {
		result = append(result, service.programs[id])
	}
	return result
}

func (s *Snapshot) Observed(key ServiceKey) bool {
	service := s.services[key]
	if service == nil {
		return false
	}
	for tableID := range service.tables {
		if isScheduleBasic(tableID) {
			return true
		}
	}
	return false
}
