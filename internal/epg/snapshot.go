package epg

import (
	"log/slog"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
)

type ServiceKey struct {
	NetworkID uint16
	ServiceID uint16
}

type Snapshot struct {
	services     map[ServiceKey]*snapshotService
	lastProgress time.Time
}

type EITSnapshot = Snapshot

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

func NewEITSnapshot() *Snapshot {
	return NewSnapshot()
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
	key := ServiceKey{NetworkID: section.OriginalNetworkID, ServiceID: section.ServiceID}
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

func rebuildServicePrograms(service *snapshotService) {
	service.programs = make(map[int64]*program.Program)
	tableIDs := make([]int, 0, len(service.tables))
	for tableID := range service.tables {
		tableIDs = append(tableIDs, int(tableID))
	}
	sort.Ints(tableIDs)

	extended := make(map[int64][]*program.Program)
	for _, id := range tableIDs {
		tableID := uint8(id)
		table := service.tables[tableID]
		sectionNumbers := make([]int, 0, len(table.sectionPrograms))
		for sectionNumber := range table.sectionPrograms {
			sectionNumbers = append(sectionNumbers, int(sectionNumber))
		}
		sort.Ints(sectionNumbers)
		for _, sectionNumber := range sectionNumbers {
			for _, item := range table.sectionPrograms[uint8(sectionNumber)] {
				if item == nil {
					continue
				}
				switch {
				case isScheduleBasic(tableID):
					if service.programs[item.ID] == nil {
						service.programs[item.ID] = cloneProgram(item)
					} else {
						mergeProgram(service.programs[item.ID], item, true)
					}
				case isScheduleExtended(tableID):
					extended[item.ID] = append(extended[item.ID], item)
				default:
					service.programs[item.ID] = cloneProgram(item)
				}
			}
		}
	}
	for _, id := range sortedProgramIDs(service.programs) {
		for _, item := range extended[id] {
			mergeProgram(service.programs[id], item, false)
		}
	}
}

func cloneProgram(src *program.Program) *program.Program {
	if src == nil {
		return nil
	}
	dst := *src
	if len(src.Genres) > 0 {
		dst.Genres = append([]program.Genre(nil), src.Genres...)
	}
	if src.Video != nil {
		video := *src.Video
		dst.Video = &video
	}
	if len(src.Audios) > 0 {
		dst.Audios = append([]program.Audio(nil), src.Audios...)
	}
	if len(src.Extended) > 0 {
		dst.Extended = cloneStringMap(src.Extended)
	}
	if len(src.RelatedItems) > 0 {
		dst.RelatedItems = append([]program.RelatedItem(nil), src.RelatedItems...)
	}
	if src.Series != nil {
		series := *src.Series
		dst.Series = &series
	}
	return &dst
}

func mergeProgram(dst, src *program.Program, updateTiming bool) {
	if dst == nil || src == nil {
		return
	}
	if updateTiming && src.StartAt != 0 {
		dst.StartAt = src.StartAt
	}
	if updateTiming && src.Duration != 0 {
		dst.Duration = src.Duration
	}
	dst.IsFree = src.IsFree
	if dst.Name == "" && src.Name != "" {
		dst.Name = src.Name
	}
	if dst.Description == "" && src.Description != "" {
		dst.Description = src.Description
	}
	if len(dst.Genres) == 0 && len(src.Genres) > 0 {
		dst.Genres = append([]program.Genre(nil), src.Genres...)
	}
	if dst.Video == nil && src.Video != nil {
		video := *src.Video
		dst.Video = &video
	}
	if len(dst.Audios) == 0 && len(src.Audios) > 0 {
		dst.Audios = append([]program.Audio(nil), src.Audios...)
	}
	if len(src.Extended) > 0 {
		if dst.Extended == nil {
			dst.Extended = make(map[string]string, len(src.Extended))
		}
		for key, value := range src.Extended {
			if dst.Extended[key] == "" && value != "" {
				dst.Extended[key] = value
			}
		}
	}
	if len(dst.RelatedItems) == 0 && len(src.RelatedItems) > 0 {
		dst.RelatedItems = append([]program.RelatedItem(nil), src.RelatedItems...)
	}
	if dst.Series == nil && src.Series != nil {
		series := *src.Series
		dst.Series = &series
	}
}

func sortedProgramIDs(programs map[int64]*program.Program) []int64 {
	ids := make([]int64, 0, len(programs))
	for id := range programs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *Snapshot) ServiceComplete(key ServiceKey) bool {
	return s.ServiceReady(key)
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

func (s *snapshotService) maxExpectedTableID(base uint8, observedMax uint8) uint8 {
	if last, ok := s.lastTableID[base]; ok && last >= base && last <= base+7 {
		return last
	}
	return observedMax
}

func maxObservedSnapshotTableID(base uint8, tables map[uint8]*snapshotTable) uint8 {
	maxTable := base
	for tableID := range tables {
		if tableID > maxTable {
			maxTable = tableID
		}
	}
	return maxTable
}

func maxObservedTableID(base uint8, tables map[uint8]struct{}) uint8 {
	maxTable := base
	for tableID := range tables {
		if tableID > maxTable {
			maxTable = tableID
		}
	}
	return maxTable
}

func snapshotTableComplete(table *snapshotTable, allowLeadingMissing bool) bool {
	firstObservedSegment := uint8(0)
	if allowLeadingMissing {
		firstObservedSegment = firstSegmentWithInfo(table)
	}
	lastSegment := table.lastSection / 8
	for segment := uint8(0); segment <= lastSegment; segment++ {
		segmentLast, ok := table.segmentLast[segment]
		if !ok {
			if allowLeadingMissing && segment < firstObservedSegment {
				continue
			}
			return false
		}
		first := segment * 8
		last := segmentLast
		if last > table.lastSection {
			last = table.lastSection
		}
		if last < first {
			continue
		}
		for section := first; section <= last; section++ {
			if _, ok := table.sections[section]; !ok {
				return false
			}
			if section == 255 {
				break
			}
		}
		if segment == 31 {
			break
		}
	}
	return true
}

func firstSegmentWithInfo(table *snapshotTable) uint8 {
	if len(table.segmentLast) == 0 {
		return 0
	}
	first := table.lastSection/8 + 1
	for segment := range table.segmentLast {
		if segment < first {
			first = segment
		}
	}
	return first
}

func (s *Snapshot) AllComplete(expected []ServiceKey) bool {
	return s.AllReady(expected)
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
