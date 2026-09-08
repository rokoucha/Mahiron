package epg

import (
	"sort"

	"github.com/21S1298001/mahiron/internal/program"
)

// rebuildPrograms merges the observed sections back into the service's program
// set, and does nothing if no section has arrived since the last time.
//
// It runs when the programs are read — every few seconds, on a partial flush
// or at the end of a collection — rather than on every section observed. A
// service's 8-day schedule arrives as hundreds of sections, and rebuilding
// after each one made a collection quadratic in the number of sections: it
// deep-copied every program of the service, maps included, once per section,
// which for one service came to 300 MB of garbage and 1.6M allocations.
func (s *snapshotService) rebuildPrograms() {
	if !s.stale {
		return
	}
	s.stale = false
	s.programs = make(map[int64]*program.Program)
	tableIDs := make([]int, 0, len(s.tables))
	for tableID := range s.tables {
		tableIDs = append(tableIDs, int(tableID))
	}
	sort.Ints(tableIDs)

	extended := make(map[int64][]*program.Program)
	for _, id := range tableIDs {
		tableID := uint8(id)
		table := s.tables[tableID]
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
					if s.programs[item.ID] == nil {
						s.programs[item.ID] = cloneProgram(item)
					} else {
						mergeProgram(s.programs[item.ID], item, true)
					}
				case isScheduleExtended(tableID):
					extended[item.ID] = append(extended[item.ID], item)
				default:
					s.programs[item.ID] = cloneProgram(item)
				}
			}
		}
	}
	for _, id := range sortedProgramIDs(s.programs) {
		for _, item := range extended[id] {
			mergeProgram(s.programs[id], item, false)
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
