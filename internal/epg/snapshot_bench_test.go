package epg

import (
	"fmt"
	"testing"
	"time"
)

// A service's 8-day schedule arrives as 8 tables of up to 512 sections, each
// carrying a handful of events.
func benchmarkSections(tables, sectionsPerTable, eventsPerSection int) []*EITSection {
	sections := make([]*EITSection, 0, tables*sectionsPerTable)
	eventID := 0
	for table := range tables {
		for number := range sectionsPerTable {
			events := make([]EITEvent, 0, eventsPerSection)
			for range eventsPerSection {
				eventID++
				events = append(events, EITEvent{
					EventID:   uint16(eventID),
					StartTime: int64(eventID) * 1800000,
					Duration:  1800000,
					Descriptors: []EITDescriptor{
						{Type: "ShortEvent", EventName: fmt.Sprintf("program %d", eventID), Text: "description"},
						{Type: "ExtendedEvent", Items: [][]string{{"番組内容", "text"}}},
					},
				})
			}
			sections = append(sections, &EITSection{
				OriginalNetworkID:        1,
				TransportStreamID:        1,
				ServiceID:                101,
				TableID:                  uint8(0x50 + table),
				LastTableID:              uint8(0x50 + tables - 1),
				SectionNumber:            uint8(number % 256),
				LastSectionNumber:        255,
				SegmentLastSectionNumber: 7,
				Events:                   events,
			})
		}
	}
	return sections
}

func BenchmarkSnapshotObserveService(b *testing.B) {
	sections := benchmarkSections(8, 64, 4)
	key := ServiceKey{NetworkID: 1, ServiceID: 101, TransportStreamID: 1}
	now := time.Now()

	b.ReportAllocs()
	for b.Loop() {
		snapshot := NewSnapshot()
		for _, section := range sections {
			snapshot.Observe(section, now)
		}
		if got := len(snapshot.Programs(key)); got == 0 {
			b.Fatalf("no programs")
		}
	}
}
