package isdb

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func tsHeader(section, lastSection uint8, version uint8, lastTable, segmentLast uint8) SectionHeader {
	return SectionHeader{
		TableIDExtension:   1,
		Version:            version,
		SectionNumber:      section,
		LastSectionNumber:  lastSection,
		CurrentNext:        true,
		LastTableID:        lastTable,
		SegmentLastSection: segmentLast,
	}
}

// fixedNow is JST midnight: EIT segment 0 is current, so no elapsed
// segment is excluded and coverage assertions stay deterministic.
func fixedNow() time.Time {
	return time.Date(2026, 1, 1, 15, 0, 0, 0, time.UTC)
}

func feedFullTable[P any](t *testing.T, tracker *ScheduleTracker[P], tableID uint8, payload func(section uint8) P, now time.Time) {
	t.Helper()
	for section := uint8(0); section < 8; section++ {
		if !tracker.Observe(tableID, tsHeader(section, 7, 1, tableID, 7), payload(section), now) {
			t.Fatalf("table %#02x section %d made no progress", tableID, section)
		}
	}
}

func TestScheduleTrackerBasicComplete(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	if tracker.BasicComplete() {
		t.Fatal("empty tracker unexpectedly complete")
	}
	if !tracker.ExtendedComplete() {
		t.Fatal("empty tracker should report extended complete (nothing to wait for)")
	}
	feedFullTable(t, tracker, 0x50, func(section uint8) string { return string(rune('a' + section)) }, now)
	if !tracker.BasicComplete() {
		t.Fatalf("basic not complete: %s", tracker.Diagnosis())
	}
	if got := len(tracker.Sections()); got != 8 {
		t.Fatalf("payloads = %d, want 8", got)
	}
	if !tracker.StableFor(now.Add(time.Hour), time.Minute) {
		t.Fatal("tracker should be stable after an hour without progress")
	}
}

func TestScheduleTrackerIgnoresNonScheduleTables(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	if tracker.Observe(0x4E, tsHeader(0, 0, 1, 0x4E, 0), "pf", now) {
		t.Fatal("p/f table must not drive schedule progress")
	}
	if tracker.BasicComplete() {
		t.Fatal("p/f-only tracker unexpectedly complete")
	}
}

func TestScheduleTrackerReplacesSectionPayload(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	tracker.Observe(0x50, tsHeader(0, 7, 1, 0x50, 7), "old", now)
	tracker.Observe(0x50, tsHeader(0, 7, 2, 0x50, 7), "new", now)
	sections := tracker.Sections()
	if len(sections) != 1 || sections[0].Payload != "new" || !sections[0].Basic {
		t.Fatalf("sections = %v, want one basic [new]", sections)
	}
}

func TestScheduleTrackerExtendedComplete(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	feedFullTable(t, tracker, 0x58, func(section uint8) string { return "x" }, now)
	if !tracker.ExtendedComplete() {
		t.Fatalf("extended not complete: %s", tracker.Diagnosis())
	}
	// Basic must still be incomplete: only an extended table arrived.
	if tracker.BasicComplete() {
		t.Fatal("extended-only tracker unexpectedly basic-complete")
	}
}

func TestScheduleTrackerMMTLayout(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleMMT)
	feedFullTable(t, tracker, 0x8C, func(section uint8) string { return "b" }, now)
	if !tracker.BasicComplete() {
		t.Fatalf("MMT basic not complete: %s", tracker.Diagnosis())
	}
	if tracker.Observe(0x50, tsHeader(0, 7, 1, 0x50, 7), "ts", now) {
		t.Fatal("TS table must not drive an MMT tracker")
	}
}

func TestScheduleTrackerDiagnosisNamesMissing(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	tracker.Observe(0x50, tsHeader(0, 7, 1, 0x50, 7), "a", now)
	if tracker.BasicComplete() {
		t.Fatal("partial tracker unexpectedly complete")
	}
	if got := tracker.Diagnosis(); got == "complete" {
		t.Fatalf("diagnosis = %q, want missing details", got)
	}
}

// TestScheduleTrackerCoversEightDays feeds all 32 three-hour segments of one
// table. Section numbers run up to 255, so the coverage must span 32
// segments rather than one day's 8.
func TestScheduleTrackerCoversEightDays(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	for segment := 0; segment < 32; segment++ {
		section := uint8(segment * 8)
		tracker.Observe(0x50, tsHeader(section, 255, 1, 0x50, section), "s", now)
		if segment < 31 && tracker.BasicComplete() {
			t.Fatalf("complete after segment %d of 32", segment)
		}
	}
	if !tracker.BasicComplete() {
		t.Fatalf("basic not complete: %s", tracker.Diagnosis())
	}
}

func TestScheduleTrackerOtherStreamTables(t *testing.T) {
	now := fixedNow()
	tracker := NewScheduleTracker[string](ScheduleTS)
	feedFullTable(t, tracker, 0x68, func(uint8) string { return "e" }, now)
	if tracker.HasBasic() {
		t.Fatal("extended-only tracker reports basic tables")
	}
	feedFullTable(t, tracker, 0x60, func(uint8) string { return "b" }, now)
	if !tracker.HasBasic() || !tracker.BasicComplete() || !tracker.ExtendedComplete() {
		t.Fatalf("other-stream tables not complete: %s", tracker.Diagnosis())
	}
	sections := tracker.Sections()
	if len(sections) != 16 || !sections[0].Basic || sections[8].Basic {
		t.Fatalf("sections = %v, want 8 basic then 8 extended", sections)
	}
}

func TestScheduleTrackerVersionChangePreservesOtherTables(t *testing.T) {
	for _, system := range []ScheduleSystem{ScheduleTS, ScheduleMMT} {
		t.Run(fmt.Sprint(system), func(t *testing.T) {
			first := uint8(0x58)
			if system == ScheduleMMT {
				first = 0x94
			}
			tracker := NewScheduleTracker[string](system)
			observe := func(id, section, version uint8) {
				tracker.Observe(id, tsHeader(section, 248, version, first+1, section), "payload", fixedNow())
			}
			for _, id := range []uint8{first, first + 1} {
				for segment := 0; segment < 32; segment++ {
					observe(id, uint8(segment*8), 1)
				}
			}
			if !tracker.ExtendedComplete() {
				t.Fatal(tracker.Diagnosis())
			}
			observe(first, 0, 2)
			if tracker.ExtendedComplete() {
				t.Fatal("updated table prematurely complete")
			}
			for segment := 1; segment < 32; segment++ {
				observe(first, uint8(segment*8), 2)
			}
			if !tracker.ExtendedComplete() {
				t.Fatalf("unchanged table lost coverage: %s", tracker.Diagnosis())
			}
			if tracker.CoverageResets() != 1 {
				t.Fatalf("resets = %d", tracker.CoverageResets())
			}
		})
	}
}

func TestScheduleTrackerRangeChangePreservesExistingTables(t *testing.T) {
	tracker := NewScheduleTracker[string](ScheduleMMT)
	tracker.Observe(0x94, tsHeader(0, 0, 1, 0x94, 0), "first", fixedNow())
	tracker.Observe(0x95, tsHeader(0, 0, 1, 0x95, 0), "second", fixedNow())
	if !tracker.ExtendedComplete() {
		t.Fatalf("range expansion cleared existing table: %s", tracker.Diagnosis())
	}
	tracker.Observe(0x94, tsHeader(0, 0, 2, 0x94, 0), "shrink", fixedNow())
	if !tracker.ExtendedComplete() {
		t.Fatal(tracker.Diagnosis())
	}
	tracker.Observe(0x94, tsHeader(0, 0, 3, 0x95, 0), "expand", fixedNow())
	if tracker.ExtendedComplete() {
		t.Fatal("expanded range reused removed table's coverage")
	}
}

func TestScheduleTrackerLazyDuplicatesAndBoundedDiagnosis(t *testing.T) {
	tracker := NewScheduleTracker[string](ScheduleMMT)
	calls := 0
	payload := func() string { calls++; return "payload" }
	header := tsHeader(0, 255, 1, 0x95, 0)
	tracker.ObserveLazy(0x94, header, payload, fixedNow())
	tracker.ObserveLazy(0x94, header, payload, fixedNow())
	if calls != 1 {
		t.Fatalf("duplicate decoded %d payloads", calls)
	}
	if diagnosis := tracker.Diagnosis(); !strings.Contains(diagnosis, "more)") || len(diagnosis) > 1200 {
		t.Fatalf("unbounded diagnosis: %s", diagnosis)
	}
}
