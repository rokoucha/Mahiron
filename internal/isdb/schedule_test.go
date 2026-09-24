package isdb

import (
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
	if got := len(tracker.Payloads()); got != 8 {
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
	payloads := tracker.Payloads()
	if len(payloads) != 1 || payloads[0] != "new" {
		t.Fatalf("payloads = %v, want [new]", payloads)
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
