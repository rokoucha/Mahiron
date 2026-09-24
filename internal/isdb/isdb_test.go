package isdb

import "testing"

func TestTableTrackerCompletes(t *testing.T) {
	var tracker TableTracker
	headers := []SectionHeader{
		{TableIDExtension: 1, Version: 3, SectionNumber: 0, LastSectionNumber: 1, CurrentNext: true},
		{TableIDExtension: 1, Version: 3, SectionNumber: 1, LastSectionNumber: 1, CurrentNext: true},
	}
	if _, ready := tracker.Add(headers[0]); ready {
		t.Fatal("single section of two unexpectedly ready")
	}
	if _, ready := tracker.Add(headers[1]); !ready {
		t.Fatal("both sections arrived but not ready")
	}
	if missing := tracker.Missing(); len(missing) != 0 {
		t.Fatalf("missing = %v, want none", missing)
	}
}

func TestTableTrackerResetsOnNewVersion(t *testing.T) {
	var tracker TableTracker
	tracker.Add(SectionHeader{TableIDExtension: 1, Version: 3, SectionNumber: 0, LastSectionNumber: 1, CurrentNext: true})
	tracker.Add(SectionHeader{TableIDExtension: 1, Version: 3, SectionNumber: 1, LastSectionNumber: 1, CurrentNext: true})
	reset, ready := tracker.Add(SectionHeader{TableIDExtension: 1, Version: 4, SectionNumber: 1, LastSectionNumber: 1, CurrentNext: true})
	if !reset {
		t.Fatal("version change did not reset")
	}
	if ready {
		t.Fatal("old section must not count toward the new version")
	}
}

func TestClassifyEITTableIDs(t *testing.T) {
	cases := []struct {
		tableID uint8
		ts      EITKind
		mmt     EITKind
	}{
		{0x4E, EITKindPresentFollowing, EITKindOther},
		{0x4F, EITKindPresentFollowing, EITKindOther},
		{0x50, EITKindScheduleBasic, EITKindOther},
		{0x57, EITKindScheduleBasic, EITKindOther},
		{0x58, EITKindScheduleExtended, EITKindOther},
		{0x5F, EITKindScheduleExtended, EITKindOther},
		{0x8B, EITKindOther, EITKindPresentFollowing},
		{0x8C, EITKindOther, EITKindScheduleBasic},
		{0x93, EITKindOther, EITKindScheduleBasic},
		{0x94, EITKindOther, EITKindScheduleExtended},
		{0x9B, EITKindOther, EITKindScheduleExtended},
		{0x40, EITKindOther, EITKindOther},
	}
	for _, c := range cases {
		if got := ClassifyTSEITTableID(c.tableID); got != c.ts {
			t.Errorf("ClassifyTSEITTableID(%#02x) = %v, want %v", c.tableID, got, c.ts)
		}
		if got := ClassifyMMTEITTableID(c.tableID); got != c.mmt {
			t.Errorf("ClassifyMMTEITTableID(%#02x) = %v, want %v", c.tableID, got, c.mmt)
		}
	}
}

func TestClassifyOriginalNetworkID(t *testing.T) {
	if got := ClassifyOriginalNetworkID(0x000B); got != NetworkClassAdvancedBS {
		t.Fatalf("0x000B = %v, want advanced BS", got)
	}
	if got := ClassifyOriginalNetworkID(0x000C); got != NetworkClassAdvancedWidebandCS {
		t.Fatalf("0x000C = %v, want advanced wideband CS", got)
	}
	for _, onid := range []uint16{0x0004, 0x0006, 0x0007} {
		if !IsSatelliteOriginalNetworkID(onid) {
			t.Fatalf("%#04x should keep the legacy satellite predicate", onid)
		}
	}
	if IsSatelliteOriginalNetworkID(0x000B) || IsSatelliteOriginalNetworkID(0x000C) {
		t.Fatal("advanced networks must not match the satellite predicate")
	}
}
