package model

// EITRevision identifies the schedule subtable that supplied an event.
// Reception orders buffered writes within this process; it is not a timestamp
// or a version counter shared by independently operated actual/other tables.
type EITRevision struct {
	Source    string
	TableID   uint8
	Version   uint8
	Reception uint64
}

func (r EITRevision) Other() bool { return r.TableID >= 0x60 && r.TableID <= 0x6F }

// PreferTo compares reception order only within one source. Actual EIT is
// authoritative over other EIT (TR-B15 6.1.1-6.1.2); their versions are unrelated.
func (r EITRevision) PreferTo(previous EITRevision) bool {
	if r.Other() != previous.Other() {
		return !r.Other()
	}
	if r.Other() && r.Source != previous.Source {
		return false
	}
	return r.Reception >= previous.Reception
}

// ScheduleSnapshot includes empty sections, whose newer versions must also
// retire buffered events from an older subtable version.
type ScheduleSnapshot struct {
	Service   ServiceKey
	Revisions []EITRevision
	Events    []Event
}

// PresentFollowingSnapshot contains both sections of one p/f subtable version.
// Empty sections are significant: absence from both slots ends an event.
type PresentFollowingSnapshot struct {
	Service ServiceKey
	Events  []Event
}
