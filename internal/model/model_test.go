package model

import "testing"

func TestProgramIDSchemeUnchanged(t *testing.T) {
	key := ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 104}
	const eventID = 12345
	got := ProgramID(key, eventID)
	want := int64(4)*10000000000 + int64(104)*100000 + int64(eventID)
	if got != want {
		t.Fatalf("ProgramID = %d, want %d", got, want)
	}
	if key.MirakurunID() != int64(4)*100000+int64(104) {
		t.Fatalf("MirakurunID = %d", key.MirakurunID())
	}
}

func TestUndecidedTimesAreNilable(t *testing.T) {
	var event Event
	if event.StartAt != nil || event.DurationMS != nil {
		t.Fatal("zero Event must leave start/duration undecided (nil)")
	}
}
