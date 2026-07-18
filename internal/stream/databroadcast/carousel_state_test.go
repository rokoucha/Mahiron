package databroadcast

import "testing"

func TestSnapshotDefaultsCarouselToWaitingForDII(t *testing.T) {
	hub := NewDataBroadcastHub()
	hub.mu.Lock()
	service := hub.serviceLocked(101)
	service.pmt = &DataBroadcastPMT{Components: []DataBroadcastComponent{{ComponentTag: 0x40, CarouselStatus: "waitingForDii"}}}
	snapshot := hub.snapshotLocked(101)
	hub.mu.Unlock()
	if got := snapshot.Components[0].CarouselStatus; got != "waitingForDii" {
		t.Fatalf("carousel status = %q, want waitingForDii", got)
	}
}

func TestSnapshotIncludesCurrentCarouselState(t *testing.T) {
	hub := NewDataBroadcastHub()
	hub.mu.Lock()
	service := hub.serviceLocked(101)
	service.pmt = &DataBroadcastPMT{Components: []DataBroadcastComponent{{ComponentTag: 0x40}}}
	service.carouselStates[0x40] = dataBroadcastCarouselState{status: "empty", downloadID: 0x10000001, blockSize: 4066}
	snapshot := hub.snapshotLocked(101)
	hub.mu.Unlock()
	component := snapshot.Components[0]
	if component.CarouselStatus != "empty" || component.CarouselDownloadID == nil || *component.CarouselDownloadID != 0x10000001 || component.CarouselBlockSize == nil || *component.CarouselBlockSize != 4066 {
		t.Fatalf("carousel state = %#v", component)
	}
}
