package databroadcast

import "testing"

type stubExistenceStore struct {
	present map[ModuleCacheKey]bool
}

func (s stubExistenceStore) Has(key ModuleCacheKey) bool {
	return s.present[key]
}

func TestRestoreSnapshotNullsClockFields(t *testing.T) {
	snapshot := RestoreSnapshot("GR", "27", PersistedService{ServiceID: 101}, nil)
	if snapshot.ProgramInfo != nil || snapshot.CurrentTime != nil || snapshot.PCR != nil {
		t.Fatalf("provisional snapshot carried clock state: %#v", snapshot)
	}
	if snapshot.ServiceID != 101 {
		t.Fatalf("ServiceID = %d, want 101", snapshot.ServiceID)
	}
}

func TestRestoreSnapshotHandlesNilExistenceStore(t *testing.T) {
	// A nil ModuleExistenceStore must not panic; every module simply stays
	// in whatever state the replayed DII produced (announced, not complete).
	snapshot := RestoreSnapshot("GR", "27", PersistedService{ServiceID: 101}, nil)
	if snapshot.Revision != 0 {
		t.Fatalf("Revision = %d, want 0 for an empty replay", snapshot.Revision)
	}
}

func TestRestoreSnapshotMarksModuleCompleteFromExistenceOnly(t *testing.T) {
	const serviceID uint16 = 101
	const carouselPID uint16 = 0x0200
	const componentTag byte = 0x40
	const downloadID uint32 = 1
	const moduleID uint16 = 2
	const moduleVersion byte = 3
	const moduleSize uint32 = 4

	persisted := PersistedService{
		ServiceID:  serviceID,
		PMTSection: provisionalTestBuildPMT(serviceID, carouselPID, componentTag),
		Carousels: []PersistedCarousel{
			{ComponentTag: componentTag, PID: carouselPID, DIISection: provisionalTestBuildDII(downloadID, 4, moduleID, moduleSize, moduleVersion, nil)},
		},
	}
	key := ModuleCacheKey{ChannelType: "GR", ChannelID: "27", ServiceID: serviceID, ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: moduleVersion, Size: moduleSize}
	existence := stubExistenceStore{present: map[ModuleCacheKey]bool{key: true}}

	snapshot := RestoreSnapshot("GR", "27", persisted, existence)
	if len(snapshot.Components) != 1 || len(snapshot.Components[0].Modules) != 1 {
		t.Fatalf("components = %#v", snapshot.Components)
	}
	module := snapshot.Components[0].Modules[0]
	if !module.Complete || module.Status != "complete" || module.ReceivedBlocks != module.TotalBlocks {
		t.Fatalf("module = %#v, want complete", module)
	}

	// A module the existence store has never seen stays announced, not complete.
	snapshotNoHit := RestoreSnapshot("GR", "28", persisted, existence)
	moduleNoHit := snapshotNoHit.Components[0].Modules[0]
	if moduleNoHit.Complete || moduleNoHit.Status == "complete" {
		t.Fatalf("module = %#v, want incomplete for a different channel identity", moduleNoHit)
	}
}
