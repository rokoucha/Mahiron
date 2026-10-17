package program

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
)

func revisionEvent(id uint16, name string, start int64, tableID, version uint8, reception uint64) model.Event {
	duration := 1000
	return model.Event{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 151}, EventID: id, Name: name, StartAt: &start, DurationMS: &duration, EITRevision: &model.EITRevision{Source: "source-A", TableID: tableID, Version: version, Reception: reception}}
}

func TestDelayedScheduleSnapshotsCannotRollBackPrograms(t *testing.T) {
	for _, tc := range []struct {
		name         string
		newer, older model.Event
	}{
		{"same subtable", revisionEvent(1, "new", 2000, 0x50, 5, 20), revisionEvent(1, "old", 1000, 0x50, 4, 10)},
		{"version wrap", revisionEvent(1, "new", 2000, 0x50, 0, 20), revisionEvent(1, "old", 1000, 0x50, 31, 10)},
		{"version jump", revisionEvent(1, "new", 2000, 0x50, 25, 20), revisionEvent(1, "old", 1000, 0x50, 2, 10)},
		{"independent actual other versions", revisionEvent(1, "new", 2000, 0x50, 2, 10), revisionEvent(1, "old", 1000, 0x60, 30, 20)},
		{"newer other reception is not authoritative", revisionEvent(1, "new", 2000, 0x50, 31, 10), revisionEvent(1, "old", 1000, 0x60, 0, 20)},
		{"same source other update", revisionEvent(1, "new", 2000, 0x60, 7, 20), revisionEvent(1, "old", 1000, 0x60, 6, 10)},
		{"same version after full cycle", revisionEvent(1, "new", 2000, 0x50, 2, 100), revisionEvent(1, "old", 1000, 0x50, 2, 10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			tc.newer.Extended = []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "出演者", Text: "latest details"}}}}
			if err := m.UpsertEvents(t.Context(), []model.Event{tc.newer}); err != nil {
				t.Fatal(err)
			}
			neighbour := revisionEvent(2, "neighbour", 1000, 0x50, tc.newer.EITRevision.Version, tc.newer.EITRevision.Reception)
			if err := m.UpsertEvents(t.Context(), []model.Event{neighbour}); err != nil {
				t.Fatal(err)
			}
			if err := m.UpsertEvents(t.Context(), []model.Event{tc.older}); err != nil {
				t.Fatal(err)
			}
			stored, ok, err := m.Get(t.Context(), model.ProgramID(tc.newer.Key, 1))
			if err != nil || !ok {
				t.Fatalf("program missing: %v", err)
			}
			if stored.Name != "new" || stored.StartAtOrZero() != 2000 || len(stored.Extended) == 0 {
				t.Fatalf("rolled back: %#v", stored)
			}
			if _, ok, err := m.Get(t.Context(), model.ProgramID(tc.newer.Key, 2)); err != nil || !ok {
				t.Fatalf("stale snapshot deleted neighbour: %v", err)
			}
		})
	}
}

func TestOtherScheduleCannotDeleteOverlappingActualProgram(t *testing.T) {
	m := newTestManager(t)
	actual := revisionEvent(1, "actual", 1000, 0x50, 2, 10)
	other := revisionEvent(2, "obsolete", 1000, 0x60, 25, 20)
	if err := m.UpsertEvents(t.Context(), []model.Event{actual}); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertEvents(t.Context(), []model.Event{other}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(actual.Key, 1)); err != nil || !ok {
		t.Fatalf("actual program deleted: %v", err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(other.Key, 2)); err != nil || ok {
		t.Fatalf("conflicting other program stored: %v", err)
	}
}

func TestOldTableCannotResurrectRemovedEvent(t *testing.T) {
	m := newTestManager(t)
	newest := revisionEvent(2, "replacement", 1000, 0x50, 3, 20)
	delayed := revisionEvent(1, "removed", 1000, 0x50, 2, 10)
	if err := m.UpsertEvents(t.Context(), []model.Event{newest}); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertEvents(t.Context(), []model.Event{delayed}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(delayed.Key, 1)); err != nil || ok {
		t.Fatalf("removed event resurrected: %v", err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(newest.Key, 2)); err != nil || !ok {
		t.Fatalf("replacement deleted: %v", err)
	}
}

func TestActualScheduleReplacesOtherAndFreshVersionsAdvance(t *testing.T) {
	m := newTestManager(t)
	other := revisionEvent(1, "other", 1000, 0x60, 30, 10)
	actual := revisionEvent(1, "actual", 2000, 0x50, 2, 20)
	next := revisionEvent(1, "next", 3000, 0x50, 31, 30)
	wrapped := revisionEvent(1, "wrapped", 4000, 0x50, 0, 40)
	for _, event := range []model.Event{other, actual, next, wrapped} {
		if err := m.UpsertEvents(t.Context(), []model.Event{event}); err != nil {
			t.Fatal(err)
		}
		got, ok, err := m.Get(t.Context(), model.ProgramID(event.Key, 1))
		if err != nil || !ok || got.Name != event.Name || got.StartAtOrZero() != *event.StartAt {
			t.Fatalf("update lost: %#v, %v", got, err)
		}
	}
}

func TestEmptyNewVersionRetiresDelayedOldEvents(t *testing.T) {
	m := newTestManager(t)
	delayed := revisionEvent(1, "removed", 1000, 0x50, 31, 10)
	newer := *delayed.EITRevision
	newer.Version = 0
	newer.Reception = 20
	if err := m.UpsertSchedule(t.Context(), model.ScheduleSnapshot{Service: delayed.Key, Revisions: []model.EITRevision{newer}}); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertEvents(t.Context(), []model.Event{delayed}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(delayed.Key, 1)); err != nil || ok {
		t.Fatalf("empty new version resurrected old event: %v", err)
	}
}

func TestBatchRejectsOldVersionBeforeOverlapDeletion(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		m := newTestManager(t)
		old := revisionEvent(1, "old", 1000, 0x50, 1, 10)
		new := revisionEvent(2, "new", 1000, 0x50, 2, 20)
		batch := []model.Event{old, new}
		if reverse {
			batch = []model.Event{new, old}
		}
		if err := m.UpsertEvents(t.Context(), batch); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := m.Get(t.Context(), model.ProgramID(old.Key, 1)); err != nil || ok {
			t.Fatalf("old version survived batch: %v", err)
		}
		if _, ok, err := m.Get(t.Context(), model.ProgramID(new.Key, 2)); err != nil || !ok {
			t.Fatalf("new version lost: %v", err)
		}
	}
}

func TestActualProvenanceSurvivesManagerRestart(t *testing.T) {
	original := newTestManager(t)
	actual := revisionEvent(1, "actual", 2000, 0x50, 2, 10)
	if err := original.UpsertEvents(t.Context(), []model.Event{actual}); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(original.store)
	other := revisionEvent(1, "delayed other", 1000, 0x60, 30, 100)
	if err := restarted.UpsertEvents(t.Context(), []model.Event{other}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := restarted.Get(t.Context(), model.ProgramID(actual.Key, 1))
	if err != nil || !ok || got.Name != "actual" || got.StartAtOrZero() != 2000 {
		t.Fatalf("restart lost actual provenance: %#v %v", got, err)
	}
	if got.EITRevision == nil || got.EITRevision.Version != 2 || got.EITRevision.Other() || got.EITRevision.Reception != 0 {
		t.Fatalf("wrong stored provenance: %#v", got.EITRevision)
	}
}

func TestOtherCanAddProgramsAndFillMissingActualDetails(t *testing.T) {
	m := newTestManager(t)
	actual := revisionEvent(1, "actual", 2000, 0x50, 2, 10)
	other := revisionEvent(1, "old title", 1000, 0x60, 30, 20)
	other.Extended = []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "details", Text: "supplement"}}}}
	if err := m.UpsertEvents(t.Context(), []model.Event{actual}); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertEvents(t.Context(), []model.Event{other}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.Get(t.Context(), model.ProgramID(actual.Key, 1))
	if err != nil || !ok || got.Name != "actual" || got.StartAtOrZero() != 2000 || len(got.Extended) != 1 {
		t.Fatalf("supplement failed: %#v %v", got, err)
	}
	added := revisionEvent(2, "new other event", 4000, 0x60, 30, 20)
	if err := m.UpsertEvents(t.Context(), []model.Event{added}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(added.Key, 2)); err != nil || !ok {
		t.Fatalf("other event was not added: %v", err)
	}
}

func TestOtherSupplementNeverClaimsLegacyProvenance(t *testing.T) {
	m := newTestManager(t)
	legacy := revisionEvent(1, "legacy", 1000, 0x50, 0, 0)
	legacy.EITRevision = nil
	if err := m.UpsertEvents(t.Context(), []model.Event{legacy}); err != nil {
		t.Fatal(err)
	}
	for _, restart := range []bool{false, true} {
		if restart {
			m = NewManager(m.store)
		}
		other := revisionEvent(1, "other must not win", 2000, 0x60, 2, 10)
		other.Language = "jpn"
		if err := m.UpsertEvents(t.Context(), []model.Event{other}); err != nil {
			t.Fatal(err)
		}
		other.EITRevision.Version++
		other.EITRevision.Reception++
		if err := m.UpsertEvents(t.Context(), []model.Event{other}); err != nil {
			t.Fatal(err)
		}
		got, ok, err := m.Get(t.Context(), model.ProgramID(legacy.Key, 1))
		if err != nil || !ok || got.Name != "legacy" || got.StartAtOrZero() != 1000 || got.Language != "jpn" || got.EITRevision != nil {
			t.Fatalf("other claimed legacy row: %#v %v", got, err)
		}
	}
}

func TestRejectedOverlapCannotDeleteEarlierCandidates(t *testing.T) {
	for range 32 {
		m := newTestManager(t)
		older := revisionEvent(1, "other neighbour", 1000, 0x60, 1, 5)
		protected := revisionEvent(2, "actual neighbour", 3000, 0x50, 1, 20)
		candidate := revisionEvent(3, "blocked other", 1000, 0x60, 1, 10)
		duration := 3000
		candidate.DurationMS = &duration
		for _, event := range []model.Event{older, protected, candidate} {
			if err := m.UpsertEvents(t.Context(), []model.Event{event}); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range []uint16{1, 2} {
			if _, ok, err := m.Get(t.Context(), model.ProgramID(older.Key, id)); err != nil || !ok {
				t.Fatalf("blocked candidate deleted neighbour %d: %v", id, err)
			}
		}
		if _, ok, err := m.Get(t.Context(), model.ProgramID(older.Key, 3)); err != nil || ok {
			t.Fatalf("blocked candidate persisted: %v", err)
		}
	}
}

func TestOtherCannotDeleteLegacyOrClaimRejectedEvent(t *testing.T) {
	m := newTestManager(t)
	legacy := revisionEvent(1, "legacy", 1000, 0x50, 0, 0)
	legacy.EITRevision = nil
	other := revisionEvent(2, "other", 1000, 0x60, 2, 10)
	if err := m.UpsertEvents(t.Context(), []model.Event{legacy, other}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(legacy.Key, 1)); err != nil || !ok {
		t.Fatalf("legacy neighbour deleted: %v", err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(other.Key, 2)); err != nil || ok {
		t.Fatalf("conflicting other persisted: %v", err)
	}
	if _, ok := m.eit[model.ProgramID(other.Key, 2)]; ok {
		t.Fatal("rejected event acquired provenance")
	}
}

func TestDelayedExtendedVersionCannotRollBackDetails(t *testing.T) {
	m := newTestManager(t)
	base := revisionEvent(1, "basic", 1000, 0x50, 1, 5)
	newest := base
	newest.EITExtendedRevision = &model.EITRevision{Source: "source-A", TableID: 0x58, Version: 0, Reception: 20}
	newest.Extended = []model.ExtendedBlock{{Language: "jpn", Body: "new details"}}
	old := base
	old.EITExtendedRevision = &model.EITRevision{Source: "source-A", TableID: 0x58, Version: 31, Reception: 10}
	old.Extended = []model.ExtendedBlock{{Language: "jpn", Body: "old details"}}
	if err := m.UpsertEvents(t.Context(), []model.Event{newest, old}); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertEvents(t.Context(), []model.Event{old}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := m.Get(t.Context(), model.ProgramID(base.Key, 1))
	if err != nil || !ok || len(got.Extended) != 1 || got.Extended[0].Body != "new details" {
		t.Fatalf("extended details rolled back: %#v %v", got, err)
	}
}

func TestBlockedMoveKeepsPreviouslyStoredProgram(t *testing.T) {
	m := newTestManager(t)
	original := revisionEvent(1, "original other", 1000, 0x60, 1, 5)
	actual := revisionEvent(2, "actual", 3000, 0x50, 2, 20)
	moved := revisionEvent(1, "blocked move", 3000, 0x60, 2, 10)
	for _, event := range []model.Event{original, actual, moved} {
		if err := m.UpsertEvents(t.Context(), []model.Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := m.Get(t.Context(), model.ProgramID(original.Key, 1))
	if err != nil || !ok || got.Name != "original other" || got.StartAtOrZero() != 1000 {
		t.Fatalf("blocked move deleted original: %#v %v", got, err)
	}
	if _, ok, err := m.Get(t.Context(), model.ProgramID(actual.Key, 2)); err != nil || !ok {
		t.Fatalf("actual neighbour deleted: %v", err)
	}
}
