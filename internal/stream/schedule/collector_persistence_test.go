package schedule

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
)

func snapshotOf(update model.ScheduleUpdate) model.ScheduleSnapshot {
	return model.ScheduleSnapshot{Service: update.Service, Revisions: update.Revisions(), Events: update.Events()}
}

func TestBufferedOldActualAndOtherCannotRollBackNewActual(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	programs := program.NewManager(program.NewSQLiteStore(database))
	actual := NewCollector(isdb.ScheduleTS, "actual input")
	old := snapshotOf(observe(t, actual, section(0x50, 0, 0, 31, event(1, "old actual"))))
	updated := event(1, "new actual")
	start := int64(2000)
	updated.StartAt = &start
	updated.Extended = []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "details", Text: "new details"}}}}
	newest := snapshotOf(observe(t, actual, section(0x50, 0, 0, 0, updated)))
	other := snapshotOf(observe(t, NewCollector(isdb.ScheduleTS, "other input"), section(0x60, 0, 0, 30, event(1, "delayed other"))))
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-ready
		if err := programs.UpsertSchedule(t.Context(), other); err != nil {
			done <- err
			return
		}
		done <- programs.UpsertSchedule(t.Context(), old)
	}()
	if err := programs.UpsertSchedule(t.Context(), newest); err != nil {
		close(ready)
		if drainErr := <-done; drainErr != nil {
			t.Error(drainErr)
		}
		t.Fatal(err)
	}
	close(ready)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, ok, err := programs.Get(t.Context(), model.ProgramID(testService, 1))
	if err != nil || !ok || got.Name != "new actual" || got.StartAtOrZero() != 2000 || len(got.Extended) != 1 {
		t.Fatalf("buffered snapshots rolled back program: %#v %v", got, err)
	}
}
