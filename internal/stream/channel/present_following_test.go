package channel

import (
	"context"
	"errors"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
	"testing"
)

type snapshotUpdater struct {
	snapshots []model.PresentFollowingSnapshot
	fail      bool
}

func (u *snapshotUpdater) UpsertEvents(context.Context, []model.Event) error {
	return errors.New("unpaired p/f write")
}
func (u *snapshotUpdater) UpsertPresentFollowing(_ context.Context, snapshot model.PresentFollowingSnapshot) error {
	if u.fail {
		return errors.New("test failure")
	}
	u.snapshots = append(u.snapshots, snapshot)
	return nil
}

func TestSectionUpdatesCompletePresentFollowingAndRetry(t *testing.T) {
	u := &snapshotUpdater{}
	s := &Session{eitUpdater: u, sectionQueue: make(chan ts.Section, sectionQueueSize)}
	key := epgClockTestKey{networkID: 32736, serviceID: 1024}
	p := streamBuildEIT(ts.TableIDEITPF0, key, 6473)
	p[7] = 1
	p[12] = 1
	for i := 21; i < 24; i++ {
		p[i] = 0xff
	}
	streamWriteCRC(p)
	f := append(ts.Section(nil), p...)
	f[6] = 1
	// Undefined following has no meaningful ID; it must not become a program.
	for i := 16; i < 24; i++ {
		f[i] = 0xff
	}
	streamWriteCRC(f)
	s.observeSection(p)
	s.updateSection(t.Context(), <-s.sectionQueue)
	if len(u.snapshots) != 0 {
		t.Fatal("published incomplete p/f")
	}
	u.fail = true
	s.observeSection(f)
	s.updateSection(t.Context(), <-s.sectionQueue)
	u.fail = false
	s.observeSection(f)
	if len(s.sectionQueue) != 1 {
		t.Fatal("failed complete snapshot was not retried")
	}
	s.updateSection(t.Context(), <-s.sectionQueue)
	if len(u.snapshots) != 1 || len(u.snapshots[0].Events) != 1 || u.snapshots[0].Events[0].DurationMS != nil || !u.snapshots[0].Events[0].EITPresent {
		t.Fatalf("snapshot=%+v", u.snapshots)
	}
	other := append(ts.Section(nil), p...)
	other[0] = ts.TableIDEITPF1
	streamWriteCRC(other)
	s.updateSection(t.Context(), other)
	if len(u.snapshots) != 1 {
		t.Fatal("other-TS p/f replaced actual snapshot")
	}
}
