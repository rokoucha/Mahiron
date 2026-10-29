package tlv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/mmt"
)

type snapshotUpdater struct {
	snapshots []model.PresentFollowingSnapshot
}

func (u *snapshotUpdater) UpsertEvents(context.Context, []model.Event) error {
	return errors.New("unpaired MH-EIT write")
}
func (u *snapshotUpdater) UpsertPresentFollowing(_ context.Context, snapshot model.PresentFollowingSnapshot) error {
	u.snapshots = append(u.snapshots, snapshot)
	return nil
}

func TestMHEITPresentFollowingWaitsForBothSections(t *testing.T) {
	u := &snapshotUpdater{}
	s := &Session{eventUpdater: u}
	eit := &mmt.MHEIT{SectionHeader: mmt.SectionHeader{TableID: mmt.TableIDMHEITPF, VersionNumber: 1, LastSectionNumber: 1}, OriginalNetworkID: 11, TLVStreamID: 1, ServiceID: 101, Events: []mmt.MHEITEvent{{EventID: 1, StartTime: time.Unix(100, 0)}}}
	if err := s.updatePresentFollowing(t.Context(), eit); err != nil {
		t.Fatal(err)
	}
	if len(u.snapshots) != 0 {
		t.Fatal("published incomplete snapshot")
	}
	eit.SectionNumber = 1
	eit.Events = nil
	if err := s.updatePresentFollowing(t.Context(), eit); err != nil {
		t.Fatal(err)
	}
	if len(u.snapshots) != 1 || len(u.snapshots[0].Events) != 1 || !u.snapshots[0].Events[0].EITPresent || u.snapshots[0].Events[0].DurationMS != nil {
		t.Fatalf("snapshot=%+v", u.snapshots)
	}
	eit.VersionNumber = 2
	eit.SectionNumber = 0
	if err := s.updatePresentFollowing(t.Context(), eit); err != nil {
		t.Fatal(err)
	}
	eit.SectionNumber = 1
	if err := s.updatePresentFollowing(t.Context(), eit); err != nil {
		t.Fatal(err)
	}
	if len(u.snapshots) != 2 || len(u.snapshots[1].Events) != 0 {
		t.Fatalf("empty snapshot=%+v", u.snapshots)
	}
}
