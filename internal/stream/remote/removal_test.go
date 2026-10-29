package remote

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
)

type removalUpdater struct {
	order []string
	fail  bool
}

func (u *removalUpdater) UpsertEvents(_ context.Context, events []model.Event) error {
	for _, e := range events {
		u.order = append(u.order, e.Name)
	}
	return nil
}
func (u *removalUpdater) RemovePrograms(context.Context, []int64) error {
	if u.fail {
		return errors.New("delete failed")
	}
	u.order = append(u.order, "remove")
	return nil
}
func TestRemoteRemovalPreservesOrderAndErrors(t *testing.T) {
	src := `[
{"resource":"program","type":"create","data":{"id":10000200001,"eventId":1,"networkId":1,"serviceId":2,"startAt":100,"duration":0,"name":"before","isFree":true}},
{"resource":"program","type":"remove","data":{"id":10000200001}},
{"resource":"program","type":"create","data":{"id":10000200001,"eventId":1,"networkId":1,"serviceId":2,"startAt":100,"duration":200,"name":"after","isFree":true}}
]`
	for _, batched := range []bool{false, true} {
		u := &removalUpdater{}
		var err error
		if batched {
			err = readRemoteEventsBatched(t.Context(), strings.NewReader(src), u, nil, time.Hour, 100)
		} else {
			err = readRemoteProgramEvents(t.Context(), strings.NewReader(src), u)
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(u.order, ",") != "before,remove,after" {
			t.Fatalf("order=%v", u.order)
		}
		u = &removalUpdater{fail: true}
		if err = readRemoteEventsBatched(t.Context(), strings.NewReader(src), u, nil, time.Hour, 100); err == nil {
			t.Fatal("removal error swallowed")
		}
	}
}
