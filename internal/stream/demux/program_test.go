package demux

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
)

func TestSubscribeProgramSharesReceiverAndServiceSource(t *testing.T) {
	var starts atomic.Int32
	d := New(func(context.Context, io.Writer) error {
		starts.Add(1)
		return nil
	}, nil)

	startAt, duration := time.Now().UnixMilli(), 1000
	err := d.SubscribeProgram(t.Context(), d, model.Event{
		Key:        model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID:    10,
		StartAt:    &startAt,
		DurationMS: &duration,
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 1 {
		t.Fatalf("source starts = %d, want 1", starts.Load())
	}
	if !streamtest.Eventually(time.Second, d.Stopped) {
		t.Fatal("demuxer did not stop after the program subscription ended")
	}
}
