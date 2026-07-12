package demux

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/ts"
)

func TestProgramGateTracksTargetEvent(t *testing.T) {
	restoreProgramTimings(t)
	programEventEndGrace = 10 * time.Millisecond
	programEventStaleAfter = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	gate := newProgramGate(1, 101, 10, time.Second, cancel)
	gate.observe(programEIT(1, 101, 9))
	if programGateReady(gate) {
		t.Fatal("gate became ready for a different event")
	}
	gate.observe(programEIT(1, 101, 10))
	if !programGateReady(gate) {
		t.Fatal("gate did not become ready for the target event")
	}
	gate.observe(programEIT(1, 101, 11))
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("gate did not close after the target event ended")
	}
}

func TestProgramGateClosesWhenEventNeverAppears(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_ = newProgramGate(1, 101, 10, 10*time.Millisecond, cancel)
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("gate did not close after its initial timeout")
	}
}

func TestCopyProgramDropsPacketsUntilGateIsReady(t *testing.T) {
	packet := bytes.Repeat([]byte{0x47}, ts.PacketSize)
	gate := &programGate{}
	if err := copyProgram(bytes.NewReader(packet), ioDiscard{}, gate); err != nil {
		t.Fatal(err)
	}

	gate.ready = true
	var out bytes.Buffer
	if err := copyProgram(bytes.NewReader(packet), &out, gate); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), packet) {
		t.Fatalf("output length = %d, want %d", out.Len(), len(packet))
	}
}

func TestSubscribeProgramSharesReceiverAndServiceSource(t *testing.T) {
	var starts atomic.Int32
	d := New(func(context.Context, io.Writer) error {
		starts.Add(1)
		return nil
	}, nil)

	err := d.SubscribeProgram(t.Context(), d, &program.Program{
		NetworkID: 1,
		ServiceID: 101,
		EventID:   10,
		StartAt:   time.Now().UnixMilli(),
		Duration:  1000,
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

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func programGateReady(g *programGate) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ready
}

func restoreProgramTimings(t *testing.T) {
	t.Helper()
	endGrace := programEventEndGrace
	missingFallback := programEventMissingFallback
	staleAfter := programEventStaleAfter
	watchInterval := programEventWatchInterval
	t.Cleanup(func() {
		programEventEndGrace = endGrace
		programEventMissingFallback = missingFallback
		programEventStaleAfter = staleAfter
		programEventWatchInterval = watchInterval
	})
}

func programEIT(networkID, serviceID, eventID uint16) *ts.EIT {
	return &ts.EIT{
		OriginalNetworkID: networkID,
		ServiceID:         serviceID,
		TableID:           ts.TableIDEITPF0,
		SectionNumber:     0,
		Events:            []ts.EITEvent{{EventID: eventID}},
	}
}
