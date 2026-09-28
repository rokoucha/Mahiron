package programstream

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestTrackerTracksTargetEvent(t *testing.T) {
	restoreTimings(t)
	eventEndGrace = 10 * time.Millisecond
	// Long enough that only the end of the event can close the stream.
	eventStaleAfter = time.Minute

	ctx, cancel := context.WithCancel(context.Background())
	g := newTracker(10, time.Second, cancel)
	g.observe(9)
	if g.isReady() {
		t.Fatal("tracker became ready for a different event")
	}
	g.observe(10)
	if !g.isReady() {
		t.Fatal("tracker did not become ready for the target event")
	}
	g.observe(11)
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("tracker did not close after the target event ended")
	}
}

func TestTrackerClosesWhenEventNeverAppears(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_ = newTracker(10, 10*time.Millisecond, cancel)
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("tracker did not close after its initial timeout")
	}
}

type fixedReader struct {
	r    io.Reader
	size int
}

func (f fixedReader) Next() ([]byte, error) {
	p := make([]byte, f.size)
	_, err := io.ReadFull(f.r, p)
	return p, err
}

func TestCopyPacketsDropsPacketsUntilEventIsOnAir(t *testing.T) {
	packet := bytes.Repeat([]byte{0x47}, 8)
	g := &tracker{}
	if err := copyPackets(fixedReader{bytes.NewReader(packet), 8}, io.Discard, g); err != nil {
		t.Fatal(err)
	}

	g.ready = true
	var out bytes.Buffer
	if err := copyPackets(fixedReader{bytes.NewReader(packet), 8}, &out, g); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), packet) {
		t.Fatalf("output length = %d, want %d", out.Len(), len(packet))
	}
}

func restoreTimings(t *testing.T) {
	t.Helper()
	endGrace, missingFallback, staleAfter, watchInterval := eventEndGrace, eventMissingFallback, eventStaleAfter, eventWatchInterval
	t.Cleanup(func() {
		eventEndGrace, eventMissingFallback, eventStaleAfter, eventWatchInterval = endGrace, missingFallback, staleAfter, watchInterval
	})
}
