// Package programstream cuts a service stream down to one program, by
// following present/following (EIT or MH-EIT) and passing packets on only
// while the requested event is on air. Reading the event ID and packet
// framing differ per system, so both are plugged in.
package programstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/internal/util"
)

var (
	eventEndGrace        = time.Second
	eventMissingFallback = 3 * time.Minute
	eventStaleAfter      = 10 * time.Second
	eventWatchInterval   = 3 * time.Second
)

// Observer reports the present event's ID through present until ctx ends;
// attached closes once registered, so no signaling is missed after it.
type Observer func(ctx context.Context, present func(eventID uint16), attached chan<- struct{}) error

// Run writes the service stream to dst while event is on air: observe reads
// the present event, stream feeds it, and newReader splits it into packets.
func Run(ctx context.Context, event model.Event, dst io.Writer, observe Observer, stream func(context.Context, io.Writer) error, newReader func(io.Reader) fanout.PacketReader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var startAt int64
	var duration int
	if event.StartAt != nil {
		startAt = *event.StartAt
	}
	if event.DurationMS != nil {
		duration = *event.DurationMS
	}
	tracker := newTracker(event.EventID, timeout(startAt, duration), cancel)
	attached := make(chan struct{})
	observeDone := make(chan error, 1)
	go func() { observeDone <- observe(ctx, tracker.observe, attached) }()
	select {
	case <-attached:
	case err := <-observeDone:
		return expectedClose(err)
	case <-ctx.Done():
		return expectedClose(ctx.Err())
	}

	r, w := io.Pipe()
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream(ctx, w)
		_ = w.Close()
	}()
	err := copyPackets(newReader(r), dst, tracker)
	_ = r.Close()
	cancel()
	return errors.Join(expectedClose(err), expectedClose(<-streamDone), expectedClose(<-observeDone))
}

type tracker struct {
	cancel         context.CancelFunc
	eventID        uint16
	lastDetectedAt time.Time
	mu             sync.RWMutex
	ready          bool
	stopTimer      *time.Timer
}

func newTracker(eventID uint16, timeout time.Duration, cancel context.CancelFunc) *tracker {
	if timeout <= 0 {
		timeout = eventMissingFallback
	}
	g := &tracker{cancel: cancel, eventID: eventID}
	g.stopTimer = time.AfterFunc(timeout, g.closeIfStale)
	return g
}

func (g *tracker) observe(presentEventID uint16) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if presentEventID == g.eventID {
		g.ready = true
		g.lastDetectedAt = time.Now()
		g.stopTimer.Reset(eventStaleAfter)
	} else if g.ready {
		// The event ended. Forget the last detection so the grace timer
		// closes the stream instead of waiting out eventStaleAfter.
		g.lastDetectedAt = time.Time{}
		g.stopTimer.Reset(eventEndGrace)
	}
}

func (g *tracker) closeIfStale() {
	g.mu.RLock()
	last := g.lastDetectedAt
	g.mu.RUnlock()
	if !last.IsZero() && time.Since(last) < eventStaleAfter {
		g.stopTimer.Reset(eventWatchInterval)
		return
	}
	g.cancel()
}

func (g *tracker) isReady() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ready
}

func copyPackets(src fanout.PacketReader, dst io.Writer, g *tracker) error {
	for {
		packet, err := src.Next()
		if err != nil {
			return expectedClose(err)
		}
		if !g.isReady() {
			continue
		}
		if n, err := dst.Write(packet); err != nil {
			return err
		} else if n != len(packet) {
			return io.ErrShortWrite
		}
	}
}

func timeout(startAt int64, duration int) time.Duration {
	timeout := time.Until(time.UnixMilli(startAt + int64(duration)))
	if duration == 1 {
		timeout += eventMissingFallback
	}
	if timeout < 0 {
		return eventMissingFallback
	}
	return timeout
}

func expectedClose(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || util.IsExpectedStreamCloseError(err) {
		return nil
	}
	return err
}
