package demux

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/ts"
)

var (
	programEventEndGrace        = time.Second
	programEventMissingFallback = 3 * time.Minute
	programEventStaleAfter      = 10 * time.Second
	programEventWatchInterval   = 3 * time.Second
)

// SubscribeProgram filters a service stream to the requested event, using EIT
// present/following sections observed on the receiver demuxer.
func (e *Demuxer) SubscribeProgram(ctx context.Context, stream *Demuxer, p *program.Program, dst io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newProgramGate(p.NetworkID, p.ServiceID, p.EventID, programTimeout(p.StartAt, p.Duration), cancel)
	attached := make(chan struct{})
	observeDone := make(chan error, 1)
	go func() {
		observeDone <- e.ObserveSectionsPassive(ctx, func(section ts.Section) bool { return ts.IsEITPF(section.TableID()) }, func(section ts.Section) error {
			if eit, err := ts.ParseEIT(section); err == nil {
				gate.observe(eit)
			}
			return nil
		}, attached)
	}()
	select {
	case <-attached:
	case err := <-observeDone:
		return expectedProgramClose(err)
	case <-ctx.Done():
		return expectedProgramClose(ctx.Err())
	}

	r, w := io.Pipe()
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.SubscribeService(ctx, p.ServiceID, w)
		_ = w.Close()
	}()
	err := copyProgram(r, dst, gate)
	_ = r.Close()
	cancel()
	return errors.Join(expectedProgramClose(err), expectedProgramClose(<-streamDone), expectedProgramClose(<-observeDone))
}

type programGate struct {
	cancel                        context.CancelFunc
	eventID, networkID, serviceID uint16
	lastDetectedAt                time.Time
	mu                            sync.RWMutex
	ready                         bool
	stopTimer                     *time.Timer
}

func newProgramGate(networkID, serviceID, eventID uint16, timeout time.Duration, cancel context.CancelFunc) *programGate {
	if timeout <= 0 {
		timeout = programEventMissingFallback
	}
	g := &programGate{cancel: cancel, eventID: eventID, networkID: networkID, serviceID: serviceID}
	g.stopTimer = time.AfterFunc(timeout, g.closeIfStale)
	return g
}

func (g *programGate) observe(eit *ts.EIT) {
	if eit == nil || eit.TableID != ts.TableIDEITPF0 || eit.SectionNumber != 0 || eit.ServiceID != g.serviceID || eit.OriginalNetworkID != g.networkID || len(eit.Events) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if eit.Events[0].EventID == g.eventID {
		g.ready = true
		g.lastDetectedAt = time.Now()
		g.stopTimer.Reset(programEventStaleAfter)
	} else if g.ready {
		g.stopTimer.Reset(programEventEndGrace)
	}
}

func (g *programGate) closeIfStale() {
	g.mu.RLock()
	last := g.lastDetectedAt
	g.mu.RUnlock()
	if !last.IsZero() && time.Since(last) < programEventStaleAfter {
		g.stopTimer.Reset(programEventWatchInterval)
		return
	}
	g.cancel()
}

func copyProgram(src io.Reader, dst io.Writer, gate *programGate) error {
	packet := make([]byte, ts.PacketSize)
	for {
		if _, err := io.ReadFull(src, packet); err != nil {
			return expectedProgramClose(err)
		}
		gate.mu.RLock()
		ready := gate.ready
		gate.mu.RUnlock()
		if ready {
			if n, err := dst.Write(packet); err != nil {
				return err
			} else if n != len(packet) {
				return io.ErrShortWrite
			}
		}
	}
}

func programTimeout(startAt int64, duration int) time.Duration {
	timeout := time.Until(time.UnixMilli(startAt + int64(duration)))
	if duration == 1 {
		timeout += programEventMissingFallback
	}
	if timeout < 0 {
		return programEventMissingFallback
	}
	return timeout
}

func expectedProgramClose(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || util.IsExpectedStreamCloseError(err) {
		return nil
	}
	return err
}
