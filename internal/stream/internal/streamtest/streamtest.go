// Package streamtest provides test helpers shared by the packages under
// internal/stream. It must only be imported from test files.
package streamtest

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
)

// RecordingProgramUpdater collects every event passed to UpsertEvents.
type RecordingProgramUpdater struct {
	mu     sync.Mutex
	events []model.Event
}

func (u *RecordingProgramUpdater) UpsertEvents(_ context.Context, events []model.Event) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.events = append(u.events, events...)
	return nil
}

// Events returns a snapshot of the collected events.
func (u *RecordingProgramUpdater) Events() []model.Event {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]model.Event(nil), u.events...)
}

// RoundTripFunc adapts a function into an http.RoundTripper for stubbing
// upstream HTTP servers in tests.
type RoundTripFunc func(*http.Request) (*http.Response, error)

func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// StringResponse builds an *http.Response with the given status code and body.
func StringResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{},
	}
}

// Eventually polls ok until it reports true or the timeout elapses.
func Eventually(timeout time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return ok()
}

// TestPacket builds a single valid TS packet with the given PID and
// continuity counter, padded with 0xff.
func TestPacket(pid uint16, counter byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, ts.PacketSize)
	packet[0] = ts.SyncByte
	packet[1] = byte(pid >> 8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | counter&0x0f
	return packet
}

// ClosedStart returns an already-closed channel, for sources that should
// start emitting immediately.
func ClosedStart() <-chan struct{} {
	start := make(chan struct{})
	close(start)
	return start
}

// FinitePacketSource is a LiveSource that emits a fixed byte sequence once
// the start channel is closed, then reports completion via Done.
type FinitePacketSource struct {
	data  []byte
	done  chan struct{}
	err   error
	start <-chan struct{}
}

func NewFinitePacketSource(data []byte, start <-chan struct{}) *FinitePacketSource {
	return &FinitePacketSource{data: data, done: make(chan struct{}), start: start}
}

func (s *FinitePacketSource) Start(_ context.Context, dst io.Writer) error {
	go func() {
		<-s.start
		_, s.err = dst.Write(s.data)
		close(s.done)
	}()
	return nil
}

func (*FinitePacketSource) Stop(context.Context) error { return nil }
func (s *FinitePacketSource) Done() <-chan struct{}    { return s.done }
func (s *FinitePacketSource) Err() error               { return s.err }
func (*FinitePacketSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}
