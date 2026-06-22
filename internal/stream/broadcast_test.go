package stream

import (
	"bytes"
	"context"
	"io"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestBroadcastStopsSourceAfterLastSubscriberDetaches(t *testing.T) {
	source := newFakeLiveSource()
	broadcast := NewBroadcast(source, nil, nil)

	var first bytes.Buffer
	var second bytes.Buffer
	if err := broadcast.attach(&first); err != nil {
		t.Fatal(err)
	}
	if err := broadcast.attach(&second); err != nil {
		t.Fatal(err)
	}

	if got := source.starts(); got != 1 {
		t.Fatalf("source starts = %d, want 1", got)
	}

	broadcast.detach(&first)
	if got := source.stops(); got != 0 {
		t.Fatalf("source stops after first detach = %d, want 0", got)
	}

	broadcast.detach(&second)
	if got := source.stops(); got != 1 {
		t.Fatalf("source stops after last detach = %d, want 1", got)
	}
}

func TestBroadcastRunsAllStopCallbacks(t *testing.T) {
	source := newFakeLiveSource()
	var mu sync.Mutex
	var calls []string
	broadcast := NewBroadcast(source, nil, func() {
		mu.Lock()
		calls = append(calls, "initial")
		mu.Unlock()
	})
	if !broadcast.AddOnStop(func() {
		mu.Lock()
		calls = append(calls, "added")
		mu.Unlock()
	}) {
		t.Fatal("AddOnStop rejected callback before stop")
	}

	if err := broadcast.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broadcast.AddOnStop(func() {
		mu.Lock()
		calls = append(calls, "late")
		mu.Unlock()
	}) {
		t.Fatal("AddOnStop accepted callback after stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := calls, []string{"initial", "added"}; !slices.Equal(got, want) {
		t.Fatalf("stop callbacks = %v, want %v", got, want)
	}
}

func TestBroadcastTapDoesNotKeepSourceAlive(t *testing.T) {
	source := newFakeLiveSource()
	broadcast := NewBroadcast(source, nil, nil)

	var subscriber bytes.Buffer
	if err := broadcast.attach(&subscriber); err != nil {
		t.Fatal(err)
	}

	tapDone := make(chan error, 1)
	tapCtx, cancelTap := context.WithCancel(context.Background())
	defer cancelTap()
	go func() {
		tapDone <- broadcast.Tap(tapCtx, io.Discard)
	}()

	deadline := time.Now().Add(time.Second)
	for broadcast.hub.Count() < 2 {
		select {
		case err := <-tapDone:
			t.Fatalf("tap finished before attaching: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("tap did not attach")
		}
		time.Sleep(time.Millisecond)
	}

	broadcast.detach(&subscriber)
	if got := source.stops(); got != 1 {
		t.Fatalf("source stops after subscriber detach with tap = %d, want 1", got)
	}

	select {
	case err := <-tapDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("tap did not finish after source stopped")
	}
}

type fakeLiveSourceForBroadcast struct {
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	startsN   int
	stopsN    int
}

func newFakeLiveSource() *fakeLiveSourceForBroadcast {
	return &fakeLiveSourceForBroadcast{done: make(chan struct{})}
}

func (s *fakeLiveSourceForBroadcast) Start(context.Context, io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startsN++
	return nil
}

func (s *fakeLiveSourceForBroadcast) Stop(context.Context) error {
	s.mu.Lock()
	s.stopsN++
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

func (s *fakeLiveSourceForBroadcast) Done() <-chan struct{} {
	return s.done
}

func (s *fakeLiveSourceForBroadcast) Err() error {
	return nil
}

func (s *fakeLiveSourceForBroadcast) WithUser(context.Context, func() error) error {
	panic("not used")
}

func (s *fakeLiveSourceForBroadcast) starts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startsN
}

func (s *fakeLiveSourceForBroadcast) stops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopsN
}
