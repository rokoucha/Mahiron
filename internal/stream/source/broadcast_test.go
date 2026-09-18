package source

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestBroadcastStopsSourceAfterLastSubscriberDetaches(t *testing.T) {
	source := newFakeLiveSource()
	broadcast := NewBroadcast(source, nil)

	var first bytes.Buffer
	var second bytes.Buffer
	if err := broadcast.attach(context.Background(), &first); err != nil {
		t.Fatal(err)
	}
	if err := broadcast.attach(context.Background(), &second); err != nil {
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
	broadcast := NewBroadcast(source, func() {
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

type fakeLiveSourceForBroadcast struct {
	ctx       context.Context
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	startsN   int
	stopsN    int
}

func newFakeLiveSource() *fakeLiveSourceForBroadcast {
	return &fakeLiveSourceForBroadcast{done: make(chan struct{})}
}

func (s *fakeLiveSourceForBroadcast) Start(ctx context.Context, _ io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = ctx
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

func (s *fakeLiveSourceForBroadcast) WithUser(context.Context, func(context.Context) error) error {
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

func TestDetachDoesNotLogExpectedClosedFileStopError(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	done := make(chan struct{})
	close(done)
	broadcast := NewBroadcast(&tunerLiveSource{
		channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
		device: fakeStopErrorDevice{
			done:    done,
			stopErr: &os.PathError{Op: "read", Path: "|0", Err: os.ErrClosed},
		},
	}, nil)

	var dst bytes.Buffer
	if err := broadcast.attach(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	broadcast.detach(&dst)

	if strings.Contains(logs.String(), "failed to stop broadcast") {
		t.Fatalf("unexpected stop error log: %s", logs.String())
	}
}

type fakeStopErrorDevice struct {
	done    <-chan struct{}
	stopErr error
}

func (d fakeStopErrorDevice) Start(ctx context.Context, _ io.Writer) error {
	return nil
}

func (d fakeStopErrorDevice) Stop(context.Context) error {
	return d.stopErr
}

func (d fakeStopErrorDevice) Done() <-chan struct{} {
	return d.done
}

func (d fakeStopErrorDevice) Err() error {
	return nil
}

func TestBroadcastPreservesTraceSuppressionWithoutSubscriberCancellation(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, span := observability.NewFilteringTracerProvider(provider, []string{"stream"}).Tracer("test").Start(ctx, "stream")
	defer span.End()
	source := newFakeLiveSource()
	broadcast := NewBroadcast(source, nil)
	var dst bytes.Buffer
	if err := broadcast.attach(ctx, &dst); err != nil {
		t.Fatal(err)
	}
	defer broadcast.detach(&dst)
	cancel()
	if source.ctx.Err() != nil {
		t.Fatal("source canceled with its first subscriber")
	}
	_, startup := observability.StartSpan(source.ctx, observability.SpanTunerProcessStart)
	defer startup.End()
	if startup.IsRecording() {
		t.Fatal("source lost trace suppression")
	}
	if err := broadcast.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.ctx.Err() != context.Canceled {
		t.Fatal("source not canceled on stop")
	}
}
