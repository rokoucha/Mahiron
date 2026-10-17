package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
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

type blockingStartSource struct {
	*fakeLiveSourceForBroadcast
	entered chan struct{}
	proceed chan struct{}
}

func (s *blockingStartSource) Start(ctx context.Context, w io.Writer) error {
	close(s.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.proceed:
		return s.fakeLiveSourceForBroadcast.Start(ctx, w)
	}
}

func TestBroadcastStopInterruptsStartingSource(t *testing.T) {
	source := &blockingStartSource{fakeLiveSourceForBroadcast: newFakeLiveSource(), entered: make(chan struct{}), proceed: make(chan struct{})}
	b := NewBroadcast(source, nil)
	result := make(chan error, 1)
	go func() { result <- b.attach(context.Background(), io.Discard) }()
	<-source.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("stopped startup succeeded")
		}
	case <-ctx.Done():
		t.Fatal("Stop could not interrupt startup")
	}
}

func TestBroadcastCanceledSubscriberDoesNotCancelOtherStartup(t *testing.T) {
	source := &blockingStartSource{fakeLiveSourceForBroadcast: newFakeLiveSource(), entered: make(chan struct{}), proceed: make(chan struct{})}
	b := NewBroadcast(source, nil)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan error, 1)
	second := make(chan error, 1)
	w1, w2 := new(bytes.Buffer), new(bytes.Buffer)
	go func() { first <- b.attach(firstCtx, w1) }()
	<-source.entered
	go func() { second <- b.attach(context.Background(), w2) }()
	deadline := time.Now().Add(time.Second)
	for {
		b.mu.Lock()
		refs := b.refs
		b.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second subscriber did not attach")
		}
		time.Sleep(time.Millisecond)
	}
	cancelFirst()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first error: %v", err)
	}
	close(source.proceed)
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second subscriber lost shared startup")
	}
	b.detach(w2)
}

func TestBroadcastCancellationDuringLocalTunerStartup(t *testing.T) {
	device := tuner.NewCommandDevice(nil, "sleep 60", tuner.StartupRetryConfig{Max: 1, Timeout: 10 * time.Second})
	b := NewBroadcast(&tunerLiveSource{device: device}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- b.SubscribeRaw(ctx, io.Discard) }()
	deadline := time.Now().Add(time.Second)
	for device.(tuner.ProcessStatus).ProcessStatus().PID == 0 {
		if time.Now().After(deadline) {
			t.Fatal("local process did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local startup cancellation waited for Stop deadline")
	}
}
