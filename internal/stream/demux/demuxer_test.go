package demux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

func TestPacketDemuxerNormalizesInputFrames(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 3)
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{name: "188", frame: packet},
		{name: "192", frame: append([]byte{0, 1, 2, 3}, packet...)},
		{name: "204", frame: append(append([]byte{}, packet...), bytes.Repeat([]byte{0xee}, 16)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := bytes.Repeat(tc.frame, 4)
			var starts atomic.Int32
			engine := New(func(_ context.Context, dst io.Writer) error {
				starts.Add(1)
				_, err := dst.Write(input)
				return err
			}, nil)
			var out bytes.Buffer
			if err := engine.SubscribeChannel(t.Context(), &out); err != nil {
				t.Fatal(err)
			}
			if starts.Load() != 1 {
				t.Fatalf("source starts = %d, want 1", starts.Load())
			}
			if got, want := out.Len(), 4*ts.PacketSize; got != want {
				t.Fatalf("output bytes = %d, want %d", got, want)
			}
			for off := 0; off < out.Len(); off += ts.PacketSize {
				if !bytes.Equal(out.Bytes()[off:off+ts.PacketSize], packet) {
					t.Fatalf("packet at %d was not normalized", off/ts.PacketSize)
				}
			}
		})
	}
}

func TestPacketDemuxerReportsStreamInfo(t *testing.T) {
	input := append(streamtest.TestPacket(0x0100, 1), streamtest.TestPacket(0x0100, 3)...)
	input = append(input, streamtest.TestPacket(0x0100, 5)...)
	engine := New(func(_ context.Context, dst io.Writer) error {
		_, err := dst.Write(input)
		return err
	}, nil).WithMetricLabels("GR", "27")

	var gotUserID, gotKey string
	var gotInfo tuner.StreamInfo
	ctx := tuner.WithUser(t.Context(), tuner.User{ID: "viewer"})
	ctx = tuner.WithStreamInfoReporter(ctx, func(userID, key string, info tuner.StreamInfo) {
		gotUserID = userID
		gotKey = key
		gotInfo = info
	})
	if err := engine.SubscribeChannel(ctx, io.Discard); err != nil {
		t.Fatal(err)
	}
	if gotUserID != "viewer" || gotKey != "GR/27" {
		t.Fatalf("stream info target = %q/%q", gotUserID, gotKey)
	}
	if gotInfo.Packet != 3 || gotInfo.Drop != 2 {
		t.Fatalf("stream info = %+v", gotInfo)
	}
}

func TestPacketDemuxerSharesOneSourceAcrossSubscribers(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	var starts atomic.Int32
	engine := New(func(_ context.Context, dst io.Writer) error {
		starts.Add(1)
		<-start
		_, err := dst.Write(bytes.Repeat(packet, 4))
		return err
	}, nil)

	var first, second bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- engine.SubscribeChannel(t.Context(), &first) }()
	go func() { errs <- engine.SubscribeChannel(t.Context(), &second) }()
	waitForDemuxerSubscribers(t, engine, 2)
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if starts.Load() != 1 {
		t.Fatalf("source starts = %d, want 1", starts.Load())
	}
	if first.Len() != 4*ts.PacketSize || second.Len() != 4*ts.PacketSize {
		t.Fatalf("subscriber bytes = %d/%d", first.Len(), second.Len())
	}
}

func TestPacketDemuxerDisconnectsOverflowingSubscriberOnly(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	engine := New(func(_ context.Context, dst io.Writer) error {
		<-start
		for range packetSubscriberBuffer + 32 {
			if _, err := dst.Write(packet); err != nil {
				return err
			}
			time.Sleep(50 * time.Microsecond)
		}
		return nil
	}, nil)

	blocked := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var fast bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- engine.SubscribeChannel(t.Context(), blocked) }()
	go func() { errs <- engine.SubscribeChannel(t.Context(), &fast) }()
	waitForDemuxerSubscribers(t, engine, 2)
	close(start)
	<-blocked.entered

	var overflow error
	for range 2 {
		err := <-errs
		if errors.Is(err, ErrSubscriberOverflow) {
			overflow = err
		}
	}
	close(blocked.release)
	if overflow == nil {
		t.Fatal("slow subscriber did not return ErrSubscriberOverflow")
	}
	if fast.Len() == 0 {
		t.Fatal("fast subscriber received no packets")
	}
}

func TestPacketDemuxerToleratesStalledSubscriberWithinBuffer(t *testing.T) {
	const count = 2000 // well beyond the old 512-packet buffer, within the new one
	start := make(chan struct{})
	engine := New(func(_ context.Context, dst io.Writer) error {
		<-start
		for i := range count {
			if _, err := dst.Write(streamtest.TestPacket(0x0100, byte(i))); err != nil {
				return err
			}
		}
		return nil
	}, nil)

	stall := &stallingWriter{release: make(chan struct{})}
	errCh := make(chan error, 1)
	go func() { errCh <- engine.SubscribeChannel(t.Context(), stall) }()
	waitForDemuxerSubscribers(t, engine, 1)
	close(start)

	time.Sleep(200 * time.Millisecond)
	close(stall.release)

	if err := <-errCh; err != nil {
		t.Fatalf("SubscribeChannel returned error = %v, want nil", err)
	}
	if got, want := stall.Len(), count*ts.PacketSize; got != want {
		t.Fatalf("subscriber received %d bytes, want %d", got, want)
	}
}

func TestPacketDemuxerWaitsForWriterOnContextCancel(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	engine := New(func(_ context.Context, dst io.Writer) error {
		<-start
		_, err := dst.Write(packet)
		return err
	}, nil)

	blocked := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- engine.SubscribeChannel(ctx, blocked) }()
	waitForDemuxerSubscribers(t, engine, 1)
	close(start)
	<-blocked.entered

	cancel()
	select {
	case err := <-returned:
		t.Fatalf("SubscribeChannel returned before writer finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(blocked.release)
	if err := <-returned; err != nil {
		t.Fatalf("SubscribeChannel error = %v, want nil", err)
	}
}

type stallingWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	once    sync.Once
	release chan struct{}
}

func (w *stallingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { <-w.release })
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *stallingWriter) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Len()
}

func TestPacketDemuxerObserveSectionsWaitsForObserverOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	engine := New(func(context.Context, io.Writer) error {
		return nil
	}, nil)
	attached := make(chan struct{})
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan error, 1)

	go func() {
		returned <- engine.ObserveSectionsPassive(ctx, nil, func(ts.Section) error {
			close(entered)
			<-release
			return ctx.Err()
		}, attached)
	}()
	<-attached

	engine.dispatch(nil, []ts.PIDSection{{PID: ts.PIDEIT, Section: ts.Section{ts.TableIDEITSStart, 0, 0}}})
	<-entered
	cancel()

	select {
	case err := <-returned:
		t.Fatalf("ObserveSections returned before observer finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("ObserveSections error = %v, want context canceled", err)
	}
}

func TestContinuityMonitorDetectsCounterGap(t *testing.T) {
	monitor := &continuityMonitor{}
	if monitor.observe(streamtest.TestPacket(0x0100, 1)) != nil {
		t.Fatal("first packet reported continuity error")
	}
	if monitor.observe(streamtest.TestPacket(0x0100, 2)) != nil {
		t.Fatal("sequential packet reported continuity error")
	}
	drop := monitor.observe(streamtest.TestPacket(0x0100, 4))
	if drop == nil {
		t.Fatal("counter gap did not report continuity error")
	}
	if drop.PID != 0x0100 || drop.ExpectedCounter != 3 || drop.ActualCounter != 4 {
		t.Fatalf("drop = %+v, want pid=0x0100 expected=3 actual=4", drop)
	}
	if monitor.observe(streamtest.TestPacket(0x0101, 9)) != nil {
		t.Fatal("first packet for another PID reported continuity error")
	}
}

func TestContinuityMonitorIgnoresInvalidPackets(t *testing.T) {
	monitor := &continuityMonitor{}
	packet := streamtest.TestPacket(0x0100, 1)
	packet[0] = 0
	if monitor.observe(packet) != nil {
		t.Fatal("invalid packet reported continuity error")
	}
	if monitor.seen[0x0100] {
		t.Fatal("invalid packet changed continuity state")
	}
}

func TestContinuityMonitorAcceptsSignaledDiscontinuity(t *testing.T) {
	monitor := &continuityMonitor{}
	if monitor.observe(streamtest.TestPacket(0x0100, 1)) != nil {
		t.Fatal("first packet reported continuity error")
	}
	packet := streamtest.TestPacket(0x0100, 9)
	packet[3] = 0x30 | 9
	packet[4] = 1
	packet[5] = 0x80
	if monitor.observe(packet) != nil {
		t.Fatal("signaled discontinuity reported continuity error")
	}
	if monitor.observe(streamtest.TestPacket(0x0100, 10)) != nil {
		t.Fatal("packet following signaled discontinuity reported continuity error")
	}
}

func TestContinuityMonitorAcceptsSignaledDiscontinuityWithoutPayload(t *testing.T) {
	monitor := &continuityMonitor{}
	if monitor.observe(streamtest.TestPacket(0x0100, 1)) != nil {
		t.Fatal("first packet reported continuity error")
	}
	packet := streamtest.TestPacket(0x0100, 1)
	packet[3] = 0x20 | 1
	packet[4] = 1
	packet[5] = 0x80
	if monitor.observe(packet) != nil {
		t.Fatal("adaptation-only discontinuity reported continuity error")
	}
	if monitor.observe(streamtest.TestPacket(0x0100, 9)) != nil {
		t.Fatal("packet following adaptation-only discontinuity reported continuity error")
	}
}

func TestContinuityMonitorAcceptsOneIdenticalDuplicate(t *testing.T) {
	monitor := &continuityMonitor{}
	packet := streamtest.TestPacket(0x0100, 1)
	if monitor.observe(packet) != nil {
		t.Fatal("first packet reported continuity error")
	}
	if monitor.observe(packet) != nil {
		t.Fatal("identical duplicate reported continuity error")
	}
	if monitor.observe(packet) == nil {
		t.Fatal("second identical duplicate did not report continuity error")
	}
	if monitor.observe(streamtest.TestPacket(0x0100, 2)) != nil {
		t.Fatal("sequential packet after duplicate reported continuity error")
	}
}

func TestContinuityMonitorRejectsChangedPacketWithSameCounter(t *testing.T) {
	monitor := &continuityMonitor{}
	packet := streamtest.TestPacket(0x0100, 1)
	if monitor.observe(packet) != nil {
		t.Fatal("first packet reported continuity error")
	}
	changed := append(ts.Packet(nil), packet...)
	changed[10] = 0
	if monitor.observe(changed) == nil {
		t.Fatal("changed packet with repeated counter did not report continuity error")
	}
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	called  atomic.Bool
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	if w.called.CompareAndSwap(false, true) {
		close(w.entered)
	}
	<-w.release
	return len(p), nil
}

func waitForDemuxerSubscribers(t *testing.T, engine *Demuxer, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		engine.mu.Lock()
		got := len(engine.packets)
		engine.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("packet subscribers did not reach %d", want)
}
