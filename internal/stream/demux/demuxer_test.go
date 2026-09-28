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

	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

func TestPacketDemuxerNormalizesInputFrames(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 3)
	// The engine's reader now checks the stream is TS before delivering a
	// packet (see framing.NewCheckReader), which needs five packets in a
	// row of one stride to be convinced; six gives it margin.
	const repeats = 6
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{name: "188", frame: packet},
		{name: "192", frame: append([]byte{0, 1, 2, 3}, packet...)},
		{name: "204", frame: append(append([]byte{}, packet...), bytes.Repeat([]byte{0xee}, 16)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := bytes.Repeat(tc.frame, repeats)
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
			if got, want := out.Len(), repeats*ts.PacketSize; got != want {
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

func TestPacketDemuxerDropsForOverflowingSubscriberWithoutDisconnecting(t *testing.T) {
	// More packets than the buffer holds, so the blocked subscriber must lose
	// some of them. The source is live and cannot be paused for one slow
	// consumer, so the demuxer drops for it rather than disconnecting it.
	const extra = 1000
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	sent := make(chan struct{})
	engine := New(func(_ context.Context, dst io.Writer) error {
		<-start
		defer close(sent)
		for range packetSubscriberBuffer + extra {
			if _, err := dst.Write(packet); err != nil {
				return err
			}
		}
		return nil
	}, nil)

	blocked := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var fast countingWriter
	errs := make(chan error, 2)
	go func() { errs <- engine.SubscribeChannel(t.Context(), blocked) }()
	go func() { errs <- engine.SubscribeChannel(t.Context(), &fast) }()
	waitForDemuxerSubscribers(t, engine, 2)
	close(start)

	<-blocked.entered
	<-sent
	close(blocked.release)

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("SubscribeChannel returned error = %v, want nil", err)
		}
	}
	if got := fast.Len(); got == 0 {
		t.Fatal("fast subscriber received no packets")
	}
	// The blocked subscriber keeps whatever the buffer still held; the packets
	// pushed out while it was stalled are gone.
	sentBytes := (packetSubscriberBuffer + extra) * ts.PacketSize
	if got := blocked.Len(); got >= sentBytes {
		t.Fatalf("blocked subscriber received %d bytes, want fewer than the %d sent", got, sentBytes)
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

func TestKeepAliveDoesNotQueueSections(t *testing.T) {
	engine := New(func(ctx context.Context, _ io.Writer) error {
		<-ctx.Done()
		return nil
	}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	returned := make(chan error, 1)
	go func() {
		returned <- engine.KeepAlive(ctx)
	}()

	if !streamtest.Eventually(time.Second, func() bool {
		return engine.SectionSubscriberCount() == 1
	}) {
		t.Fatal("keep-alive subscription did not attach")
	}

	packet := ts.Packet(make([]byte, ts.PacketSize))
	section := ts.PIDSection{Section: ts.Section{ts.TableIDTOT}}
	for range 1024 {
		engine.Dispatch(packet, []ts.PIDSection{section})
	}
	select {
	case err := <-returned:
		t.Fatalf("KeepAlive returned while sections were dispatched: %v", err)
	default:
	}

	cancel()
	if err := <-returned; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("KeepAlive error = %v, want nil or context canceled", err)
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

	engine.Dispatch(nil, []ts.PIDSection{{PID: ts.PIDEIT, Section: ts.Section{ts.TableIDEITSStart, 0, 0}}})
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
	written atomic.Int64
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	if w.called.CompareAndSwap(false, true) {
		close(w.entered)
	}
	<-w.release
	w.written.Add(int64(len(p)))
	return len(p), nil
}

func (w *blockingWriter) Len() int { return int(w.written.Load()) }

// countingWriter records only how much it was given, so a fast subscriber can
// be checked without retaining megabytes of packets.
type countingWriter struct{ written atomic.Int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.written.Add(int64(len(p)))
	return len(p), nil
}

func (w *countingWriter) Len() int { return int(w.written.Load()) }

// packetSubscriberBuffer is how many TS packets fit in a subscriber's byte
// budget.
const packetSubscriberBuffer = fanout.SubscriberBufferBytes / ts.PacketSize

func waitForDemuxerSubscribers(t *testing.T, engine *Demuxer, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if engine.PacketSubscriberCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("packet subscribers did not reach %d", want)
}

// A subscriber that falls behind and then recovers must keep receiving. This is
// the case the previous behaviour got wrong: a momentary stall ended the
// stream, which for a recording meant losing the whole file rather than the
// packets that did not fit.
func TestPacketDemuxerResumesDeliveryAfterOverflow(t *testing.T) {
	const beyondBuffer = packetSubscriberBuffer + 1000
	packet := streamtest.TestPacket(0x0100, 1)
	stalled := make(chan struct{})
	resume := make(chan struct{})
	start := make(chan struct{})
	engine := New(func(_ context.Context, dst io.Writer) error {
		<-start
		// Overrun the buffer while the consumer is stalled...
		for range beyondBuffer {
			if _, err := dst.Write(packet); err != nil {
				return err
			}
		}
		close(stalled)
		// ...then keep sending once it has recovered.
		<-resume
		for range 100 {
			if _, err := dst.Write(packet); err != nil {
				return err
			}
		}
		return nil
	}, nil)

	blocked := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	errCh := make(chan error, 1)
	go func() { errCh <- engine.SubscribeChannel(t.Context(), blocked) }()
	waitForDemuxerSubscribers(t, engine, 1)
	close(start)

	<-blocked.entered
	<-stalled
	close(blocked.release)

	// Let the subscriber drain what it kept before sending more.
	if !streamtest.Eventually(2*time.Second, func() bool { return blocked.Len() > 0 }) {
		t.Fatal("subscriber received nothing after being released")
	}
	drained := blocked.Len()
	close(resume)

	if err := <-errCh; err != nil {
		t.Fatalf("SubscribeChannel returned error = %v, want nil", err)
	}
	if got := blocked.Len(); got <= drained {
		t.Fatalf("subscriber received %d bytes, want more than the %d it had before recovering", got, drained)
	}
}

// TestPacketDemuxerRejectsTLV pins the reverse of TLV's own guard: a route
// configured as TS that actually carries ISDB-S3 TLV (a misconfigured
// channel, or a remote/decoder that converts the other way) is caught
// before any packet is delivered, instead of PacketReader's lenient resync
// picking out coincidental sync bytes from the TLV bytes.
func TestPacketDemuxerRejectsTLV(t *testing.T) {
	tlvPacket := append([]byte{0x7F, 0xFF, 0x00, byte(ts.PacketSize - 4)}, bytes.Repeat([]byte{0xAA}, ts.PacketSize-4)...)
	input := bytes.Repeat(tlvPacket, 2000)
	engine := New(func(_ context.Context, dst io.Writer) error {
		_, err := dst.Write(input)
		return err
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.SubscribeChannel(ctx, io.Discard); !errors.Is(err, ErrNotTSStream) {
		t.Fatalf("SubscribeChannel over TLV bytes error = %v, want ErrNotTSStream", err)
	}
	if ctx.Err() != nil {
		t.Fatal("SubscribeChannel over TLV bytes hung instead of rejecting")
	}
}

// TestPacketDemuxerRejectsGarbage is the demux-side counterpart of TLV's
// own garbage rejection test.
func TestPacketDemuxerRejectsGarbage(t *testing.T) {
	engine := New(func(_ context.Context, dst io.Writer) error {
		_, err := dst.Write([]byte{0x00, 0x01, 0x02})
		return err
	}, nil)

	if err := engine.SubscribeChannel(context.Background(), io.Discard); !errors.Is(err, ErrNotTSStream) {
		t.Fatalf("SubscribeChannel over garbage error = %v, want ErrNotTSStream", err)
	}
}
