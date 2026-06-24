package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

func TestPacketEngineNormalizesInputFrames(t *testing.T) {
	packet := engineTestPacket(0x0100, 3)
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
			engine := newPacketEngine(func(_ context.Context, dst io.Writer) error {
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

func TestPacketEngineReportsStreamInfo(t *testing.T) {
	input := append(engineTestPacket(0x0100, 1), engineTestPacket(0x0100, 3)...)
	input = append(input, engineTestPacket(0x0100, 5)...)
	engine := newPacketEngine(func(_ context.Context, dst io.Writer) error {
		_, err := dst.Write(input)
		return err
	}, nil).withMetricLabels("GR", "27")

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

func TestPacketEngineSharesOneSourceAcrossSubscribers(t *testing.T) {
	packet := engineTestPacket(0x0100, 1)
	start := make(chan struct{})
	var starts atomic.Int32
	engine := newPacketEngine(func(_ context.Context, dst io.Writer) error {
		starts.Add(1)
		<-start
		_, err := dst.Write(bytes.Repeat(packet, 4))
		return err
	}, nil)

	var first, second bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- engine.SubscribeChannel(t.Context(), &first) }()
	go func() { errs <- engine.SubscribeChannel(t.Context(), &second) }()
	waitForEngineSubscribers(t, engine, 2)
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

func TestPacketEngineDisconnectsOverflowingSubscriberOnly(t *testing.T) {
	packet := engineTestPacket(0x0100, 1)
	start := make(chan struct{})
	engine := newPacketEngine(func(_ context.Context, dst io.Writer) error {
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
	waitForEngineSubscribers(t, engine, 2)
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

func TestSharedSessionUsesOneDescramblerForDecodedSubscribers(t *testing.T) {
	packet := engineTestPacket(0x0100, 1)
	start := make(chan struct{})
	source := &finitePacketSource{data: bytes.Repeat(packet, 4), start: start, done: make(chan struct{})}
	descrambler := &passthroughDescrambler{}
	session := NewChannelSession(ChannelSessionConfig{
		Broadcast:   NewBroadcast(source, nil),
		Channel:     "27",
		Descrambler: descrambler,
		OnStop:      func() {},
		Type:        "GR",
	})

	var first, second bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- session.ChannelStream(t.Context(), true, &first) }()
	go func() { errs <- session.ChannelStream(t.Context(), true, &second) }()
	waitForEngineSubscribers(t, session.decodedEngine, 2)
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if descrambler.starts.Load() != 1 {
		t.Fatalf("descrambler starts = %d, want 1", descrambler.starts.Load())
	}
	if first.Len() != 4*ts.PacketSize || second.Len() != 4*ts.PacketSize {
		t.Fatalf("decoded subscriber bytes = %d/%d", first.Len(), second.Len())
	}
}

func TestContinuityMonitorDetectsCounterGap(t *testing.T) {
	monitor := &continuityMonitor{last: map[uint16]byte{}}
	if monitor.observe(engineTestPacket(0x0100, 1)) {
		t.Fatal("first packet reported continuity error")
	}
	if monitor.observe(engineTestPacket(0x0100, 2)) {
		t.Fatal("sequential packet reported continuity error")
	}
	if !monitor.observe(engineTestPacket(0x0100, 4)) {
		t.Fatal("counter gap did not report continuity error")
	}
	if monitor.observe(engineTestPacket(0x0101, 9)) {
		t.Fatal("first packet for another PID reported continuity error")
	}
}

func TestContinuityMonitorIgnoresInvalidPackets(t *testing.T) {
	monitor := &continuityMonitor{last: map[uint16]byte{}}
	packet := engineTestPacket(0x0100, 1)
	packet[0] = 0
	if monitor.observe(packet) {
		t.Fatal("invalid packet reported continuity error")
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

func waitForEngineSubscribers(t *testing.T, engine *packetEngine, want int) {
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

func engineTestPacket(pid uint16, counter byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, ts.PacketSize)
	packet[0] = ts.SyncByte
	packet[1] = byte(pid >> 8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | counter&0x0f
	return packet
}

type finitePacketSource struct {
	data  []byte
	done  chan struct{}
	err   error
	start <-chan struct{}
}

func (s *finitePacketSource) Start(_ context.Context, dst io.Writer) error {
	go func() {
		<-s.start
		_, s.err = dst.Write(s.data)
		close(s.done)
	}()
	return nil
}

func (*finitePacketSource) Stop(context.Context) error { return nil }
func (s *finitePacketSource) Done() <-chan struct{}    { return s.done }
func (s *finitePacketSource) Err() error               { return s.err }
func (*finitePacketSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}

type passthroughDescrambler struct {
	starts atomic.Int32
}

func (d *passthroughDescrambler) Descramble(_ context.Context, src io.Reader, dst io.Writer) error {
	d.starts.Add(1)
	_, err := io.Copy(dst, src)
	return err
}
