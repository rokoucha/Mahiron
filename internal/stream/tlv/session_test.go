package tlv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/mmt"
)

func tlvBytes(payload ...byte) []byte {
	// Minimal TLV packet: sync byte, null packet type, length, payload.
	packet := []byte{mmt.TLVSyncByte, 0xFF, 0x00, byte(len(payload))}
	return append(packet, payload...)
}

type finiteSource struct {
	chunks [][]byte
	done   chan struct{}
}

func newFiniteSource(chunks ...[]byte) *finiteSource {
	return &finiteSource{chunks: chunks, done: make(chan struct{})}
}

func (s *finiteSource) Start(_ context.Context, dst io.Writer) error {
	go func() {
		defer close(s.done)
		for _, chunk := range s.chunks {
			if _, err := dst.Write(chunk); err != nil {
				return
			}
		}
	}()
	return nil
}

func (s *finiteSource) Stop(context.Context) error { return nil }

func (s *finiteSource) Done() <-chan struct{} { return s.done }

func (s *finiteSource) Err() error { return nil }

func (s *finiteSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}

// hangSource emits its prefix, then TLV packets until its context ends.
type hangSource struct {
	prefix []byte
	done   chan struct{}
}

func (s *hangSource) Start(ctx context.Context, dst io.Writer) error {
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		if _, err := dst.Write(s.prefix); err != nil {
			return
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := dst.Write(tlvBytes(0xAA)); err != nil {
					return
				}
			}
		}
	}()
	return nil
}

func (s *hangSource) Stop(context.Context) error { return nil }

func (s *hangSource) Done() <-chan struct{} { return s.done }

func (s *hangSource) Err() error { return nil }

func (s *hangSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}

type countingDescrambler struct {
	mu    sync.Mutex
	calls int
}

func (d *countingDescrambler) Descramble(ctx context.Context, src io.Reader, dst io.Writer) error {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	_, err := io.Copy(dst, src)
	return err
}

func (d *countingDescrambler) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func testSession(broadcast *source.Broadcast, descrambler source.Descrambler) *Session {
	return NewSession(Config{
		Channel:     "101",
		Type:        "BS4K",
		Broadcast:   broadcast,
		Descrambler: descrambler,
	})
}

func TestChannelStreamPassesRawTLVThrough(t *testing.T) {
	want := append(tlvBytes(0x01, 0x02), tlvBytes(0x03)...)
	session := testSession(source.NewBroadcast(newFiniteSource(want), nil), nil)

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("raw stream = %x, want %x", out.Bytes(), want)
	}
}

func TestServiceStreamPassesRawTLVThrough(t *testing.T) {
	want := tlvBytes(0x01)
	session := testSession(source.NewBroadcast(newFiniteSource(want), nil), nil)

	var out bytes.Buffer
	if err := session.ServiceStream(context.Background(), 101, false, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("service stream = %x, want %x", out.Bytes(), want)
	}
}

func TestDecodeWithoutDescramblerFallsBackToRaw(t *testing.T) {
	want := tlvBytes(0x01)
	session := testSession(source.NewBroadcast(newFiniteSource(want), nil), nil)

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), true, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("decode=1 without b61Decoder = %x, want raw %x", out.Bytes(), want)
	}
}

func TestDecodeSharesOneDescrambler(t *testing.T) {
	descrambler := &countingDescrambler{}
	session := testSession(source.NewBroadcast(&hangSource{}, nil), descrambler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var first, second lockedBuffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = session.ChannelStream(ctx, true, &first)
	}()
	go func() {
		defer wg.Done()
		_ = session.ChannelStream(ctx, true, &second)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for (first.Len() == 0 || second.Len() == 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if first.Len() == 0 || second.Len() == 0 {
		t.Fatalf("both decoded subscribers should receive data: first=%d second=%d bytes", first.Len(), second.Len())
	}
	if first.Bytes()[0] != mmt.TLVSyncByte || second.Bytes()[0] != mmt.TLVSyncByte {
		t.Fatal("decoded streams must start with the TLV sync byte")
	}
	if got := descrambler.count(); got != 1 {
		t.Fatalf("descrambler calls = %d, want 1 shared", got)
	}
}

func TestChannelStreamRejectsTS(t *testing.T) {
	tsPacket := append([]byte{0x47}, bytes.Repeat([]byte{0xFF}, 187)...)
	session := testSession(source.NewBroadcast(newFiniteSource(tsPacket), nil), nil)

	if err := session.ChannelStream(context.Background(), false, io.Discard); !errors.Is(err, ErrNotTLVStream) {
		t.Fatalf("TS input error = %v, want ErrNotTLVStream", err)
	}
}

func TestChannelStreamRejectsGarbage(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource([]byte{0x00, 0x01, 0x02}), nil), nil)

	if err := session.ChannelStream(context.Background(), false, io.Discard); !errors.Is(err, ErrNotTLVStream) {
		t.Fatalf("garbage input error = %v, want ErrNotTLVStream", err)
	}
}

// TestSessionDoesNotImplementBMLSource pins the interface split: TLV
// sessions never serve BML (TS) data broadcast, so BML API handlers must
// reject TLV services before allocating a tuner instead of relying on a
// session that reports "not supported yet".
func TestSessionDoesNotImplementBMLSource(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource(tlvBytes(0x01)), nil), nil)
	if _, ok := any(session).(interface {
		ObserveDataBroadcast(context.Context, uint16, bool, func(bml.Event) error) error
	}); ok {
		t.Fatal("TLV session implements ObserveDataBroadcast, want no BML methods")
	}
}

func TestStopMakesSessionUnusable(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource(tlvBytes(0x01)), nil), nil)

	if !session.Alive() {
		t.Fatal("Alive = false before Stop")
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if session.Alive() {
		t.Fatal("Alive = true after Stop")
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop error = %v", err)
	}
	if err := session.ChannelStream(context.Background(), false, io.Discard); err == nil {
		t.Fatal("ChannelStream after Stop = nil, want error")
	}
	if session.Type() != "BS4K" || session.Channel() != "101" {
		t.Fatalf("identity = %s/%s", session.Type(), session.Channel())
	}
}

type stubRemoteClient struct {
	data []byte
}

func (s *stubRemoteClient) CheckAvailableForRoute(context.Context, string, string) error {
	return nil
}

func (s *stubRemoteClient) ChannelStream(_ context.Context, _, _ string, _ bool, dst io.Writer) error {
	_, err := dst.Write(s.data)
	return err
}

func TestChannelStreamOverRemoteHandle(t *testing.T) {
	want := tlvBytes(0x09)
	channel := config.ChannelConfig{Type: "BS4K", Channel: "101", Transport: config.TransportTLV}
	handle := source.NewRemoteInputHandle(&stubRemoteClient{data: want}, channel, channel, "living", "BS4K")
	session := NewSession(Config{Channel: "101", Type: "BS4K", Handle: handle})

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("remote tlv stream = %x, want %x", out.Bytes(), want)
	}
	if !session.Alive() {
		t.Fatal("remote session Alive = false")
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChannelStreamOverRemoteHandleRejectsTS(t *testing.T) {
	channel := config.ChannelConfig{Type: "BS4K", Channel: "101", Transport: config.TransportTLV}
	handle := source.NewRemoteInputHandle(&stubRemoteClient{data: []byte{0x47}}, channel, channel, "living", "BS4K")
	session := NewSession(Config{Channel: "101", Type: "BS4K", Handle: handle})

	if err := session.ChannelStream(context.Background(), false, io.Discard); !errors.Is(err, ErrNotTLVStream) {
		t.Fatalf("remote TS error = %v, want ErrNotTLVStream", err)
	}
}

func TestChannelStreamEmptySourceEndsCleanly(t *testing.T) {
	session := testSession(source.NewBroadcast(newFiniteSource(), nil), nil)

	var out bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &out); err != nil {
		t.Fatalf("empty source error = %v, want nil", err)
	}
	if out.Len() != 0 {
		t.Fatalf("empty source bytes = %d, want 0", out.Len())
	}
}

// lockedBuffer is a goroutine-safe bytes.Buffer for polling stream output in
// tests.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}
