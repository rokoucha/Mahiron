package channel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/demux"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/ts"
)

type decoderProbe func(context.Context, io.Reader, io.Writer) error

func (f decoderProbe) Descramble(ctx context.Context, r io.Reader, w io.Writer) error {
	return f(ctx, r, w)
}

func TestDecoderInputPrimesFragmentedStream(t *testing.T) {
	for _, size := range []int{1, 17, ts.PacketSize} {
		data := bytes.Repeat(streamtest.TestPacket(0x100, 0), 384)
		var first int
		session := &Session{
			rawDemuxer: demux.New(func(ctx context.Context, dst io.Writer) error {
				for offset := 0; offset < len(data); offset += size {
					if _, err := dst.Write(data[offset:min(offset+size, len(data))]); err != nil {
						return err
					}
				}
				return nil
			}, nil),
			descrambler: decoderProbe(func(_ context.Context, r io.Reader, w io.Writer) error {
				buf := make([]byte, 32*1024)
				n, err := r.Read(buf)
				first = n
				if err != nil {
					return err
				}
				if _, err = w.Write(buf[:n]); err != nil {
					return err
				}
				_, err = io.Copy(w, r)
				return err
			}),
		}
		var got bytes.Buffer
		if err := session.subscribeDecodedMux(t.Context(), &got); err != nil {
			t.Fatal(err)
		}
		if first != 128*ts.PacketSize {
			t.Fatalf("fragment=%d: decoder first read=%d", size, first)
		}
		if !bytes.Equal(got.Bytes(), data) {
			t.Fatalf("fragment=%d: got %d bytes, want %d", size, got.Len(), len(data))
		}
	}
}

func TestDecoderInputFlushesShortStream(t *testing.T) {
	data := bytes.Repeat(streamtest.TestPacket(0x100, 0), 4)
	session := &Session{rawDemuxer: demux.New(func(_ context.Context, w io.Writer) error { _, err := w.Write(data); return err }, nil), descrambler: &passthroughDescrambler{}}
	var got bytes.Buffer
	if err := session.subscribeDecodedMux(t.Context(), &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatalf("got %d bytes, want %d", got.Len(), len(data))
	}
}

func TestDecoderInputPreservesSourceError(t *testing.T) {
	expected := errors.New("receiver failed")
	session := &Session{rawDemuxer: demux.New(func(context.Context, io.Writer) error { return expected }, nil), descrambler: &passthroughDescrambler{}}
	if err := session.subscribeDecodedMux(t.Context(), io.Discard); !errors.Is(err, expected) {
		t.Fatalf("err=%v, want source error", err)
	}
}

func TestDecoderInputCancelsIncompletePriming(t *testing.T) {
	for _, command := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		wrote := make(chan struct{})
		var started atomic.Bool
		marker := filepath.Join(t.TempDir(), "decoder-started")
		session := &Session{rawDemuxer: demux.New(func(ctx context.Context, w io.Writer) error {
			_, err := w.Write(bytes.Repeat(streamtest.TestPacket(0x100, 0), 8))
			close(wrote)
			if err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		}, nil), descrambler: decoderProbe(func(_ context.Context, r io.Reader, w io.Writer) error {
			started.Store(true)
			_, err := io.Copy(w, r)
			return err
		})}
		if command {
			session.descrambler = source.NewCommandDescrambler(fmt.Sprintf("touch %q; cat", marker))
		}
		done := make(chan error, 1)
		go func() { done <- session.subscribeDecodedMux(ctx, io.Discard) }()
		select {
		case <-wrote:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("input did not start")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("decoder input did not cancel")
		}
		if started.Load() {
			t.Fatal("decoder started after cancellation")
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("decoder process started after cancellation: %v", err)
		}
	}
}
