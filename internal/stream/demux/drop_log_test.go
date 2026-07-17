package demux

import (
	"bytes"
	"log/slog"
	"testing"
	"time"
)

func TestLogStreamDropRateLimit(t *testing.T) {
	streamDropLogLast.Clear()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	drop := continuityDrop{PID: 0x0100, ExpectedCounter: 3, ActualCounter: 5}

	logStreamDrop("GR", "27", "", drop)
	logStreamDrop("GR", "27", "", drop)

	if count := bytes.Count(buf.Bytes(), []byte("TS packet drop detected")); count != 1 {
		t.Fatalf("logged %d times, want 1", count)
	}

	streamDropLogLast.Store("GR/27//256", time.Now().Add(-streamDropLogInterval).UnixNano())
	buf.Reset()

	logStreamDrop("GR", "27", "", drop)
	if count := bytes.Count(buf.Bytes(), []byte("TS packet drop detected")); count != 1 {
		t.Fatalf("logged %d times after interval, want 1", count)
	}
}

func TestLogStreamDropIncludesStreamKey(t *testing.T) {
	streamDropLogLast.Clear()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	logStreamDrop("GR", "27", "GR/27:1", continuityDrop{PID: 0x0100, ExpectedCounter: 1, ActualCounter: 3})

	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("stream=GR/27:1")) {
		t.Fatalf("log = %q, want stream key", out)
	}
}
