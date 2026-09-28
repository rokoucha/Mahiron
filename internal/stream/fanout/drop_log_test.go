package fanout

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

	drop := Drop{Sequence: "pid=0x0100", Expected: 3, Actual: 5}

	logStreamDrop("ts", "GR", "27", "", drop)
	logStreamDrop("ts", "GR", "27", "", drop)

	if count := bytes.Count(buf.Bytes(), []byte("TS packet drop detected")); count != 1 {
		t.Fatalf("logged %d times, want 1", count)
	}

	streamDropLogLast.Store("ts/GR/27//pid=0x0100", time.Now().Add(-streamDropLogInterval).UnixNano())
	buf.Reset()

	logStreamDrop("ts", "GR", "27", "", drop)
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

	logStreamDrop("tlv", "BS4K", "BS1", "BS4K/BS1:101", Drop{Sequence: "packet_id=0x0100", Expected: 1, Actual: 3})

	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("stream=BS4K/BS1:101")) || !bytes.Contains(buf.Bytes(), []byte("TLV packet drop detected")) {
		t.Fatalf("log = %q, want TLV message and stream key", out)
	}
}
