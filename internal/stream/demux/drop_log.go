package demux

import (
	"log/slog"
	"strconv"
	"sync"
	"time"
)

const streamDropLogInterval = 5 * time.Second

var streamDropLogLast sync.Map // string -> int64 (UnixNano)

type continuityDrop struct {
	PID             uint16
	ExpectedCounter byte
	ActualCounter   byte
}

func logStreamDrop(channelType, channelID, streamKey string, drop continuityDrop) {
	key := channelType + "/" + channelID + "/" + streamKey + "/" + strconv.FormatUint(uint64(drop.PID), 10)
	now := time.Now().UnixNano()
	if last, ok := streamDropLogLast.Load(key); ok {
		if now-last.(int64) < streamDropLogInterval.Nanoseconds() {
			return
		}
	}
	streamDropLogLast.Store(key, now)

	attrs := []any{
		"pid", drop.PID,
		"expectedCounter", drop.ExpectedCounter,
		"actualCounter", drop.ActualCounter,
	}
	if channelType != "" {
		attrs = append(attrs, "type", channelType)
	}
	if channelID != "" {
		attrs = append(attrs, "channel", channelID)
	}
	if streamKey != "" {
		attrs = append(attrs, "stream", streamKey)
	}
	slog.Warn("TS packet drop detected", attrs...)
}
