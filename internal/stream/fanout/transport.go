// Package fanout distributes one live packet stream to many subscribers: the
// part TS and TLV share (attaching subscribers, starting and stopping the
// source, buffering, batched writes, statistics). What differs per system
// plugs in through Transport: splitting bytes into packets, completing
// signaling, filtering a service, and detecting sequence gaps.
package fanout

import "io"

// Transport is the system-specific half of an Engine; the engine calls Feed
// and NewFilter under its own lock, in packet order, so they can share state.
type Transport[S any] interface {
	// Name is the transport label of metrics and logs: "ts" or "tlv".
	Name() string
	// NewReader splits the source bytes into packets.
	NewReader(io.Reader) PacketReader
	// Feed parses one packet and returns the signaling it completes.
	Feed(packet []byte) ([]S, error)
	// NewFilter picks one service's packets for one subscriber.
	NewFilter(serviceID uint16) Filter
	// NewDropDetector returns a detector of gaps in packet sequences.
	NewDropDetector() DropDetector
}

// PacketReader returns packets one by one. A packet is only valid until the
// next call, and io.EOF ends the stream.
type PacketReader interface {
	Next() ([]byte, error)
}

// Filter selects the packets of one service. Packet returns the bytes to
// deliver, nil to skip the packet, or an error that ends the subscription;
// the returned bytes may alias internal buffers, as the engine copies them.
type Filter interface {
	Packet(packet []byte) ([]byte, error)
}

// FilterDropDetector is a Filter whose removed packets would otherwise look
// like drops, so it supplies its own drop detector instead.
type FilterDropDetector interface {
	Filter
	NewDropDetector() DropDetector
}

// DropDetector watches the sequence numbers of a stream's packets.
type DropDetector interface {
	// Observe returns the gap packet reveals, or nil.
	Observe(packet []byte) *Drop
}

// Drop is a gap in one packet sequence.
type Drop struct {
	// Sequence names the packet sequence, such as "pid=0x0100".
	Sequence string
	Expected uint64
	Actual   uint64
}
