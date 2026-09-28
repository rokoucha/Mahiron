package fanout

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
)

// lengthTransport frames packets as a 2-byte length followed by the body,
// so the engine sees packets of varying sizes like TLV.
type lengthTransport struct{}

func (lengthTransport) Name() string { return "test" }

func (lengthTransport) NewReader(r io.Reader) PacketReader { return &lengthReader{r: r} }

func (lengthTransport) Feed([]byte) ([]string, error) { return nil, nil }

func (lengthTransport) NewFilter(uint16) Filter { return nil }

func (lengthTransport) NewDropDetector() DropDetector { return noDrops{} }

type lengthReader struct {
	r   io.Reader
	buf []byte
}

func (r *lengthReader) Next() ([]byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint16(header[:]))
	r.buf = append(r.buf[:0], header[:]...)
	r.buf = append(r.buf, make([]byte, size)...)
	if _, err := io.ReadFull(r.r, r.buf[2:]); err != nil {
		return nil, err
	}
	return r.buf, nil
}

type noDrops struct{}

func (noDrops) Observe([]byte) *Drop { return nil }

func framed(size int, fill byte) []byte {
	packet := make([]byte, 2+size)
	binary.BigEndian.PutUint16(packet, uint16(size))
	for i := range size {
		packet[2+i] = fill
	}
	return packet
}

func TestEngineDeliversVariableSizePackets(t *testing.T) {
	var input []byte
	for i, size := range []int{1, 1500, 7, 65535, 300} {
		input = append(input, framed(size, byte(i))...)
	}
	engine := New[string](lengthTransport{}, func(_ context.Context, dst io.Writer) error {
		_, err := dst.Write(input)
		return err
	}, nil)
	var out bytes.Buffer
	if err := engine.SubscribeChannel(t.Context(), &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Fatalf("output differs from input: %d bytes, want %d", out.Len(), len(input))
	}
}

func TestPacketQueueDropsOldestByBytes(t *testing.T) {
	q := newPacketQueue()
	for i := range 3 {
		if dropped := q.push(make([]byte, 100+i), 400); dropped != 0 {
			t.Fatalf("push %d dropped %d bytes", i, dropped)
		}
	}
	// 100+101+102 queued; 250 more only fits after the two oldest go.
	if dropped := q.push(make([]byte, 250), 400); dropped != 201 {
		t.Fatalf("dropped %d bytes, want 201", dropped)
	}
	batch, ok := q.take(nil, 1<<20)
	if !ok {
		t.Fatal("take reported a closed queue")
	}
	if len(batch) != 2 || len(batch[0]) != 102 || len(batch[1]) != 250 {
		t.Fatalf("remaining packets = %d, want the 102- and 250-byte ones", len(batch))
	}
	q.close()
	if _, ok := q.take(nil, 1<<20); ok {
		t.Fatal("take on a closed, drained queue reported packets")
	}
}
