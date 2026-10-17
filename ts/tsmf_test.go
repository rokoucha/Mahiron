package ts

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"
	"testing"
)

func tsmfHeader(inverted bool) []byte {
	p := make([]byte, PacketSize)
	p[0], p[2], p[3], p[4], p[5], p[6] = 0x47, 0x2f, 0x10, 0xfa, 0x86, 1
	if inverted {
		p[4], p[5] = 0xe5, 0x79
	}
	for i := 0; i < 26; i++ {
		p[73+i] = 0x1f
	}
	binary.BigEndian.PutUint32(p[184:], crc32MPEG2(p[4:184]))
	return p
}

func TestTSMFReader(t *testing.T) {
	for _, stride := range []int{188, 192, 204} {
		t.Run(strconv.Itoa(stride), func(t *testing.T) {
			var input, want []byte
			add := func(p []byte) {
				if stride == 192 {
					input = append(input, 0, 0, 0, 0)
				}
				input = append(input, p...)
				if stride == 204 {
					input = append(input, make([]byte, 16)...)
				}
			}
			packet := func(n byte) []byte { p := make([]byte, 188); p[0], p[2], p[3], p[4] = 0x47, 0x20, 0x10, n; return p }
			add(packet(99)) // Starting mid-frame must not leak packets.
			for frame := 0; frame < 2; frame++ {
				add(tsmfHeader(frame == 1))
				for i := 0; i < 52; i++ {
					p := packet(byte(i))
					add(p)
					if i%2 == 1 {
						want = append(want, p...)
					}
				}
				add(packet(98)) // Never carry the slot table beyond its 52 slots.
			}
			r := NewTSMFReader(bytes.NewReader(input), 15)
			var got bytes.Buffer
			if _, err := io.CopyBuffer(&got, r, make([]byte, 17)); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("got %d bytes, want %d", got.Len(), len(want))
			}
		})
	}
}

func TestTSMFReaderRejectsHeaders(t *testing.T) {
	for _, mutate := range []func([]byte){
		func(p []byte) { p[100] ^= 1 },
		func(p []byte) { p[6] = 0x11 },
		func(p []byte) { p[6] = 2 },
		func(p []byte) { p[4] = 0 },
		func(p []byte) { p[1] |= 0x80 },
	} {
		header := tsmfHeader(false)
		mutate(header)
		// Recalculate CRC except in the deliberately corrupt-CRC case.
		if header[100] == 0 {
			binary.BigEndian.PutUint32(header[184:], crc32MPEG2(header[4:184]))
		}
		p := make([]byte, 188)
		p[0], p[2], p[3] = 0x47, 0x20, 0x10
		data := append(header, bytes.Repeat(p, 52)...)
		got, err := io.ReadAll(NewTSMFReader(bytes.NewReader(data), 1))
		if err != nil || len(got) != 0 {
			t.Fatalf("got %d bytes, err=%v", len(got), err)
		}
	}
}

func TestTSMFReaderRecoversAfterSyncLoss(t *testing.T) {
	p := make([]byte, 188)
	p[0], p[2], p[3] = 0x47, 0x20, 0x10
	input := append(tsmfHeader(false), bytes.Repeat(p, 10)...)
	input = append(input, 0xff)
	other := bytes.Clone(p)
	other[4] = 0xaa
	input = append(input, bytes.Repeat(other, 10)...)
	input = append(input, tsmfHeader(true)...)
	input = append(input, bytes.Repeat(p, 52)...)
	got, err := io.ReadAll(NewTSMFReader(bytes.NewReader(input), 15))
	if err != nil {
		t.Fatal(err)
	}
	// Packets skipped during sync recovery must invalidate the old slot count.
	// The final complete frame always contributes its 26 selected packets.
	for i := 0; i < len(got); i += 188 {
		if got[i+4] == 0xaa {
			t.Fatal("packets leaked after sync loss")
		}
	}
	if len(got) < 26*188 || len(got) > 31*188 {
		t.Fatalf("output size=%d", len(got))
	}
}
