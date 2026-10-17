package tuner

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
)

func TestTunerTSMF(t *testing.T) {
	header := make([]byte, 188)
	header[0], header[2], header[3], header[4], header[5], header[6] = 0x47, 0x2f, 0x10, 0xfa, 0x86, 1
	header[73] = 0x12
	crc := uint32(0xffffffff)
	for _, b := range header[4:184] {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	binary.BigEndian.PutUint32(header[184:], crc)
	packet := make([]byte, 188)
	packet[0], packet[2], packet[3] = 0x47, 0x20, 0x10
	other := bytes.Clone(packet)
	other[4] = 0xaa
	raw := append(bytes.Clone(header), packet...)
	raw = append(raw, other...)
	raw = bytes.Repeat(raw, 128)
	path := filepath.Join(t.TempDir(), "input.ts")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, dvb := range []bool{false, true} {
		for _, rel := range []uint8{0, 1, 2} {
			t.Run(fmt.Sprintf("dvb=%v/rel=%d", dvb, rel), func(t *testing.T) {
				channel := &config.ChannelConfig{TsmfRelTs: &rel}
				var device Device
				if dvb {
					device = NewDVBDevice(channel, "true", path)
				} else {
					device = NewCommandDevice(channel, "cat "+path)
				}
				var dst bytes.Buffer
				if err := device.Start(context.Background(), &dst); err != nil {
					t.Fatal(err)
				}
				select {
				case <-device.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("device did not finish")
				}
				if err := device.Err(); err != nil {
					t.Fatal(err)
				}
				want := raw
				switch rel {
				case 1:
					want = bytes.Repeat(packet, 128)
				case 2:
					want = bytes.Repeat(other, 128)
				}
				if !bytes.Equal(dst.Bytes(), want) {
					t.Fatalf("got %d bytes, want %d", dst.Len(), len(want))
				}
			})
		}
	}
}
