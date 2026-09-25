package isdb

import "testing"

func TestVideoComponentTypeRoundTrip(t *testing.T) {
	var defined []byte
	for v := 0; v < 256; v++ {
		c := byte(v)
		parsed, ok := ParseVideoComponentType(c)
		if !ok {
			continue
		}
		defined = append(defined, c)
		raw, ok := VideoComponentTypeToRaw(parsed.Resolution, parsed.Aspect)
		if !ok {
			t.Fatalf("VideoComponentTypeToRaw(%q, %q) not ok for %#02x", parsed.Resolution, parsed.Aspect, c)
		}
		if raw != c {
			t.Fatalf("round trip mismatch: %#02x -> %+v -> %#02x", c, parsed, raw)
		}
	}
	if len(defined) == 0 {
		t.Fatal("no defined component types")
	}
	// Spot-check documented families, including 2160p (0x91-0x94).
	for _, c := range []byte{0x01, 0x83, 0x91, 0xA1, 0xB1, 0xC1, 0xD1, 0xE1} {
		if _, ok := ParseVideoComponentType(c); !ok {
			t.Fatalf("ParseVideoComponentType(%#02x) not ok", c)
		}
	}
	if _, ok := ParseVideoComponentType(0x00); ok {
		t.Fatal("ParseVideoComponentType(0x00) unexpectedly ok")
	}
	if _, ok := VideoComponentTypeToRaw("8K", VideoAspect43); ok {
		t.Fatal("VideoComponentTypeToRaw with unknown resolution unexpectedly ok")
	}
}

func TestAudioCodecForTSStreamContent(t *testing.T) {
	codec, ok := AudioCodecForTSStreamContent(0x2)
	if !ok || codec != "aac" {
		t.Fatalf("TS stream_content 0x2 = %q, %v; want aac, true", codec, ok)
	}
	if _, ok := AudioCodecForTSStreamContent(0x3); ok {
		t.Fatal("TS stream_content 0x3 (MMT's AAC value) unexpectedly ok")
	}
}
