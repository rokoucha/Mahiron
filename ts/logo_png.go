package ts

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var (
	pngChunkIHDR = [4]byte{'I', 'H', 'D', 'R'}
	pngChunkPLTE = [4]byte{'P', 'L', 'T', 'E'}
	pngChunktRNS = [4]byte{'t', 'R', 'N', 'S'}
	pngChunkIEND = [4]byte{'I', 'E', 'N', 'D'}
)

// ARIB STD-B24 permits palette-index PNG data without a PLTE chunk when the
// receiver refers to an external CLUT. Store service logos as ordinary PNGs by
// materializing the receiver common fixed colors from ARIB TR-B14 Appendix-1.
var aribCommonFixedColorPLTE, aribCommonFixedColortRNS = buildARIBCommonFixedColorChunks()

func NormalizeARIBLogoPNG(data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, pngSignature) {
		return nil, ErrInvalidSection
	}

	pos := len(pngSignature)
	ihdrEnd := -1
	colorType := byte(0xff)
	hasPLTE := false
	hasIEND := false

	for {
		if pos+12 > len(data) {
			return nil, ErrInvalidSection
		}
		chunkLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		chunkTypeStart := pos + 4
		chunkDataStart := pos + 8
		chunkDataEnd := chunkDataStart + chunkLen
		chunkEnd := chunkDataEnd + 4
		if chunkLen < 0 || chunkDataEnd < chunkDataStart || chunkEnd > len(data) {
			return nil, ErrInvalidSection
		}

		var chunkType [4]byte
		copy(chunkType[:], data[chunkTypeStart:chunkDataStart])
		switch chunkType {
		case pngChunkIHDR:
			if chunkLen != 13 || ihdrEnd != -1 {
				return nil, ErrInvalidSection
			}
			colorType = data[chunkDataStart+9]
			ihdrEnd = chunkEnd
		case pngChunkPLTE:
			hasPLTE = true
		case pngChunkIEND:
			hasIEND = true
		}

		pos = chunkEnd
		if hasIEND {
			if pos != len(data) {
				return nil, ErrInvalidSection
			}
			break
		}
	}

	if ihdrEnd == -1 {
		return nil, ErrInvalidSection
	}
	if colorType != 3 || hasPLTE {
		return data, nil
	}

	out := make([]byte, 0, len(data)+12+len(aribCommonFixedColorPLTE)+12+len(aribCommonFixedColortRNS))
	out = append(out, data[:ihdrEnd]...)
	out = appendPNGChunk(out, pngChunkPLTE, aribCommonFixedColorPLTE)
	out = appendPNGChunk(out, pngChunktRNS, aribCommonFixedColortRNS)
	out = append(out, data[ihdrEnd:]...)
	return out, nil
}

func appendPNGChunk(dst []byte, chunkType [4]byte, chunkData []byte) []byte {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], uint32(len(chunkData)))
	dst = append(dst, scratch[:]...)
	dst = append(dst, chunkType[:]...)
	dst = append(dst, chunkData...)

	crc := crc32.NewIEEE()
	_, _ = crc.Write(chunkType[:])
	_, _ = crc.Write(chunkData)
	binary.BigEndian.PutUint32(scratch[:], crc.Sum32())
	dst = append(dst, scratch[:]...)
	return dst
}

func buildARIBCommonFixedColorChunks() ([]byte, []byte) {
	type color struct {
		r, g, b, a byte
	}
	compat := []color{
		{0, 0, 0, 255},
		{255, 0, 0, 255},
		{0, 255, 0, 255},
		{255, 255, 0, 255},
		{0, 0, 255, 255},
		{255, 0, 255, 255},
		{0, 255, 255, 255},
		{255, 255, 255, 255},
		{170, 0, 0, 255},
		{0, 170, 0, 255},
		{170, 170, 0, 255},
		{0, 0, 170, 255},
		{170, 0, 170, 255},
		{0, 170, 170, 255},
		{170, 170, 170, 255},
	}
	compatRGB := make(map[[3]byte]struct{}, len(compat))
	for _, c := range compat {
		compatRGB[[3]byte{c.r, c.g, c.b}] = struct{}{}
	}

	colors := make([]color, 0, 128)
	colors = append(colors, compat[:8]...)
	colors = append(colors, color{0, 0, 0, 0})
	colors = append(colors, compat[8:]...)

	levels := []byte{0, 85, 170, 255}
	for _, r := range levels {
		for _, g := range levels {
			for _, b := range levels {
				if _, ok := compatRGB[[3]byte{r, g, b}]; ok {
					continue
				}
				colors = append(colors, color{r, g, b, 255})
			}
		}
	}

	for _, c := range compat {
		colors = append(colors, color{c.r, c.g, c.b, 128})
	}
	for _, r := range levels {
		for _, g := range levels {
			for _, b := range levels {
				rgb := [3]byte{r, g, b}
				if _, ok := compatRGB[rgb]; ok || rgb == [3]byte{255, 255, 170} {
					continue
				}
				colors = append(colors, color{r, g, b, 128})
			}
		}
	}

	plte := make([]byte, 0, len(colors)*3)
	trns := make([]byte, 0, len(colors))
	for _, c := range colors {
		plte = append(plte, c.r, c.g, c.b)
		trns = append(trns, c.a)
	}
	return plte, trns
}
