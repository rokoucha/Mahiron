package ts

// Logo transmission descriptors and CDT payloads are tested against the
// current ARIB STD-B10 service-information representation.

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestParseLogoTransmissionDescriptorType1(t *testing.T) {
	desc := Descriptor{DescriptorTagLogoTransmission, 7, 0x01, 0xff, 0x2a, 0xfa, 0xbc, 0x12, 0x34}

	logo, err := ParseLogoTransmissionDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	if logo.TransmissionType != LogoTransmissionTypeCDTDirect || logo.LogoID != 0x12a || logo.LogoVersion != 0xabc || logo.DownloadDataID != 0x1234 {
		t.Fatalf("logo descriptor = %#v", logo)
	}
}

func TestServiceScanUsesLogoTransmissionDescriptor(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID: 100,
		descriptors: append(
			serviceDescriptor(1, nil, []byte{0x0e, 'L', 'O', 'G', 'O'}),
			DescriptorTagLogoTransmission, 7, 0x01, 0xff, 0x2a, 0xf0, 0x01, 0x12, 0x34,
		),
	}})
	scan := NewServiceScan()
	scan.Observe(buildPAT(t, map[uint16]uint16{100: 0x0100}))
	scan.Observe(section)
	got := scan.Services()
	if len(got) != 1 || got[0].LogoId != 0x12a {
		t.Fatalf("services = %#v", got)
	}
	if got[0].LogoVersion == nil || *got[0].LogoVersion != 1 {
		t.Fatalf("logo version = %v, want 1", got[0].LogoVersion)
	}
	if got[0].LogoDownloadDataId == nil || *got[0].LogoDownloadDataId != 0x1234 {
		t.Fatalf("logo download data id = %v, want 0x1234", got[0].LogoDownloadDataId)
	}
}

func TestParseCDTLogoImage(t *testing.T) {
	png := append([]byte(nil), pngSignature...)
	png = append(png, 0, 1, 2, 3)
	module := []byte{
		0x05,
		0xff, 0x2a,
		0xf0, 0x03,
		byte(len(png) >> 8), byte(len(png)),
	}
	module = append(module, png...)
	cdt := buildCDT(t, 0x1234, 0x5678, 5, module)

	parsed, err := ParseCDT(cdt)
	if err != nil {
		t.Fatal(err)
	}
	image, err := ParseCDTLogoImage(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if image.OriginalNetworkID != 0x5678 || image.DownloadDataID != 0x1234 || image.LogoID != 0x12a || image.LogoVersion != 3 || image.LogoType != 5 || !bytes.Equal(image.Data, png) {
		t.Fatalf("image = %#v", image)
	}
}

func TestParseCDTLogoImageReportsDeletion(t *testing.T) {
	cdt := buildCDT(t, 0x1234, 0x5678, 5, []byte{0x05, 0xff, 0x2a, 0xf0, 0x03, 0, 0})
	parsed, err := ParseCDT(cdt)
	if err != nil {
		t.Fatal(err)
	}
	image, err := ParseCDTLogoImage(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if !image.IsDeleted || len(image.Data) != 0 {
		t.Fatalf("image = %#v, want deletion", image)
	}
}

func TestParseCDTLogoImageRejectsNonPNG(t *testing.T) {
	cdt := buildCDT(t, 0x1234, 0x5678, 5, []byte{0x05, 0xff, 0x2a, 0xf0, 0x03, 0, 4, 'B', 'A', 'D', '!'})
	parsed, err := ParseCDT(cdt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCDTLogoImage(parsed); err == nil {
		t.Fatal("ParseCDTLogoImage succeeded for non-PNG data")
	}
}

func TestNormalizeARIBLogoPNGAddsPaletteAndTransparency(t *testing.T) {
	png := buildPalettePNG(t, false)

	normalized, err := NormalizeARIBLogoPNG(png)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(normalized, png) {
		t.Fatal("NormalizeARIBLogoPNG returned unchanged data")
	}
	if got, ok := pngChunkData(t, normalized, "PLTE"); !ok {
		t.Fatal("PLTE chunk was not inserted")
	} else if !bytes.Equal(got, aribCommonFixedColorPLTE) {
		t.Fatalf("PLTE = % x, want ARIB common fixed color palette", got)
	} else if len(got) != 128*3 {
		t.Fatalf("PLTE length = %d, want %d", len(got), 128*3)
	}
	if got, ok := pngChunkData(t, normalized, "tRNS"); !ok {
		t.Fatal("tRNS chunk was not inserted")
	} else if !bytes.Equal(got, aribCommonFixedColortRNS) {
		t.Fatalf("tRNS = % x, want ARIB common fixed color alpha table", got)
	} else if len(got) != 128 {
		t.Fatalf("tRNS length = %d, want 128", len(got))
	}
	if !pngChunkOrder(t, normalized, "IHDR", "PLTE", "tRNS", "IDAT", "IEND") {
		t.Fatalf("unexpected PNG chunk order: %v", pngChunkTypes(t, normalized))
	}
}

func TestARIBCommonFixedColorPaletteMatchesTRB14Appendix(t *testing.T) {
	tests := []struct {
		index      int
		r, g, b, a byte
	}{
		{0, 0, 0, 0, 255},
		{1, 255, 0, 0, 255},
		{7, 255, 255, 255, 255},
		{8, 0, 0, 0, 0},
		{15, 170, 170, 170, 255},
		{16, 0, 0, 85, 255},
		{64, 255, 255, 170, 255},
		{65, 0, 0, 0, 128},
		{79, 170, 170, 170, 128},
		{80, 0, 0, 85, 128},
		{127, 255, 255, 85, 128},
	}
	for _, tt := range tests {
		rgb := aribCommonFixedColorPLTE[tt.index*3 : tt.index*3+3]
		if rgb[0] != tt.r || rgb[1] != tt.g || rgb[2] != tt.b || aribCommonFixedColortRNS[tt.index] != tt.a {
			t.Fatalf("index %d = R,G,B,A %d,%d,%d,%d; want %d,%d,%d,%d",
				tt.index, rgb[0], rgb[1], rgb[2], aribCommonFixedColortRNS[tt.index], tt.r, tt.g, tt.b, tt.a)
		}
	}
}

func TestNormalizeARIBLogoPNGKeepsPNGWithPaletteUnchanged(t *testing.T) {
	png := buildPalettePNG(t, true)

	normalized, err := NormalizeARIBLogoPNG(png)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(normalized, png) {
		t.Fatal("NormalizeARIBLogoPNG changed PNG data that already has PLTE")
	}
}

func buildCDT(t *testing.T, downloadDataID, originalNetworkID uint16, sectionNumber byte, module []byte) Section {
	t.Helper()
	sectionLength := 10 + len(module) + 4
	s := make(Section, 3+sectionLength)
	s[0] = TableIDCDT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(downloadDataID >> 8)
	s[4] = byte(downloadDataID)
	s[5] = 0xc1
	s[6] = sectionNumber
	s[7] = sectionNumber
	s[8] = byte(originalNetworkID >> 8)
	s[9] = byte(originalNetworkID)
	s[10] = 0x01
	s[11] = 0xf0
	s[12] = 0x00
	copy(s[13:], module)
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
	return s
}

func buildPalettePNG(t *testing.T, includePLTE bool) []byte {
	t.Helper()
	var png []byte
	png = append(png, pngSignature...)

	ihdr := make([]byte, 13)
	binary.BigEndian.PutUint32(ihdr[0:4], 1)
	binary.BigEndian.PutUint32(ihdr[4:8], 1)
	ihdr[8] = 8
	ihdr[9] = 3
	png = appendTestPNGChunk(png, "IHDR", ihdr)
	if includePLTE {
		png = appendTestPNGChunk(png, "PLTE", aribCommonFixedColorPLTE)
	}
	png = appendTestPNGChunk(png, "IDAT", []byte{0x78, 0x9c, 0x63, 0x60, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01})
	png = appendTestPNGChunk(png, "IEND", nil)
	return png
}

func appendTestPNGChunk(dst []byte, chunkType string, chunkData []byte) []byte {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], uint32(len(chunkData)))
	dst = append(dst, scratch[:]...)
	dst = append(dst, chunkType...)
	dst = append(dst, chunkData...)
	crc := crc32.NewIEEE()
	_, _ = crc.Write([]byte(chunkType))
	_, _ = crc.Write(chunkData)
	binary.BigEndian.PutUint32(scratch[:], crc.Sum32())
	dst = append(dst, scratch[:]...)
	return dst
}

func pngChunkData(t *testing.T, png []byte, wantType string) ([]byte, bool) {
	t.Helper()
	pos := len(pngSignature)
	for pos+12 <= len(png) {
		chunkLen := int(binary.BigEndian.Uint32(png[pos : pos+4]))
		chunkDataStart := pos + 8
		chunkDataEnd := chunkDataStart + chunkLen
		chunkEnd := chunkDataEnd + 4
		if chunkEnd > len(png) {
			t.Fatalf("invalid PNG chunk at %d", pos)
		}
		if string(png[pos+4:pos+8]) == wantType {
			return png[chunkDataStart:chunkDataEnd], true
		}
		pos = chunkEnd
	}
	return nil, false
}

func pngChunkTypes(t *testing.T, png []byte) []string {
	t.Helper()
	var types []string
	pos := len(pngSignature)
	for pos+12 <= len(png) {
		chunkLen := int(binary.BigEndian.Uint32(png[pos : pos+4]))
		chunkEnd := pos + 8 + chunkLen + 4
		if chunkEnd > len(png) {
			t.Fatalf("invalid PNG chunk at %d", pos)
		}
		types = append(types, string(png[pos+4:pos+8]))
		pos = chunkEnd
	}
	return types
}

func pngChunkOrder(t *testing.T, png []byte, want ...string) bool {
	t.Helper()
	got := pngChunkTypes(t, png)
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
