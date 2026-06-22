package ts

import (
	"bytes"
	"context"
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

func TestServiceScannerUsesLogoTransmissionDescriptor(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID: 100,
		descriptors: append(
			serviceDescriptor(1, nil, []byte{0x0e, 'L', 'O', 'G', 'O'}),
			DescriptorTagLogoTransmission, 7, 0x01, 0xff, 0x2a, 0xf0, 0x01, 0x12, 0x34,
		),
	}})
	input := append(sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}), 0), sectionPackets(PIDSDT, section, 0)...)

	got, err := NewServiceScanner().ScanServices(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
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
