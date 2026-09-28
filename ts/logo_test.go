package ts

// Logo transmission descriptors and CDT payloads are tested against the
// current ARIB STD-B10 service-information representation.

import (
	"bytes"
	"encoding/binary"
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

func TestParseLogoTransmissionDescriptorSimpleLogo(t *testing.T) {
	desc := Descriptor{DescriptorTagLogoTransmission, 5, 0x03, 0x0e, 'N', 'H', 'K'}

	logo, err := ParseLogoTransmissionDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	if logo.TransmissionType != LogoTransmissionTypeSimple || logo.SimpleLogo != "ＮＨＫ" {
		t.Fatalf("logo descriptor = %#v", logo)
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

func TestParseLogoDataModule(t *testing.T) {
	png := append([]byte(nil), pngSignature...)
	png = append(png, 1, 2, 3)
	module := []byte{
		0x05,
		0x00, 0x02,
		0xff, 0x2a,
		0x02,
		0x00, 0x04, 0x40, 0x10, 0x00, 0x65,
		0x00, 0x04, 0x40, 0x10, 0x00, 0x66,
		byte(len(png) >> 8), byte(len(png)),
	}
	module = append(module, png...)
	module = append(module,
		0xff, 0x2b,
		0x01,
		0x00, 0x04, 0xff, 0xff, 0xff, 0xff,
		0x00, 0x00,
	)

	images, err := ParseLogoDataModule(module)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 2 {
		t.Fatalf("images = %#v", images)
	}
	if images[0].LogoType != 5 || images[0].LogoID != 0x12a || len(images[0].Services) != 2 || !bytes.Equal(images[0].Data, png) {
		t.Fatalf("service logo = %#v", images[0])
	}
	if images[0].Services[1].ServiceID != 0x66 {
		t.Fatalf("services = %#v", images[0].Services)
	}
	if !images[1].IsNetwork || !images[1].IsDeleted {
		t.Fatalf("network logo = %#v", images[1])
	}
}

func TestDSMCCLogoCarouselReassemblesLogoModule(t *testing.T) {
	png := append([]byte(nil), pngSignature...)
	png = append(png, 9)
	module := []byte{
		0x05,
		0x00, 0x01,
		0xff, 0x2a,
		0x01,
		0x00, 0x04, 0x40, 0x10, 0x00, 0x65,
		byte(len(png) >> 8), byte(len(png)),
	}
	module = append(module, png...)
	dii := buildDSMCCDII(t, 0x12345678, 32, 0x2001, uint32(len(module)), 7, []byte{0x02, 0x07, 'L', 'O', 'G', 'O', '-', '0', '5'})
	ddb := buildDSMCCDDB(t, 0x12345678, 0x2001, 7, 0, module)

	parsedDII, err := ParseDSMCCDII(dii)
	if err != nil {
		t.Fatal(err)
	}
	parsedDDB, err := ParseDSMCCDDB(ddb)
	if err != nil {
		t.Fatal(err)
	}
	carousel := NewDSMCCLogoCarousel()
	carousel.ObserveDII(parsedDII)
	images, err := carousel.ObserveDDB(parsedDDB)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 1 || images[0].LogoID != 0x12a || images[0].LogoType != 5 || images[0].LogoVersion != 7 || images[0].DownloadID != 0x5678 {
		t.Fatalf("images = %#v", images)
	}
}

func TestParseSDTTCommonDataAnnouncements(t *testing.T) {
	section := buildSDTT(t, 0xfffe, 0x4031, 0x0004, 929, true, 0x0007, 0x12345678, true)

	got, err := ParseSDTTCommonDataAnnouncements(section)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("announcements = %#v", got)
	}
	if got[0].OriginalNetworkID != 0x0004 || got[0].TransportStreamID != 0x4031 || got[0].ServiceID != 929 ||
		got[0].VersionID != 7 || got[0].DownloadID != 0x12345678 {
		t.Fatalf("announcement = %#v", got[0])
	}
}

func TestParseSDTTCommonDataAnnouncementsIgnoresNonCommonAndNotCurrent(t *testing.T) {
	for _, section := range []Section{
		buildSDTT(t, 0x0101, 0x4031, 0x0004, 929, true, 0x0007, 0x12345678, true),
		buildSDTT(t, 0xfffe, 0x4031, 0x0004, 929, false, 0x0007, 0x12345678, true),
		buildSDTT(t, 0xfffe, 0x4031, 0x0004, 929, true, 0x0007, 0x12345678, false),
	} {
		got, err := ParseSDTTCommonDataAnnouncements(section)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("announcements = %#v, want none", got)
		}
	}
}

func TestParseSDTTCommonDataAnnouncementsIgnoresNonSatelliteCommonData(t *testing.T) {
	for _, tableIDExt := range []uint16{0xfffa, 0xfff8} {
		section := buildSDTT(t, tableIDExt, 0x4031, 0x0004, 929, true, 0x0007, 0x12345678, true)
		section[24] = 0xff
		writeCRC(section)

		got, err := ParseSDTTCommonDataAnnouncements(section)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("announcements = %#v, want none", got)
		}
	}
}

func TestParseSDTTCommonDataAnnouncementsIgnoresNonSatelliteNetwork(t *testing.T) {
	section := buildSDTT(t, 0xfffe, 0x4031, 0x7fe0, 929, true, 0x0007, 0x12345678, true)
	section[24] = 0xff
	writeCRC(section)

	got, err := ParseSDTTCommonDataAnnouncements(section)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("announcements = %#v, want none", got)
	}
}

func buildDSMCCDII(t *testing.T, downloadID uint32, blockSize, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte) Section {
	t.Helper()
	body := make([]byte, 0)
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], downloadID)
	body = append(body, scratch[:]...)
	binary.BigEndian.PutUint16(scratch[:2], blockSize)
	body = append(body, scratch[:2]...)
	body = append(body, 0, 0)
	body = append(body, 0, 0, 0, 0, 0, 0, 0, 0)
	body = append(body, 0, 0)
	body = append(body, 0, 1)
	binary.BigEndian.PutUint16(scratch[:2], moduleID)
	body = append(body, scratch[:2]...)
	binary.BigEndian.PutUint32(scratch[:], moduleSize)
	body = append(body, scratch[:]...)
	body = append(body, moduleVersion, byte(len(moduleInfo)))
	body = append(body, moduleInfo...)
	body = append(body, 0, 0)
	return buildDSMCCSection(t, TableIDDSMCCDII, 0x1002, 1, body)
}

func buildDSMCCDDB(t *testing.T, downloadID uint32, moduleID uint16, moduleVersion byte, blockNumber uint16, data []byte) Section {
	t.Helper()
	// downloadId lives in dsmccDownloadDataHeader(); the payload starts at
	// moduleId (ARIB STD-B24 Part 3 Tables 6-21 and 6-22).
	body := make([]byte, 0)
	var scratch [4]byte
	binary.BigEndian.PutUint16(scratch[:2], moduleID)
	body = append(body, scratch[:2]...)
	body = append(body, moduleVersion, 0xff)
	binary.BigEndian.PutUint16(scratch[:2], blockNumber)
	body = append(body, scratch[:2]...)
	body = append(body, data...)
	return buildDSMCCSection(t, TableIDDSMCCDDB, 0x1003, downloadID, body)
}

func buildDSMCCSection(t *testing.T, tableID byte, messageID uint16, headerID uint32, body []byte) Section {
	t.Helper()
	message := []byte{0x11, 0x03, byte(messageID >> 8), byte(messageID), byte(headerID >> 24), byte(headerID >> 16), byte(headerID >> 8), byte(headerID), 0xff, 0}
	message = append(message, byte(len(body)>>8), byte(len(body)))
	message = append(message, body...)
	sectionLength := 5 + len(message) + 4
	s := make(Section, 3+sectionLength)
	s[0] = tableID
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x00, 0x01
	s[5], s[6], s[7] = 0xc1, 0, 0
	copy(s[8:], message)
	writeCRC(s)
	return s
}

func buildSDTT(t *testing.T, tableIDExt, tsid, onid, sid uint16, current bool, versionID uint16, downloadID uint32, includeDownloadDescriptor bool) Section {
	t.Helper()
	var descriptors []byte
	if includeDownloadDescriptor {
		data := make([]byte, 19)
		data[0] = 0x00
		binary.BigEndian.PutUint32(data[1:5], 0x00001000)
		binary.BigEndian.PutUint32(data[5:9], downloadID)
		binary.BigEndian.PutUint32(data[9:13], 0x00000bb8)
		data[13], data[14], data[15] = 0, 0, 0
		data[16] = 0x00
		data[17] = 0x42
		data[18] = 0
		descriptors = append(descriptors, DescriptorTagDownloadContent, byte(len(data)))
		descriptors = append(descriptors, data...)
	} else {
		descriptors = append(descriptors, 0x40, 0x06, 0x00, 0x04, 0x40, 0x31, 0x03, 0xa1)
	}
	contentDescriptionLength := len(descriptors)
	content := make([]byte, 8, 8+len(descriptors))
	content[0] = 0x00
	content[1] = 0x00
	content[2] = byte(versionID >> 4)
	content[3] = byte(versionID<<4) | 0x03
	content[4] = byte(contentDescriptionLength >> 4)
	content[5] = byte(contentDescriptionLength<<4) | 0x08
	content[6] = 0x00
	content[7] = 0x0f
	content = append(content, descriptors...)

	sectionLength := 12 + len(content) + 4
	s := make(Section, 3+sectionLength)
	s[0] = TableIDSDTT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = byte(tableIDExt>>8), byte(tableIDExt)
	s[5] = 0xc0 | byte(boolToBit(current))
	s[6], s[7] = 0, 0
	s[8], s[9] = byte(tsid>>8), byte(tsid)
	s[10], s[11] = byte(onid>>8), byte(onid)
	s[12], s[13] = byte(sid>>8), byte(sid)
	s[14] = 1
	copy(s[15:], content)
	writeCRC(s)
	return s
}

func boolToBit(value bool) byte {
	if value {
		return 1
	}
	return 0
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
