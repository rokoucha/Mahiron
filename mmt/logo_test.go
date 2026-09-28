package mmt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParseMHLogoTransmissionDescriptor(t *testing.T) {
	// A descriptor as observed in a real broadcast's MH-SDT.
	d := Descriptor{Tag: DescriptorTagMHLogoTransmission, Data: []byte{0x01, 0xfe, 0x65, 0xf0, 0x01, 0x00, 0x01, 0x05, 0x00, 0x01, 0x06, 0x01, 0x01, 0x07, 0x02, 0x01}}
	got, err := ParseMHLogoTransmissionDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	want := &MHLogoTransmissionDescriptor{
		TransmissionType: LogoTransmissionTypeCDT1,
		LogoID:           101,
		LogoVersion:      1,
		DownloadDataID:   1,
		LogoTypes: []LogoTypeSections{
			{LogoType: LogoType2K, StartSectionNumber: 0, NumOfSections: 1},
			{LogoType: LogoTypeSmall, StartSectionNumber: 1, NumOfSections: 1},
			{LogoType: LogoTypeLarge, StartSectionNumber: 2, NumOfSections: 1},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("descriptor = %+v, want %+v", got, want)
	}

	indirect, err := ParseMHLogoTransmissionDescriptor(Descriptor{Tag: DescriptorTagMHLogoTransmission, Data: []byte{0x02, 0xfe, 0x66}})
	if err != nil || indirect.TransmissionType != LogoTransmissionTypeCDT2 || indirect.LogoID != 102 {
		t.Fatalf("indirect descriptor = %+v, %v", indirect, err)
	}
	if _, err := ParseMHLogoTransmissionDescriptor(Descriptor{Tag: DescriptorTagMHLogoTransmission, Data: d.Data[:15]}); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("partial logo type entry error = %v, want ErrInvalidSection", err)
	}
}

func TestParseMHCDTAndJoinLogoData(t *testing.T) {
	image := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0xab}, 5000)...)
	parts := [][]byte{image[:4072], image[4072:]}
	var modules [][]byte
	for i, part := range parts {
		section := mmttest.Section(TableIDMHCDT, 1, 2, byte(1+i), 3, append([]byte{0x00, 0x0b, CDTDataTypeLogo, 0xf0, 0x00}, logoModule(LogoTypeSmall, 101, 1, part)...))
		cdt, err := ParseMHCDT(section)
		if err != nil {
			t.Fatal(err)
		}
		if cdt.DownloadDataID != 1 || cdt.VersionNumber != 2 || cdt.SectionNumber != byte(1+i) || cdt.OriginalNetworkID != 0x000b || cdt.DataType != CDTDataTypeLogo {
			t.Fatalf("MH-CDT = %+v", cdt)
		}
		modules = append(modules, cdt.DataModule)
	}
	logo, err := JoinLogoData(modules)
	if err != nil {
		t.Fatal(err)
	}
	if logo.LogoType != LogoTypeSmall || logo.LogoID != 101 || logo.LogoVersion != 1 || !bytes.Equal(logo.Data, image) {
		t.Fatalf("logo = type %d id %d version %d, %d bytes", logo.LogoType, logo.LogoID, logo.LogoVersion, len(logo.Data))
	}
}

func TestJoinLogoDataRejectsMismatchedModules(t *testing.T) {
	modules := map[string][][]byte{
		"no modules":        nil,
		"different type":    {logoModule(LogoTypeSmall, 101, 1, []byte{1}), logoModule(LogoTypeLarge, 101, 1, []byte{2})},
		"different version": {logoModule(LogoTypeLarge, 101, 1, []byte{1}), logoModule(LogoTypeLarge, 101, 2, []byte{2})},
		"truncated data":    {logoModule(LogoTypeLarge, 101, 1, []byte{1, 2})[:8]},
	}
	for name, m := range modules {
		if _, err := JoinLogoData(m); !errors.Is(err, ErrInvalidSection) {
			t.Errorf("%s: error = %v, want ErrInvalidSection", name, err)
		}
	}
}

func logoModule(logoType byte, logoID, logoVersion uint16, data []byte) []byte {
	b := []byte{logoType, 0xfe | byte(logoID>>8), byte(logoID), 0xf0 | byte(logoVersion>>8), byte(logoVersion), byte(len(data) >> 8), byte(len(data))}
	return append(b, data...)
}
