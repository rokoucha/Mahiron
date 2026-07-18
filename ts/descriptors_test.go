package ts

import "testing"

func TestParseAdditionalAribBXMLInfo(t *testing.T) {
	// data carousel, entry point, default BML version, event id 0xf.
	info, err := ParseAdditionalAribBXMLInfo([]byte{0x30, 0xf0, 0xf8, 0xa0})
	if err != nil {
		t.Fatal(err)
	}
	if !info.EntryPointFlag || info.EntryPointInfo == nil || !info.EntryPointInfo.AutoStartFlag || info.EntryPointInfo.DocumentResolution != 0 {
		t.Fatalf("entry point = %#v", info.EntryPointInfo)
	}
	if info.AdditionalAribCarouselInfo == nil || info.AdditionalAribCarouselInfo.DataEventID != 0x0f || !info.AdditionalAribCarouselInfo.EventSectionFlag || !info.AdditionalAribCarouselInfo.OnDemandRetrievalFlag || info.AdditionalAribCarouselInfo.FileStorableFlag || info.AdditionalAribCarouselInfo.StartPriority != 1 {
		t.Fatalf("carousel = %#v", info.AdditionalAribCarouselInfo)
	}
}

func TestParseDataComponentDescriptor(t *testing.T) {
	desc := Descriptor{DescriptorTagDataComponent, 4, 0, 0x0c, 0x30, 0}
	value, err := ParseDataComponentDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	if value.DataComponentID != 0x000c || string(value.AdditionalDataComponentInfo) != string([]byte{0x30, 0}) {
		t.Fatalf("descriptor = %#v", value)
	}
}
