package mmt

import (
	"net/netip"
	"testing"
)

func TestIPDataFlowDescriptor(t *testing.T) {
	data := []byte{1, 0, 192, 0, 2, 1, 239, 0, 0, 1, 0x12, 0x34, 0x56, 0x78}
	entries, err := ParseIPDataFlowDescriptor(Descriptor{Tag: DescriptorTagIPDataFlow, Data: data})
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	e := entries[0]
	if e.ID != 0 || e.SourcePort != 0x1234 || e.Flow.Source != netip.MustParseAddr("192.0.2.1") || e.Flow.Destination != netip.MustParseAddr("239.0.0.1") || e.Flow.DestinationPort != 0x5678 {
		t.Fatalf("entry: %+v", e)
	}
	for n := range len(data) {
		if _, err := ParseIPDataFlowDescriptor(Descriptor{Tag: DescriptorTagIPDataFlow, Data: data[:n]}); err == nil {
			t.Fatalf("accepted truncated descriptor length %d", n)
		}
	}
}
