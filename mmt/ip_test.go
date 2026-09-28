package mmt

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParseUDPPacketIPv6(t *testing.T) {
	src := netip.MustParseAddr("2401:dbc0:1000::2")
	dst := netip.MustParseAddr("ff02::101")
	packet := mmttest.IPv6UDP(src.As16(), dst.As16(), 123, 123, []byte("ntp"))
	got, err := ParseUDPPacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	if got.Compressed || got.Source != netip.AddrPortFrom(src, 123) || got.Destination != netip.AddrPortFrom(dst, 123) || string(got.Payload) != "ntp" {
		t.Fatalf("packet = %+v", got)
	}
}

func TestParseUDPPacketCompressed(t *testing.T) {
	got, err := ParseUDPPacket(mmttest.CompressedIP(0x123, 5, []byte("mmtp")))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Compressed || got.ContextID != 0x123 || got.SequenceNumber != 5 || got.Source.IsValid() || string(got.Payload) != "mmtp" {
		t.Fatalf("packet = %+v", got)
	}
}

func TestParseUDPPacketCompressedIPv6Header(t *testing.T) {
	src := netip.MustParseAddr("2401:dbc0:1000::2")
	dst := netip.MustParseAddr("ff3e::1")
	header := []byte{0x60, 0, 0, 0, 17, 255}
	header = append(header, src.AsSlice()...)
	header = append(header, dst.AsSlice()...)
	header = append(header, 0x12, 0x34, 0x56, 0x78)
	data := append([]byte{0x00, 0x10, CompressedHeaderIPv6UDP}, header...)
	got, err := ParseUDPPacket(mmttest.TLV(TLVPacketTypeCompressedIP, append(data, "mmtp"...)))
	if err != nil {
		t.Fatal(err)
	}
	if got.ContextID != 1 || got.Source != netip.AddrPortFrom(src, 0x1234) || got.Destination != netip.AddrPortFrom(dst, 0x5678) || string(got.Payload) != "mmtp" {
		t.Fatalf("packet = %+v", got)
	}
}

func TestParseUDPPacketRejectsOtherPackets(t *testing.T) {
	if _, err := ParseUDPPacket(mmttest.TLV(TLVPacketTypeNull, []byte{0xff})); !errors.Is(err, ErrNotUDP) {
		t.Fatalf("null packet error = %v, want ErrNotUDP", err)
	}
	if _, err := ParseUDPPacket(mmttest.TLV(TLVPacketTypeCompressedIP, []byte{0, 0x10, 0x60, 0x60})); !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("truncated header error = %v, want ErrInvalidPacket", err)
	}
	if _, err := ParseUDPPacket(mmttest.TLV(TLVPacketTypeCompressedIP, []byte{0, 0x10, 0x42})); !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("unknown header type error = %v, want ErrInvalidPacket", err)
	}
}

func TestFlowTrackerResolvesCompressedContexts(t *testing.T) {
	src := [16]byte{0x20, 0x01}
	dst := [16]byte{0xff, 0x3e, 15: 0x01}
	var flows FlowTracker
	for _, tc := range []struct {
		packet []byte
		ok     bool
	}{
		{mmttest.CompressedIP(7, 0, []byte{1}), false},
		{mmttest.CompressedIPv6UDP(7, 1, src, dst, 1000, 51216, []byte{2}), true},
		{mmttest.CompressedIP(7, 2, []byte{3}), true},
		{mmttest.CompressedIP(8, 0, []byte{4}), false},
	} {
		u, err := ParseUDPPacket(TLVPacket(tc.packet))
		if err != nil {
			t.Fatal(err)
		}
		flow, ok := flows.Flow(u)
		if ok != tc.ok {
			t.Fatalf("Flow(%x) ok = %v, want %v", tc.packet, ok, tc.ok)
		}
		want := IPDataFlow{Source: netip.AddrFrom16(src), Destination: netip.AddrFrom16(dst), DestinationPort: 51216}
		if ok && flow != want {
			t.Fatalf("flow = %+v, want %+v", flow, want)
		}
	}
}
