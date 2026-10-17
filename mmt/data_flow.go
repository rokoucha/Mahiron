package mmt

import "net/netip"

const DescriptorTagIPDataFlow = 0x801A

// IPDataFlowEntry associates a service's flow label with its UDP endpoints.
// Flow ID zero identifies the flow carrying its MPT (STD-B60 7.4.3.29).
type IPDataFlowEntry struct {
	ID         byte
	Flow       IPDataFlow
	SourcePort uint16
}

func ParseIPDataFlowDescriptor(d Descriptor) ([]IPDataFlowEntry, error) {
	if d.Tag != DescriptorTagIPDataFlow {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	flags := r.u8()
	size := 4
	if flags&0x80 != 0 {
		size = 16
	}
	var entries []IPDataFlowEntry
	for range flags & 0x7f {
		id := r.u8()
		src, ok1 := netip.AddrFromSlice(r.bytes(size))
		dst, ok2 := netip.AddrFromSlice(r.bytes(size))
		sourcePort, destinationPort := r.u16(), r.u16()
		if !ok1 || !ok2 {
			return nil, ErrInvalidSection
		}
		entries = append(entries, IPDataFlowEntry{ID: id, SourcePort: sourcePort, Flow: IPDataFlow{Source: src, Destination: dst, DestinationPort: destinationPort}})
	}
	if err := r.err(); err != nil {
		return nil, err
	}
	if len(r.b) != 0 {
		return nil, ErrInvalidSection
	}
	return entries, nil
}
