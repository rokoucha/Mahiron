package mmt

import "net/netip"

// IPDataFlow identifies an IP data flow by source, destination and
// destination port, as MMT_general_location_info and PLT point at one
// (ARIB STD-B60, 7.3.2). The zero value is an unknown flow.
type IPDataFlow struct {
	Source          netip.Addr
	Destination     netip.Addr
	DestinationPort uint16
}

// IsValid reports whether the flow is known.
func (f IPDataFlow) IsValid() bool { return f.Destination.IsValid() }

// flowOf returns the flow of a UDP datagram that carries its addresses.
func flowOf(u *UDPPacket) (IPDataFlow, bool) {
	if !u.Destination.IsValid() {
		return IPDataFlow{}, false
	}
	return IPDataFlow{Source: u.Source.Addr(), Destination: u.Destination.Addr(), DestinationPort: u.Destination.Port()}, true
}

// FlowTracker resolves the IP data flow of UDP datagrams, remembering each
// context's flow from its full-header packets, since a header-compressed
// packet usually carries only its context ID (ARIB STD-B32 Part 3, 3.7).
type FlowTracker struct {
	contexts map[uint16]IPDataFlow
}

// Flow returns the flow of u. It reports false for a compressed packet whose
// context has not yet carried a full header.
func (t *FlowTracker) Flow(u *UDPPacket) (IPDataFlow, bool) {
	flow, ok := flowOf(u)
	if !u.Compressed {
		return flow, ok
	}
	if ok {
		if t.contexts == nil {
			t.contexts = make(map[uint16]IPDataFlow)
		}
		t.contexts[u.ContextID] = flow
		return flow, true
	}
	flow, ok = t.contexts[u.ContextID]
	return flow, ok
}

// PacketKey identifies an MMTP packet sequence; packet_id alone is not
// enough, since advanced wide band CS can reuse it across IP data flows.
type PacketKey struct {
	Flow     IPDataFlow
	PacketID uint16
}
