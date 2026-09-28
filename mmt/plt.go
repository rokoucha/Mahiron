package mmt

import "net/netip"

// PLT is a Package List Table, carried in the PA message on PacketIDPA
// (ARIB STD-B60, 7.3.3.2), pointing to the packet_id of each package's MPT.
type PLT struct {
	Version      byte
	Packages     []PLTPackage
	IPDeliveries []PLTIPDelivery
}

type PLTPackage struct {
	PackageID []byte
	// Location is where the PA message carrying the package's MPT is sent.
	Location GeneralLocationInfo
}

// ServiceID returns the service_id of the package.
func (p *PLTPackage) ServiceID() uint16 { return packageServiceID(p.PackageID) }

// PLTIPDelivery is an IP data flow that delivers an IP service.
type PLTIPDelivery struct {
	TransportFileID uint32
	// LocationType is LocationTypeIPv4, LocationTypeIPv6 or
	// LocationTypeURL, and only the matching fields are set.
	LocationType    byte
	Source          netip.Addr
	Destination     netip.Addr
	DestinationPort uint16
	URL             string
	Descriptors     []Descriptor
}

// ParsePLT parses a PLT from a PA message. Byte slices alias t.
func ParsePLT(t Table) (*PLT, error) {
	body, err := t.body(func(id byte) bool { return id == TableIDPLT })
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	plt := &PLT{Version: t.Version()}
	for range r.u8() {
		pkg := PLTPackage{PackageID: r.bytes(int(r.u8()))}
		pkg.Location = readGeneralLocationInfo(r)
		plt.Packages = append(plt.Packages, pkg)
	}
	for range r.u8() {
		d := PLTIPDelivery{TransportFileID: r.u32(), LocationType: r.u8()}
		switch d.LocationType {
		case LocationTypeIPv4:
			if b := r.bytes(8); b != nil {
				d.Source, d.Destination = netip.AddrFrom4([4]byte(b[:4])), netip.AddrFrom4([4]byte(b[4:]))
			}
			d.DestinationPort = r.u16()
		case LocationTypeIPv6:
			if b := r.bytes(32); b != nil {
				d.Source, d.Destination = netip.AddrFrom16([16]byte(b[:16])), netip.AddrFrom16([16]byte(b[16:]))
			}
			d.DestinationPort = r.u16()
		case LocationTypeURL:
			d.URL = string(r.bytes(int(r.u8())))
		default:
			return nil, ErrInvalidSection
		}
		if d.Descriptors, err = parseDescriptors(r.bytes(int(r.u16()))); err != nil {
			return nil, err
		}
		if err := r.err(); err != nil {
			return nil, err
		}
		plt.IPDeliveries = append(plt.IPDeliveries, d)
	}
	return plt, r.err()
}
