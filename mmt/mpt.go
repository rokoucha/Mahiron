package mmt

import "net/netip"

// Location types of MMT_general_location_info (ARIB STD-B60, Table 7-10).
const (
	LocationTypeSameFlow   = 0x00
	LocationTypeIPv4       = 0x01
	LocationTypeIPv6       = 0x02
	LocationTypeMPEG2TS    = 0x03
	LocationTypeMPEG2TSIP6 = 0x04
	LocationTypeURL        = 0x05
)

// MPT is an MMT Package Table carried in a PA message (ARIB STD-B60,
// 7.3.3.1).
type MPT struct {
	TableID     byte
	Version     byte
	Mode        byte
	PackageID   []byte
	Descriptors []Descriptor
	Assets      []MPTAsset
}

// ServiceID returns the service_id of the package.
func (m *MPT) ServiceID() uint16 { return packageServiceID(m.PackageID) }

// packageServiceID returns the lower 16 bits of a package ID, which equal the
// service_id of the service.
func packageServiceID(id []byte) uint16 {
	switch n := len(id); n {
	case 0:
		return 0
	case 1:
		return uint16(id[0])
	default:
		return uint16(id[n-2])<<8 | uint16(id[n-1])
	}
}

type MPTAsset struct {
	IdentifierType byte
	AssetIDScheme  uint32
	AssetID        []byte
	// AssetType is a four-character code such as "hev1", "mp4a" or "stpp"
	// (ARIB STD-B60, Table 7-8).
	AssetType   string
	Locations   []GeneralLocationInfo
	Descriptors []Descriptor
}

// GeneralLocationInfo is MMT_general_location_info. Only the fields of its
// LocationType are set.
type GeneralLocationInfo struct {
	LocationType      byte
	PacketID          uint16
	Source            netip.Addr
	Destination       netip.Addr
	DestinationPort   uint16
	NetworkID         uint16
	TransportStreamID uint16
	PID               uint16
	URL               string
}

// IsMPT reports whether the table_id is a complete MPT or an MPT subset.
func IsMPT(tableID byte) bool { return tableID >= 0x11 && tableID <= TableIDMPT }

// ParseMPT parses an MPT from a PA message. Byte slices alias t.
func ParseMPT(t Table) (*MPT, error) {
	body, err := t.body(IsMPT)
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	mpt := &MPT{TableID: t.TableID(), Version: t.Version(), Mode: r.u8() & 0x03}
	mpt.PackageID = r.bytes(int(r.u8()))
	descriptors, err := parseDescriptors(r.bytes(int(r.u16())))
	if err != nil {
		return nil, err
	}
	mpt.Descriptors = descriptors
	for range r.u8() {
		asset := MPTAsset{IdentifierType: r.u8(), AssetIDScheme: r.u32()}
		asset.AssetID = r.bytes(int(r.u8()))
		asset.AssetType = string(r.bytes(4))
		if r.u8()&0x01 != 0 {
			// asset_clock_relation_id, then asset_timescale when flagged.
			r.u8()
			if r.u8()&0x01 != 0 {
				r.u32()
			}
		}
		for range r.u8() {
			asset.Locations = append(asset.Locations, readGeneralLocationInfo(r))
		}
		if asset.Descriptors, err = parseDescriptors(r.bytes(int(r.u16()))); err != nil {
			return nil, err
		}
		if err := r.err(); err != nil {
			return nil, err
		}
		mpt.Assets = append(mpt.Assets, asset)
	}
	return mpt, r.err()
}

func readGeneralLocationInfo(r *reader) GeneralLocationInfo {
	loc := GeneralLocationInfo{LocationType: r.u8()}
	switch loc.LocationType {
	case LocationTypeSameFlow:
		loc.PacketID = r.u16()
	case LocationTypeIPv4:
		if b := r.bytes(8); b != nil {
			loc.Source, loc.Destination = netip.AddrFrom4([4]byte(b[:4])), netip.AddrFrom4([4]byte(b[4:]))
		}
		loc.DestinationPort = r.u16()
		loc.PacketID = r.u16()
	case LocationTypeIPv6, LocationTypeMPEG2TSIP6:
		if b := r.bytes(32); b != nil {
			loc.Source, loc.Destination = netip.AddrFrom16([16]byte(b[:16])), netip.AddrFrom16([16]byte(b[16:]))
		}
		loc.DestinationPort = r.u16()
		if loc.LocationType == LocationTypeIPv6 {
			loc.PacketID = r.u16()
		} else {
			loc.PID = r.u16() & 0x1fff
		}
	case LocationTypeMPEG2TS:
		loc.NetworkID = r.u16()
		loc.TransportStreamID = r.u16()
		loc.PID = r.u16() & 0x1fff
	case LocationTypeURL:
		loc.URL = string(r.bytes(int(r.u8())))
	default:
		// The size of an unknown location type is unknown.
		r.ok = false
	}
	return loc
}
