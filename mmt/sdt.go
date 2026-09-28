package mmt

// MHSDT is an MH-Service Description Table section (ARIB STD-B60,
// 7.3.3.13).
type MHSDT struct {
	SectionHeader
	TLVStreamID       uint16
	OriginalNetworkID uint16
	Services          []MHSDTService
}

type MHSDTService struct {
	ServiceID           uint16
	EITUserDefinedFlags byte
	EITScheduleFlag     bool
	EITPresentFollowing bool
	RunningStatus       byte
	FreeCAMode          bool
	Descriptors         []Descriptor
}

// ParseMHSDT parses an MH-SDT[actual] or MH-SDT[other] section. Descriptors
// alias s.
func ParseMHSDT(s Section) (*MHSDT, error) {
	header, body, err := parseLongSection(s, 3, TableIDMHSDTActual, TableIDMHSDTOther)
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	sdt := &MHSDT{SectionHeader: header, TLVStreamID: header.TableIDExtension, OriginalNetworkID: r.u16()}
	r.u8() // reserved_future_use
	for r.ok && len(r.b) > 0 {
		service := MHSDTService{ServiceID: r.u16()}
		eitFlags := r.u8()
		service.EITUserDefinedFlags = (eitFlags >> 2) & 0x07
		service.EITScheduleFlag = eitFlags&0x02 != 0
		service.EITPresentFollowing = eitFlags&0x01 != 0
		flags := r.u16()
		service.RunningStatus = byte(flags >> 13)
		service.FreeCAMode = flags&0x1000 != 0
		descriptors := r.bytes(int(flags & 0x0fff))
		if err := r.err(); err != nil {
			return nil, err
		}
		if service.Descriptors, err = parseDescriptors(descriptors); err != nil {
			return nil, err
		}
		sdt.Services = append(sdt.Services, service)
	}
	return sdt, r.err()
}
