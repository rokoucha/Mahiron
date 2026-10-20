package mmt

// MHBIT carries broadcaster membership and SI transmission parameters.
type MHBIT struct {
	SectionHeader
	FirstDescriptors []Descriptor
	Broadcasters     []MHBITBroadcaster
}

type MHBITBroadcaster struct {
	BroadcasterID byte
	Descriptors   []Descriptor
}

func ParseMHBIT(s Section) (*MHBIT, error) {
	header, body, err := parseLongSection(s, 2, TableIDMHBIT)
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	first := r.bytes(int(r.u16() & 0x0fff))
	if err := r.err(); err != nil {
		return nil, err
	}
	descriptors, err := parseDescriptors(first)
	if err != nil {
		return nil, err
	}
	bit := &MHBIT{SectionHeader: header, FirstDescriptors: descriptors}
	for r.ok && len(r.b) > 0 {
		broadcaster := MHBITBroadcaster{BroadcasterID: r.u8()}
		data := r.bytes(int(r.u16() & 0x0fff))
		if err := r.err(); err != nil {
			return nil, err
		}
		broadcaster.Descriptors, err = parseDescriptors(data)
		if err != nil {
			return nil, err
		}
		bit.Broadcasters = append(bit.Broadcasters, broadcaster)
	}
	return bit, r.err()
}
