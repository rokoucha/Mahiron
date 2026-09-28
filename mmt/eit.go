package mmt

import "time"

// MHEIT is an MH-Event Information Table section (ARIB STD-B60, 7.3.3.9).
// Unlike EIT in ISDB-T/S, it only describes services of its own TLV stream.
type MHEIT struct {
	SectionHeader
	ServiceID                uint16
	TLVStreamID              uint16
	OriginalNetworkID        uint16
	SegmentLastSectionNumber byte
	LastTableID              byte
	Events                   []MHEITEvent
}

type MHEITEvent struct {
	EventID uint16
	// StartTime is zero and Duration is 0 when undefined.
	StartTime     time.Time
	Duration      time.Duration
	RunningStatus byte
	FreeCAMode    bool
	Descriptors   []Descriptor
}

// IsMHEITPF reports whether the table_id is MH-EIT[p/f].
func IsMHEITPF(tableID byte) bool { return tableID == TableIDMHEITPF }

// IsMHEITSchedule reports whether the table_id is MH-EIT[schedule].
func IsMHEITSchedule(tableID byte) bool {
	return tableID >= TableIDMHEITSStart && tableID <= TableIDMHEITSEnd
}

// ParseMHEIT parses an MH-EIT section. Descriptors alias s.
func ParseMHEIT(s Section) (*MHEIT, error) {
	if len(s) < 1 || (!IsMHEITPF(s.TableID()) && !IsMHEITSchedule(s.TableID())) {
		return nil, ErrInvalidSection
	}
	header, body, err := parseLongSection(s, 6, s.TableID())
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	eit := &MHEIT{
		SectionHeader:            header,
		ServiceID:                header.TableIDExtension,
		TLVStreamID:              r.u16(),
		OriginalNetworkID:        r.u16(),
		SegmentLastSectionNumber: r.u8(),
		LastTableID:              r.u8(),
	}
	for r.ok && len(r.b) > 0 {
		event := MHEITEvent{EventID: r.u16()}
		times := r.bytes(8)
		if times == nil {
			break
		}
		if event.StartTime, err = parseMJDTime(times[:5]); err != nil {
			return nil, err
		}
		if event.Duration, err = parseBCDDuration(times[5:8]); err != nil {
			return nil, err
		}
		flags := r.u16()
		event.RunningStatus = byte(flags >> 13)
		event.FreeCAMode = flags&0x1000 != 0
		descriptors := r.bytes(int(flags & 0x0fff))
		if err := r.err(); err != nil {
			return nil, err
		}
		if event.Descriptors, err = parseDescriptors(descriptors); err != nil {
			return nil, err
		}
		eit.Events = append(eit.Events, event)
	}
	return eit, r.err()
}
