package mmt

import "time"

// MHTOT is an MH-Time Offset Table section (ARIB STD-B60, 7.3.3.14), carried
// in an M2 short section message.
type MHTOT struct {
	JSTTime     time.Time
	Descriptors []Descriptor
}

// ParseMHTOT parses an MH-TOT section. Descriptors alias s.
func ParseMHTOT(s Section) (*MHTOT, error) {
	if len(s) < 14 || s.TableID() != TableIDMHTOT || s.SectionSyntaxIndicator() || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	jstTime, err := parseMJDTime(s[3:8])
	if err != nil || jstTime.IsZero() {
		return nil, ErrInvalidSection
	}
	loopLen := int(s[8]&0x0f)<<8 | int(s[9])
	if 10+loopLen != s.TotalLength()-4 {
		return nil, ErrInvalidSection
	}
	descriptors, err := parseDescriptors(s[10 : 10+loopLen])
	if err != nil {
		return nil, err
	}
	return &MHTOT{JSTTime: jstTime, Descriptors: descriptors}, nil
}
