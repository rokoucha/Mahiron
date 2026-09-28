package mmt

import (
	"errors"
	"slices"
	"time"
)

var ErrInvalidSection = errors.New("mmt: invalid section")

// Section is an MPEG-2 style section carried in an M2 section message, an M2
// short section message or a TLV transmission control signal packet.
type Section []byte

// TableID returns the table identifier.
func (s Section) TableID() byte { return s[0] }

// SectionSyntaxIndicator reports whether the section uses the long syntax.
func (s Section) SectionSyntaxIndicator() bool { return s[1]&0x80 != 0 }

// SectionLength returns the 12-bit section_length value.
func (s Section) SectionLength() int { return int(s[1]&0x0f)<<8 | int(s[2]) }

// TotalLength returns the total byte length including the 3-byte prefix and CRC.
func (s Section) TotalLength() int { return 3 + s.SectionLength() }

// ValidateCRC checks the section CRC32-MPEG-2.
func (s Section) ValidateCRC() bool {
	if len(s) < 7 || s.TotalLength() > len(s) {
		return false
	}
	return crc32MPEG2(s[:s.TotalLength()]) == 0
}

// SectionHeader holds the common fields of a long-syntax section.
type SectionHeader struct {
	TableID              byte
	TableIDExtension     uint16
	VersionNumber        byte
	CurrentNextIndicator bool
	SectionNumber        byte
	LastSectionNumber    byte
}

// parseLongSection validates a long-syntax section with one of the given
// table IDs and returns its header and the bytes between the header and the
// CRC.
func parseLongSection(s Section, minBody int, tableIDs ...byte) (SectionHeader, []byte, error) {
	if len(s) < 8+minBody+4 || !s.SectionSyntaxIndicator() || !s.ValidateCRC() || !slices.Contains(tableIDs, s.TableID()) {
		return SectionHeader{}, nil, ErrInvalidSection
	}
	end := s.TotalLength() - 4
	if end < 8+minBody {
		return SectionHeader{}, nil, ErrInvalidSection
	}
	return SectionHeader{
		TableID:              s[0],
		TableIDExtension:     uint16(s[3])<<8 | uint16(s[4]),
		VersionNumber:        (s[5] >> 1) & 0x1f,
		CurrentNextIndicator: s[5]&0x01 != 0,
		SectionNumber:        s[6],
		LastSectionNumber:    s[7],
	}, s[8:end], nil
}

func crc32MPEG2(data []byte) uint32 {
	crc := uint32(0xffffffff)
	for _, b := range data {
		crc = (crc << 8) ^ crc32MPEG2Table[byte(crc>>24)^b]
	}
	return crc
}

var crc32MPEG2Table = func() (table [256]uint32) {
	for i := range table {
		crc := uint32(i) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

var jst = time.FixedZone("JST", 9*60*60)

// parseMJDTime decodes a 40-bit MJD + BCD time in JST. All bits set means
// undefined and yields the zero time.
func parseMJDTime(b []byte) (time.Time, error) {
	if allOnes(b[:5]) {
		return time.Time{}, nil
	}
	hms, err := parseBCDTime(b[2:5], 23)
	if err != nil {
		return time.Time{}, err
	}
	mjd := int(b[0])<<8 | int(b[1])
	// MJD 0 is 1858-11-17; time.Date normalizes the overflowing day.
	return time.Date(1858, time.November, 17+mjd, 0, 0, 0, 0, jst).Add(hms), nil
}

// parseBCDDuration decodes a 24-bit BCD duration. All bits set means
// undefined and yields zero.
func parseBCDDuration(b []byte) (time.Duration, error) {
	if allOnes(b[:3]) {
		return 0, nil
	}
	return parseBCDTime(b[:3], 99)
}

func parseBCDTime(b []byte, maxHour int) (time.Duration, error) {
	hour, ok1 := decodeBCD(b[0])
	minute, ok2 := decodeBCD(b[1])
	second, ok3 := decodeBCD(b[2])
	if !ok1 || !ok2 || !ok3 || hour > maxHour || minute > 59 || second > 59 {
		return 0, ErrInvalidSection
	}
	return time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second, nil
}

func decodeBCD(b byte) (int, bool) {
	high, low := int(b>>4), int(b&0x0f)
	return high*10 + low, high <= 9 && low <= 9
}

func allOnes(b []byte) bool {
	for _, v := range b {
		if v != 0xff {
			return false
		}
	}
	return true
}
