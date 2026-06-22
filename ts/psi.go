package ts

import "errors"

// PSI common constants and helpers.

// TableID constants for SI/PSI tables used in ARIB broadcasts.
const (
	TableIDPAT            = 0x00
	TableIDCAT            = 0x01
	TableIDPMT            = 0x02
	TableIDSDT0           = 0x42 // actual TS
	TableIDSDT1           = 0x46 // other TS
	TableIDEITPF0         = 0x4E // present/following, actual TS
	TableIDEITPF1         = 0x4F // present/following, other TS
	TableIDEITSStart      = 0x50 // schedule, actual TS
	TableIDEITSEnd        = 0x5F // schedule, actual TS
	TableIDEITSOtherStart = 0x60 // schedule, other TS
	TableIDEITSOtherEnd   = 0x6F // schedule, other TS
	TableIDCDT            = 0xC8 // common data table
	TableIDNIT0           = 0x40 // actual network
	TableIDNIT1           = 0x41 // other network
)

var (
	ErrInvalidSection = errors.New("ts: invalid section")
)

// IsEITPF reports whether the table_id is an EIT present/following table.
func IsEITPF(tableID byte) bool {
	return tableID == TableIDEITPF0 || tableID == TableIDEITPF1
}

// IsEITS reports whether the table_id is an EIT schedule table.
func IsEITS(tableID byte) bool {
	return (tableID >= TableIDEITSStart && tableID <= TableIDEITSEnd) ||
		(tableID >= TableIDEITSOtherStart && tableID <= TableIDEITSOtherEnd)
}

// SectionHeader holds common fields from the long section syntax.
type SectionHeader struct {
	TableID                byte
	SectionSyntaxIndicator bool
	SectionLength          int
	TransportStreamID      uint16
	OriginalNetworkID      uint16
	ServiceID              uint16
	VersionNumber          byte
	CurrentNextIndicator   bool
	SectionNumber          byte
	LastSectionNumber      byte
}

// ParseSectionHeader parses the common header of a long-syntax section.
// Table-specific fields are parsed by each table parser.
func ParseSectionHeader(s Section) (SectionHeader, error) {
	if len(s) < 8 || s.TotalLength() > len(s) {
		return SectionHeader{}, ErrInvalidSection
	}
	return SectionHeader{
		TableID:                s.TableID(),
		SectionSyntaxIndicator: s.SectionSyntaxIndicator(),
		SectionLength:          s.SectionLength(),
		TransportStreamID:      uint16(s[3])<<8 | uint16(s[4]),
		OriginalNetworkID:      uint16(s[3])<<8 | uint16(s[4]),
		ServiceID:              uint16(s[3])<<8 | uint16(s[4]),
		VersionNumber:          (s[5] >> 1) & 0x1f,
		CurrentNextIndicator:   s[5]&0x01 != 0,
		SectionNumber:          s[6],
		LastSectionNumber:      s[7],
	}, nil
}
