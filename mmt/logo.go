package mmt

// Logo transmission types of the MH-logo transmission descriptor (ARIB
// STD-B60, Table 7-81). Advanced BS and advanced wide band CS operate only
// the CDT types (ARIB TR-B39 Part 1, 5.2.3).
const (
	LogoTransmissionTypeCDT1   = 0x01 // MH-CDT referred to directly
	LogoTransmissionTypeCDT2   = 0x02 // MH-CDT referred to through logo_id
	LogoTransmissionTypeSimple = 0x03
)

// Logo types (ARIB STD-B63 Annex 1, Table A1-1-1). The 2K logo is a PNG
// without PLTE, like isdb.NormalizeARIBLogoPNG expects; the small and large
// logos are complete PNGs.
const (
	LogoType2K    = 0x05 // 64x36
	LogoTypeSmall = 0x06 // 128x72
	LogoTypeLarge = 0x07 // 256x144
)

// CDTDataTypeLogo is the data_type of MH-CDT carrying logo data.
const CDTDataTypeLogo = 0x01

// MHLogoTransmissionDescriptor is the MH-logo transmission descriptor
// (0x8025) of MH-SDT.
type MHLogoTransmissionDescriptor struct {
	TransmissionType byte
	// LogoID is set for the CDT types, and LogoVersion, DownloadDataID and
	// LogoTypes only for LogoTransmissionTypeCDT1.
	LogoID         uint16
	LogoVersion    uint16
	DownloadDataID uint16
	LogoTypes      []LogoTypeSections
	// SimpleLogo is set only for LogoTransmissionTypeSimple.
	SimpleLogo string
}

// LogoTypeSections tells which MH-CDT sections carry the logo of a type.
type LogoTypeSections struct {
	LogoType           byte
	StartSectionNumber byte
	NumOfSections      byte
}

func ParseMHLogoTransmissionDescriptor(d Descriptor) (*MHLogoTransmissionDescriptor, error) {
	if d.Tag != DescriptorTagMHLogoTransmission {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	out := &MHLogoTransmissionDescriptor{TransmissionType: r.u8()}
	switch out.TransmissionType {
	case LogoTransmissionTypeCDT1:
		out.LogoID = r.u16() & 0x01ff
		out.LogoVersion = r.u16() & 0x0fff
		out.DownloadDataID = r.u16()
		if len(r.b)%3 != 0 {
			return nil, ErrInvalidSection
		}
		for len(r.b) > 0 {
			out.LogoTypes = append(out.LogoTypes, LogoTypeSections{LogoType: r.u8(), StartSectionNumber: r.u8(), NumOfSections: r.u8()})
		}
	case LogoTransmissionTypeCDT2:
		out.LogoID = r.u16() & 0x01ff
	case LogoTransmissionTypeSimple:
		out.SimpleLogo = string(r.b)
	}
	return out, r.err()
}

// MHCDT is an MH-Common Data Table section (ARIB STD-B60, 7.3.3.10).
type MHCDT struct {
	SectionHeader
	DownloadDataID    uint16
	OriginalNetworkID uint16
	DataType          byte
	Descriptors       []Descriptor
	// DataModule is this section's part of the download data. For logo data,
	// pass the modules of a logo's sections to JoinLogoData.
	DataModule []byte
}

// ParseMHCDT parses an MH-CDT section. Byte slices alias s.
func ParseMHCDT(s Section) (*MHCDT, error) {
	header, body, err := parseLongSection(s, 5, TableIDMHCDT)
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	cdt := &MHCDT{SectionHeader: header, DownloadDataID: header.TableIDExtension, OriginalNetworkID: r.u16(), DataType: r.u8()}
	if cdt.Descriptors, err = parseDescriptors(r.bytes(int(r.u16() & 0x0fff))); err != nil {
		return nil, err
	}
	cdt.DataModule = r.b
	return cdt, r.err()
}

// LogoData is a logo joined from MH-CDT data modules (ARIB STD-B63 Annex 1,
// Table A1-2-4).
type LogoData struct {
	LogoType    byte
	LogoID      uint16
	LogoVersion uint16
	Data        []byte
}

// JoinLogoData joins the data modules of the consecutive MH-CDT sections
// (LogoTypeSections) that carry one logo; each module repeats the header and
// carries the next part of the image (ARIB TR-B39 Part 1, 5.2.5).
func JoinLogoData(modules [][]byte) (*LogoData, error) {
	if len(modules) == 0 {
		return nil, ErrInvalidSection
	}
	var out *LogoData
	for _, module := range modules {
		r := newReader(module)
		part := LogoData{LogoType: r.u8(), LogoID: r.u16() & 0x01ff, LogoVersion: r.u16() & 0x0fff}
		data := r.bytes(int(r.u16()))
		if err := r.err(); err != nil {
			return nil, err
		}
		if out == nil {
			out = &part
		} else if part.LogoType != out.LogoType || part.LogoID != out.LogoID || part.LogoVersion != out.LogoVersion {
			return nil, ErrInvalidSection
		}
		out.Data = append(out.Data, data...)
	}
	return out, nil
}
