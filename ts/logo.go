package ts

import (
	"bytes"
	"context"
	"errors"
	"io"
)

const (
	LogoTransmissionTypeCDTDirect   = 0x01
	LogoTransmissionTypeCDTIndirect = 0x02
)

var pngSignature = []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}

type LogoDescriptor struct {
	TransmissionType byte
	LogoID           uint16
	LogoVersion      uint16
	DownloadDataID   uint16
}

func ParseLogoTransmissionDescriptor(d Descriptor) (*LogoDescriptor, error) {
	if d.Tag() != DescriptorTagLogoTransmission {
		return nil, errors.New("ts: unexpected descriptor tag")
	}
	data := d.Data()
	if len(data) < 1 {
		return nil, ErrInvalidSection
	}
	result := &LogoDescriptor{TransmissionType: data[0]}
	switch data[0] {
	case LogoTransmissionTypeCDTDirect:
		if len(data) < 7 {
			return nil, ErrInvalidSection
		}
		result.LogoID = uint16(data[1]&0x01)<<8 | uint16(data[2])
		result.LogoVersion = uint16(data[3]&0x0f)<<8 | uint16(data[4])
		result.DownloadDataID = uint16(data[5])<<8 | uint16(data[6])
	case LogoTransmissionTypeCDTIndirect:
		if len(data) < 3 {
			return nil, ErrInvalidSection
		}
		result.LogoID = uint16(data[1]&0x01)<<8 | uint16(data[2])
	default:
		return nil, ErrInvalidSection
	}
	return result, nil
}

func LogoDescriptorFromDescriptors(descriptors []Descriptor) *LogoDescriptor {
	for _, desc := range descriptors {
		if desc.Tag() != DescriptorTagLogoTransmission {
			continue
		}
		logo, err := ParseLogoTransmissionDescriptor(desc)
		if err == nil {
			return logo
		}
	}
	return nil
}

type CDT struct {
	DownloadDataID    uint16
	VersionNumber     byte
	SectionNumber     byte
	LastSectionNumber byte
	OriginalNetworkID uint16
	DataType          byte
	Descriptors       []Descriptor
	DataModule        []byte
}

func ParseCDT(s Section) (*CDT, error) {
	if len(s) < 17 || s.TableID() != TableIDCDT || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	sectionEnd := s.TotalLength() - 4
	descriptorsLoopLen := int(uint16(s[11]&0x0f)<<8 | uint16(s[12]))
	descStart := 13
	descEnd := descStart + descriptorsLoopLen
	if descEnd > sectionEnd {
		return nil, ErrInvalidSection
	}
	return &CDT{
		DownloadDataID:    uint16(s[3])<<8 | uint16(s[4]),
		VersionNumber:     (s[5] >> 1) & 0x1f,
		SectionNumber:     s[6],
		LastSectionNumber: s[7],
		OriginalNetworkID: uint16(s[8])<<8 | uint16(s[9]),
		DataType:          s[10],
		Descriptors:       ParseDescriptors(s[descStart:descEnd]),
		DataModule:        append([]byte(nil), s[descEnd:sectionEnd]...),
	}, nil
}

type LogoImage struct {
	OriginalNetworkID uint16
	LogoID            uint16
	LogoVersion       uint16
	DownloadDataID    uint16
	LogoType          byte
	Data              []byte
	IsDeleted         bool
}

func ParseCDTLogoImage(cdt *CDT) (*LogoImage, error) {
	data := cdt.DataModule
	if len(data) < 7 {
		return nil, ErrInvalidSection
	}
	size := int(uint16(data[5])<<8 | uint16(data[6]))
	if len(data) < 7+size {
		return nil, ErrInvalidSection
	}
	image := data[7 : 7+size]
	if size > 0 && !bytes.HasPrefix(image, pngSignature) {
		return nil, ErrInvalidSection
	}
	return &LogoImage{
		OriginalNetworkID: cdt.OriginalNetworkID,
		LogoID:            uint16(data[1]&0x01)<<8 | uint16(data[2]),
		LogoVersion:       uint16(data[3]&0x0f)<<8 | uint16(data[4]),
		DownloadDataID:    cdt.DownloadDataID,
		LogoType:          data[0],
		Data:              append([]byte(nil), image...),
		IsDeleted:         size == 0,
	}, nil
}

type LogoCollector struct{}

func NewLogoCollector() *LogoCollector {
	return &LogoCollector{}
}

func (c *LogoCollector) Collect(ctx context.Context, src io.Reader, observe func(*LogoImage) error) error {
	reader := NewPacketReader(src)
	scanner := NewSectionScanner(reader, PIDCDT)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		section, err := scanner.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		cdt, err := ParseCDT(section)
		if err != nil {
			continue
		}
		image, err := ParseCDTLogoImage(cdt)
		if err != nil {
			continue
		}
		if err := observe(image); err != nil {
			return err
		}
	}
}
