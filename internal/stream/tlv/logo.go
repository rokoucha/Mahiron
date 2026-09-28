package tlv

import (
	"bytes"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/mmt"
)

// logoAssembler joins logos from MH-CDT (ARIB TR-B39 Part 1, 5.2), whose
// sections and range come from the MH-logo transmission descriptors of
// MH-SDT[actual] and [other] (any stream may carry another service's logo).
type logoAssembler struct {
	// ranges maps a download data ID to the logos its sections carry.
	ranges map[uint16]map[logoRange]bool
	// modules holds the latest data module of each MH-CDT section.
	modules map[cdtSectionKey]cdtModule
	// emitted remembers the logos already reported.
	emitted map[logoKey]bool
}

type logoRange struct {
	logoID, version uint16
	logoType        byte
	start, count    byte
}

type logoKey struct {
	networkID, downloadDataID, logoID, version uint16
	logoType                                   byte
}

type cdtSectionKey struct {
	downloadDataID uint16
	sectionNumber  byte
}

type cdtModule struct {
	networkID uint16
	version   byte
	data      []byte
}

func newLogoAssembler() *logoAssembler {
	return &logoAssembler{
		ranges:  map[uint16]map[logoRange]bool{},
		modules: map[cdtSectionKey]cdtModule{},
		emitted: map[logoKey]bool{},
	}
}

// accepts reports whether a signal is one the assembler reads.
func (a *logoAssembler) accepts(sig signal) bool {
	if sig.TLVSI || len(sig.Section) == 0 {
		return false
	}
	switch sig.Section.TableID() {
	case mmt.TableIDMHSDTActual, mmt.TableIDMHSDTOther, mmt.TableIDMHCDT:
		return true
	}
	return false
}

// Observe adds a signal and returns the logos it completes, 2K logos
// completed with the common fixed palette.
func (a *logoAssembler) Observe(sig signal) []model.Logo {
	if !a.accepts(sig) {
		return nil
	}
	var downloadDataIDs []uint16
	switch sig.Section.TableID() {
	case mmt.TableIDMHCDT:
		cdt, err := mmt.ParseMHCDT(sig.Section)
		if err != nil || cdt.DataType != mmt.CDTDataTypeLogo {
			return nil
		}
		key := cdtSectionKey{cdt.DownloadDataID, cdt.SectionNumber}
		if current, ok := a.modules[key]; ok && current.version == cdt.VersionNumber && bytes.Equal(current.data, cdt.DataModule) {
			return nil
		}
		a.modules[key] = cdtModule{networkID: cdt.OriginalNetworkID, version: cdt.VersionNumber, data: bytes.Clone(cdt.DataModule)}
		downloadDataIDs = []uint16{cdt.DownloadDataID}
	default:
		sdt, err := mmt.ParseMHSDT(sig.Section)
		if err != nil {
			return nil
		}
		for _, service := range sdt.Services {
			for _, d := range service.Descriptors {
				if d.Tag != mmt.DescriptorTagMHLogoTransmission {
					continue
				}
				logo, err := mmt.ParseMHLogoTransmissionDescriptor(d)
				if err != nil || logo.TransmissionType != mmt.LogoTransmissionTypeCDT1 {
					continue
				}
				if a.ranges[logo.DownloadDataID] == nil {
					a.ranges[logo.DownloadDataID] = map[logoRange]bool{}
				}
				for _, sections := range logo.LogoTypes {
					r := logoRange{logoID: logo.LogoID, version: logo.LogoVersion, logoType: sections.LogoType, start: sections.StartSectionNumber, count: sections.NumOfSections}
					if !a.ranges[logo.DownloadDataID][r] {
						a.ranges[logo.DownloadDataID][r] = true
						downloadDataIDs = append(downloadDataIDs, logo.DownloadDataID)
					}
				}
			}
		}
	}
	var logos []model.Logo
	for _, downloadDataID := range downloadDataIDs {
		logos = append(logos, a.complete(downloadDataID)...)
	}
	return logos
}

// complete joins the logos of a download data ID whose sections all
// arrived.
func (a *logoAssembler) complete(downloadDataID uint16) []model.Logo {
	var logos []model.Logo
	for r := range a.ranges[downloadDataID] {
		if r.count == 0 {
			continue
		}
		modules := make([][]byte, 0, r.count)
		var networkID uint16
		for n := range r.count {
			module, ok := a.modules[cdtSectionKey{downloadDataID, r.start + n}]
			if !ok {
				break
			}
			networkID = module.networkID
			modules = append(modules, module.data)
		}
		if len(modules) != int(r.count) {
			continue
		}
		joined, err := mmt.JoinLogoData(modules)
		if err != nil || joined.LogoType != r.logoType || joined.LogoID != r.logoID || joined.LogoVersion != r.version {
			continue
		}
		key := logoKey{networkID: networkID, downloadDataID: downloadDataID, logoID: r.logoID, version: r.version, logoType: r.logoType}
		if a.emitted[key] {
			continue
		}
		data, err := isdb.NormalizeARIBLogoPNG(joined.Data)
		if err != nil {
			continue
		}
		a.emitted[key] = true
		logos = append(logos, model.Logo{
			NetworkID:      networkID,
			LogoID:         r.logoID,
			Version:        r.version,
			DownloadDataID: downloadDataID,
			LogoType:       r.logoType,
			Data:           data,
		})
	}
	return logos
}
