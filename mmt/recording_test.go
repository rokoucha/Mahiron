package mmt

import (
	"bytes"
	"errors"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// recordingEnv names the environment variable listing TLV recordings
// (.mmts) to parse, separated by the OS path list separator. Recordings
// cannot be committed, so the test is skipped without it.
const recordingEnv = "MAHIRON_TEST_MMTS"

func TestRecordings(t *testing.T) {
	paths := filepath.SplitList(os.Getenv(recordingEnv))
	if len(paths) == 0 {
		t.Skipf("%s is not set", recordingEnv)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			checkRecording(t, parseRecording(t, f))
		})
	}
}

type recordingSummary struct {
	tlvNITStreams int
	// mptPacketIDs are the packet_ids PLT designates for the PA messages
	// carrying MPT.
	mptPacketIDs     map[uint16]bool
	pltServiceIDs    map[uint16]bool
	mptServiceID     uint16
	mptAssetTypes    map[string]bool
	actualServices   map[uint16]string
	pfEvents         int
	scheduleEvents   int
	shortEvents      int
	extendedEvents   int
	videoComponents  int
	totCount         int
	fragmentedEvents int
	// logoDescriptors are the MH-logo transmission descriptors of
	// MH-SDT[actual], and cdtModules the MH-CDT logo data modules by
	// download_data_id and section_number.
	logoDescriptors []*MHLogoTransmissionDescriptor
	cdtModules      map[[2]uint16][]byte
}

func parseRecording(t *testing.T, r io.Reader) *recordingSummary {
	t.Helper()
	s := &recordingSummary{
		mptPacketIDs:   map[uint16]bool{},
		pltServiceIDs:  map[uint16]bool{},
		mptAssetTypes:  map[string]bool{},
		actualServices: map[uint16]string{},
		cdtModules:     map[[2]uint16][]byte{},
	}
	reader := NewTLVReader(r)
	var assembler MessageAssembler
	var flows FlowTracker
	for {
		p, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return s
		}
		if err != nil {
			t.Fatal(err)
		}
		if p.Type() == TLVPacketTypeTransmissionControl {
			if section := Section(p.Data()); len(section) > 0 && section.TableID() == TableIDTLVNITActual {
				nit, err := ParseTLVNIT(section)
				if err != nil {
					t.Fatalf("TLV-NIT: %v", err)
				}
				s.tlvNITStreams = max(s.tlvNITStreams, len(nit.TLVStreams))
			}
			continue
		}
		u, err := ParseUDPPacket(p)
		if errors.Is(err, ErrNotUDP) || err == nil && !u.Compressed {
			continue // null packets and NTP
		}
		if err != nil {
			t.Fatalf("UDP: %v", err)
		}
		flow, ok := flows.Flow(u)
		if !ok {
			continue // a context before its first full header
		}
		m, err := ParseMMTPPacket(u.Payload)
		if err != nil {
			t.Fatalf("MMTP: %v", err)
		}
		if m.PayloadType == MMTPPayloadTypeSignaling {
			if sp, err := ParseSignalingPayload(m.Payload); err == nil && m.PacketID == PacketIDMHEIT && sp.FragmentationIndicator == FragmentLast {
				s.fragmentedEvents++
			}
		}
		messages, err := assembler.Feed(flow, m)
		if err != nil {
			t.Fatalf("messages of packet_id %#04x: %v", m.PacketID, err)
		}
		for _, message := range messages {
			s.addMessage(t, m.PacketID, message)
		}
	}
}

func (s *recordingSummary) addMessage(t *testing.T, packetID uint16, m Message) {
	t.Helper()
	switch {
	case m.ID() == MessageIDPA && (packetID == PacketIDPA || s.mptPacketIDs[packetID]):
		pa, err := ParsePAMessage(m)
		if err != nil {
			t.Fatalf("PA message: %v", err)
		}
		for _, table := range pa.Tables {
			switch {
			case table.TableID() == TableIDPLT && packetID == PacketIDPA:
				plt, err := ParsePLT(table)
				if err != nil {
					t.Fatalf("PLT: %v", err)
				}
				for _, pkg := range plt.Packages {
					if pkg.Location.LocationType == LocationTypeSameFlow {
						s.mptPacketIDs[pkg.Location.PacketID] = true
						s.pltServiceIDs[pkg.ServiceID()] = true
					}
				}
			case IsMPT(table.TableID()):
				mpt, err := ParseMPT(table)
				if err != nil {
					t.Fatalf("MPT: %v", err)
				}
				s.mptServiceID = mpt.ServiceID()
				for _, asset := range mpt.Assets {
					s.mptAssetTypes[asset.AssetType] = true
				}
			}
		}
	case m.ID() == MessageIDM2Section || m.ID() == MessageIDM2ShortSection:
		section, err := m.Section()
		if err != nil {
			t.Fatalf("section of packet_id %#04x: %v", packetID, err)
		}
		s.addSection(t, section)
	}
}

func (s *recordingSummary) addSection(t *testing.T, section Section) {
	t.Helper()
	switch id := section.TableID(); {
	case IsMHEITPF(id) || IsMHEITSchedule(id):
		eit, err := ParseMHEIT(section)
		if err != nil {
			t.Fatalf("MH-EIT %#02x: %v", id, err)
		}
		for _, event := range eit.Events {
			if IsMHEITPF(id) {
				s.pfEvents++
			} else {
				s.scheduleEvents++
			}
			for _, d := range event.Descriptors {
				var err error
				switch d.Tag {
				case DescriptorTagMHShortEvent:
					s.shortEvents++
					_, err = ParseMHShortEventDescriptor(d)
				case DescriptorTagMHExtendedEvent:
					s.extendedEvents++
					_, err = ParseMHExtendedEventDescriptor(d)
				case DescriptorTagVideoComponent:
					s.videoComponents++
					_, err = ParseVideoComponentDescriptor(d)
				case DescriptorTagMHAudioComponent:
					_, err = ParseMHAudioComponentDescriptor(d)
				case DescriptorTagMHContent:
					_, err = ParseMHContentDescriptor(d)
				case DescriptorTagMHEventGroup:
					_, err = ParseMHEventGroupDescriptor(d)
				case DescriptorTagMHSeries:
					_, err = ParseMHSeriesDescriptor(d)
				}
				if err != nil {
					t.Fatalf("MH-EIT descriptor %#04x: %v", d.Tag, err)
				}
			}
		}
	case id == TableIDMHSDTActual || id == TableIDMHSDTOther:
		sdt, err := ParseMHSDT(section)
		if err != nil {
			t.Fatalf("MH-SDT: %v", err)
		}
		for _, service := range sdt.Services {
			for _, d := range service.Descriptors {
				switch d.Tag {
				case DescriptorTagMHService:
					sd, err := ParseMHServiceDescriptor(d)
					if err != nil {
						t.Fatalf("MH-service descriptor: %v", err)
					}
					if id == TableIDMHSDTActual {
						s.actualServices[service.ServiceID] = sd.ServiceName
					}
				case DescriptorTagMHLogoTransmission:
					logo, err := ParseMHLogoTransmissionDescriptor(d)
					if err != nil {
						t.Fatalf("MH-logo transmission descriptor: %v", err)
					}
					if id == TableIDMHSDTActual && logo.TransmissionType == LogoTransmissionTypeCDT1 {
						s.logoDescriptors = append(s.logoDescriptors, logo)
					}
				}
			}
		}
	case id == TableIDMHCDT:
		cdt, err := ParseMHCDT(section)
		if err != nil {
			t.Fatalf("MH-CDT: %v", err)
		}
		if cdt.DataType == CDTDataTypeLogo {
			s.cdtModules[[2]uint16{cdt.DownloadDataID, uint16(cdt.SectionNumber)}] = bytes.Clone(cdt.DataModule)
		}
	case id == TableIDMHTOT:
		if _, err := ParseMHTOT(section); err != nil {
			t.Fatalf("MH-TOT: %v", err)
		}
		s.totCount++
	}
}

func checkRecording(t *testing.T, s *recordingSummary) {
	t.Helper()
	t.Logf("%+v", *s)
	if s.tlvNITStreams == 0 {
		t.Error("no TLV-NIT with TLV streams")
	}
	if !s.pltServiceIDs[s.mptServiceID] {
		t.Errorf("PLT services = %v, want the MPT service %d found through PLT", s.pltServiceIDs, s.mptServiceID)
	}
	if !s.mptAssetTypes["hev1"] && !s.mptAssetTypes["hvc1"] {
		t.Errorf("MPT asset types = %v, want an HEVC asset", s.mptAssetTypes)
	}
	if name, ok := s.actualServices[s.mptServiceID]; !ok || name == "" {
		t.Errorf("MH-SDT[actual] services = %v, want the MPT service %d", s.actualServices, s.mptServiceID)
	}
	if s.pfEvents == 0 || s.scheduleEvents == 0 || s.shortEvents == 0 || s.videoComponents == 0 {
		t.Error("MH-EIT lacks p/f or schedule events with short event and video component descriptors")
	}
	if s.fragmentedEvents == 0 {
		t.Error("no fragmented MH-EIT message was joined")
	}
	if s.totCount == 0 {
		t.Error("no MH-TOT")
	}
	if logos := s.logos(t); logos == 0 {
		t.Error("no logo was joined from MH-CDT")
	}
}

// logos joins the logos whose MH-CDT sections were all received and checks
// them against the MH-logo transmission descriptor. MH-CDT is sent rarely,
// so a short recording may hold only some of the logo types.
func (s *recordingSummary) logos(t *testing.T) int {
	t.Helper()
	joined := map[[2]uint16]bool{}
	for _, d := range s.logoDescriptors {
		for _, sections := range d.LogoTypes {
			key := [2]uint16{d.DownloadDataID, uint16(sections.LogoType)}
			if joined[key] {
				continue
			}
			var modules [][]byte
			for n := range uint16(sections.NumOfSections) {
				if module, ok := s.cdtModules[[2]uint16{d.DownloadDataID, uint16(sections.StartSectionNumber) + n}]; ok {
					modules = append(modules, module)
				}
			}
			if len(modules) == 0 || len(modules) != int(sections.NumOfSections) {
				continue
			}
			logo, err := JoinLogoData(modules)
			if err != nil {
				t.Fatalf("logo type %d: %v", sections.LogoType, err)
			}
			if logo.LogoType != sections.LogoType || logo.LogoID != d.LogoID || logo.LogoVersion != d.LogoVersion {
				t.Fatalf("logo = type %d id %d version %d, want %+v of %+v", logo.LogoType, logo.LogoID, logo.LogoVersion, sections, d)
			}
			if !bytes.HasPrefix(logo.Data, []byte("\x89PNG\r\n\x1a\n")) {
				t.Fatalf("logo type %d is not PNG", logo.LogoType)
			}
			// The 2K logo omits PLTE and needs the common fixed palette.
			if logo.LogoType != LogoType2K {
				if _, err := png.Decode(bytes.NewReader(logo.Data)); err != nil {
					t.Fatalf("logo type %d: %v", logo.LogoType, err)
				}
			}
			t.Logf("logo type %d id %d version %d: %d bytes", logo.LogoType, logo.LogoID, logo.LogoVersion, len(logo.Data))
			joined[key] = true
		}
	}
	return len(joined)
}
