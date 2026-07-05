package ts

import (
	"errors"
	"fmt"
)

const (
	PIDPAT  = 0x0000
	PIDCAT  = 0x0001
	PIDNIT  = 0x0010
	PIDSDT  = 0x0011
	PIDEIT  = 0x0012
	PIDRST  = 0x0013
	PIDTOT  = 0x0014
	PIDDIT  = 0x001e
	PIDSIT  = 0x001f
	PIDSDTT = 0x0023
	PIDBIT  = 0x0024
	PIDCDT  = 0x0029
	PIDNull = 0x1fff
)

var ErrServiceNotFound = errors.New("ts: service not found")

// Demuxer incrementally parses one transport stream and keeps the shared
// program/PID state required to produce service streams. It is intentionally
// synchronous; callers own its goroutine and may fan the resulting packets out
// without reparsing the input for every service.
type Demuxer struct {
	assemblers    map[uint16]*SectionAssembler
	catEMM        map[uint16]bool
	catSections   tableSectionSet
	pat           *PAT
	patGeneration uint64
	patSections   tableSectionSet
	pmtByPID      map[uint16]uint16
	sectionPIDs   map[uint16]bool
	programs      map[uint16]*demuxProgram
	services      map[uint16]*demuxServiceOutput
}

// PIDSection is a complete PSI/SI section with the PID it was assembled from.
type PIDSection struct {
	PID     uint16
	Section Section
}

type demuxProgram struct {
	pmtPID      uint16
	pids        map[uint16]bool
	sectionPIDs map[uint16]bool
}

type demuxServiceOutput struct {
	generation uint64
	patCounter byte
	patIndex   int
	patPackets []Packet
}

// ServiceDemux is a per-output view of shared demux state. Its PAT continuity
// counter is independent from every other subscriber of the same service.
type ServiceDemux struct {
	demux     *Demuxer
	output    demuxServiceOutput
	serviceID uint16
}

// NewDemuxer creates an empty transport-stream demuxer.
func NewDemuxer() *Demuxer {
	return &Demuxer{
		assemblers:  map[uint16]*SectionAssembler{},
		catEMM:      map[uint16]bool{},
		pmtByPID:    map[uint16]uint16{},
		sectionPIDs: map[uint16]bool{},
		programs:    map[uint16]*demuxProgram{},
		services:    map[uint16]*demuxServiceOutput{},
	}
}

// Feed observes one normalized 188-byte packet and returns every complete
// PSI/SI section assembled from it. The returned sections have already passed
// CRC validation.
func (d *Demuxer) Feed(packet Packet) ([]Section, error) {
	pidSections, err := d.FeedWithPID(packet)
	if err != nil {
		return nil, err
	}
	sections := make([]Section, 0, len(pidSections))
	for _, section := range pidSections {
		sections = append(sections, section.Section)
	}
	return sections, nil
}

// FeedWithPID is like Feed, but retains the PID that carried each completed
// section.
func (d *Demuxer) FeedWithPID(packet Packet) ([]PIDSection, error) {
	if len(packet) != PacketSize || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
		return nil, nil
	}
	pid := packet.PID()
	if !d.shouldAssemble(pid) {
		return nil, nil
	}
	assembler := d.assemblers[pid]
	if assembler == nil {
		assembler = NewSectionAssembler(pid)
		d.assemblers[pid] = assembler
	}
	sections, err := assembler.FeedAll(packet)
	if err != nil {
		return nil, err
	}
	result := make([]PIDSection, 0, len(sections))
	for _, section := range sections {
		result = append(result, PIDSection{PID: pid, Section: section})
		switch section.TableID() {
		case TableIDPAT:
			if pid == PIDPAT {
				d.observePAT(section)
			}
		case TableIDCAT:
			if pid == PIDCAT {
				d.observeCAT(section)
			}
		case TableIDPMT:
			if serviceID, ok := d.pmtByPID[pid]; ok {
				d.observePMT(serviceID, section)
			}
		}
	}
	return result, nil
}

// ServicePacket returns the packet to emit for serviceID, or nil when the
// input packet does not belong to that service. PAT packets are replaced with
// a single-program PAT owned by this service output.
func (d *Demuxer) ServicePacket(serviceID uint16, packet Packet) Packet {
	return d.servicePacket(serviceID, d.serviceOutput(serviceID), packet)
}

// Service creates an independent output view for serviceID.
func (d *Demuxer) Service(serviceID uint16) *ServiceDemux {
	return &ServiceDemux{demux: d, serviceID: serviceID}
}

// Packet returns the packet to emit for this service, or nil.
func (s *ServiceDemux) Packet(packet Packet) Packet {
	if s == nil || s.demux == nil {
		return nil
	}
	return s.demux.servicePacket(s.serviceID, &s.output, packet)
}

func (d *Demuxer) servicePacket(serviceID uint16, output *demuxServiceOutput, packet Packet) Packet {
	if len(packet) != PacketSize || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
		return nil
	}
	program := d.programs[serviceID]
	if program == nil {
		return nil
	}
	if output.generation != d.patGeneration {
		d.rebuildPATOutput(serviceID, output)
	}
	pid := packet.PID()
	if pid == PIDPAT {
		if len(output.patPackets) == 0 {
			return nil
		}
		result := append(Packet(nil), output.patPackets[output.patIndex]...)
		output.patIndex = (output.patIndex + 1) % len(output.patPackets)
		result[3] = (result[3] & 0xf0) | (output.patCounter & 0x0f)
		output.patCounter = (output.patCounter + 1) & 0x0f
		return result
	}
	if commonServicePID(pid) || pid == program.pmtPID || program.pids[pid] || d.catEMM[pid] {
		return packet
	}
	return nil
}

// HasService reports whether a complete current PAT advertises serviceID.
func (d *Demuxer) HasService(serviceID uint16) bool {
	_, ok := d.programs[serviceID]
	return ok
}

// PATReady reports whether a complete current PAT has been assembled.
func (d *Demuxer) PATReady() bool { return d.pat != nil }

func (d *Demuxer) shouldAssemble(pid uint16) bool {
	if pid == PIDPAT || pid == PIDCAT || pid == PIDNIT || pid == PIDSDT || pid == PIDEIT || pid == PIDTOT || pid == PIDSDTT || pid == PIDCDT {
		return true
	}
	_, ok := d.pmtByPID[pid]
	if ok {
		return true
	}
	return d.sectionPIDs[pid]
}

func (d *Demuxer) observePAT(section Section) {
	reset, ready := d.patSections.add(section)
	if reset {
		d.pat = nil
	}
	if !ready {
		return
	}
	combined := &PAT{Programs: map[uint16]uint16{}}
	for _, current := range d.patSections.ordered() {
		pat, err := ParsePAT(current)
		if err != nil {
			return
		}
		combined.TransportStreamID = pat.TransportStreamID
		combined.VersionNumber = pat.VersionNumber
		combined.NetworkPID = pat.NetworkPID
		for serviceID, pmtPID := range pat.Programs {
			combined.Programs[serviceID] = pmtPID
		}
	}
	unchanged := samePAT(d.pat, combined)
	d.pat = combined
	if !unchanged {
		d.patGeneration++
	}
	newByPID := make(map[uint16]uint16, len(combined.Programs))
	for serviceID, pmtPID := range combined.Programs {
		newByPID[pmtPID] = serviceID
		program := d.programs[serviceID]
		if program == nil || program.pmtPID != pmtPID {
			d.programs[serviceID] = &demuxProgram{pmtPID: pmtPID, pids: map[uint16]bool{}}
		}
		if !unchanged {
			d.rebuildPAT(serviceID)
		}
	}
	for serviceID := range d.programs {
		if _, ok := combined.Programs[serviceID]; !ok {
			delete(d.programs, serviceID)
			delete(d.services, serviceID)
		}
	}
	for pid := range d.pmtByPID {
		if _, ok := newByPID[pid]; !ok {
			delete(d.assemblers, pid)
		}
	}
	d.pmtByPID = newByPID
}

func (d *Demuxer) observeCAT(section Section) {
	if len(section) < 12 || section.TotalLength() > len(section) || !section.ValidateCRC() {
		return
	}
	_, ready := d.catSections.add(section)
	if !ready {
		return
	}
	emm := map[uint16]bool{}
	for _, current := range d.catSections.ordered() {
		for _, desc := range ParseDescriptors(current[8 : current.TotalLength()-4]) {
			if desc.Tag() == DescriptorTagCA {
				if pid, ok := caPID(desc); ok {
					emm[pid] = true
				}
			}
		}
	}
	d.catEMM = emm
}

func (d *Demuxer) observePMT(serviceID uint16, section Section) {
	pmt, err := ParsePMT(section)
	if err != nil || pmt.ProgramNumber != serviceID {
		return
	}
	program := d.programs[serviceID]
	if program == nil {
		return
	}
	pids := map[uint16]bool{}
	sectionPIDs := map[uint16]bool{}
	if pmt.PCRPID != PIDNull {
		pids[pmt.PCRPID] = true
	}
	for _, desc := range pmt.Descriptors {
		if desc.Tag() == DescriptorTagCA {
			if pid, ok := caPID(desc); ok {
				pids[pid] = true
			}
		}
	}
	for _, elem := range pmt.Elements {
		pids[elem.ElementaryPID] = true
		if elem.StreamType == StreamTypeDSMCCDataCarousel {
			sectionPIDs[elem.ElementaryPID] = true
		}
		for _, desc := range elem.Descriptors {
			if desc.Tag() == DescriptorTagCA {
				if pid, ok := caPID(desc); ok {
					pids[pid] = true
				}
			}
		}
	}
	program.pids = pids
	program.sectionPIDs = sectionPIDs
	d.rebuildSectionPIDs()
}

func (d *Demuxer) rebuildSectionPIDs() {
	sectionPIDs := map[uint16]bool{}
	for _, program := range d.programs {
		for pid := range program.sectionPIDs {
			sectionPIDs[pid] = true
		}
	}
	for pid := range d.sectionPIDs {
		if !sectionPIDs[pid] {
			delete(d.assemblers, pid)
		}
	}
	d.sectionPIDs = sectionPIDs
}

func (d *Demuxer) serviceOutput(serviceID uint16) *demuxServiceOutput {
	output := d.services[serviceID]
	if output == nil {
		output = &demuxServiceOutput{}
		d.services[serviceID] = output
		d.rebuildPAT(serviceID)
	}
	return output
}

func (d *Demuxer) rebuildPAT(serviceID uint16) {
	if d.pat == nil {
		return
	}
	if _, ok := d.pat.Programs[serviceID]; !ok {
		return
	}
	output := d.services[serviceID]
	if output == nil {
		output = &demuxServiceOutput{}
		d.services[serviceID] = output
	}
	d.rebuildPATOutput(serviceID, output)
}

func (d *Demuxer) rebuildPATOutput(serviceID uint16, output *demuxServiceOutput) {
	if d.pat == nil || output == nil {
		return
	}
	pmtPID, ok := d.pat.Programs[serviceID]
	if !ok {
		return
	}
	section, err := BuildPATSection(d.pat.TransportStreamID, serviceID, pmtPID, d.pat.VersionNumber)
	if err != nil {
		panic(fmt.Sprintf("ts: build demux PAT: %v", err))
	}
	var packetCounter byte
	output.patPackets = packetizeSection(PIDPAT, section, &packetCounter)
	output.patIndex = 0
	output.generation = d.patGeneration
}

func samePAT(a, b *PAT) bool {
	if a == nil || b == nil || a.TransportStreamID != b.TransportStreamID || a.VersionNumber != b.VersionNumber || a.NetworkPID != b.NetworkPID || len(a.Programs) != len(b.Programs) {
		return false
	}
	for serviceID, pmtPID := range a.Programs {
		if b.Programs[serviceID] != pmtPID {
			return false
		}
	}
	return true
}

func commonServicePID(pid uint16) bool {
	switch pid {
	case PIDCAT, PIDNIT, PIDSDT, PIDEIT, PIDRST, PIDTOT, PIDDIT, PIDSIT, PIDSDTT, PIDBIT, PIDCDT:
		return true
	default:
		return false
	}
}

func caPID(desc Descriptor) (uint16, bool) {
	data := desc.Data()
	if len(data) < 4 {
		return 0, false
	}
	return uint16(data[2]&0x1f)<<8 | uint16(data[3]), true
}

func packetizeSection(pid uint16, section Section, counter *byte) []Packet {
	var packets []Packet
	remaining := []byte(section)
	first := true
	for first || len(remaining) > 0 {
		packet := make([]byte, PacketSize)
		for i := range packet {
			packet[i] = 0xff
		}
		packet[0] = SyncByte
		packet[1] = byte(pid >> 8)
		if first {
			packet[1] |= 0x40
		}
		packet[2] = byte(pid)
		packet[3] = 0x10 | (*counter & 0x0f)
		*counter = (*counter + 1) & 0x0f

		offset := 4
		if first {
			packet[offset] = 0
			offset++
		}
		n := copy(packet[offset:], remaining)
		remaining = remaining[n:]
		packets = append(packets, Packet(packet))
		first = false
	}
	return packets
}
