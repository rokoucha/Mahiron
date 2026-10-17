package probe

import (
	"fmt"
	"time"

	"github.com/21S1298001/mahiron/ts"
)

type tsState struct {
	relative uint8
	demux    *ts.Demuxer
	services map[uint16]*Service
	stats    map[uint16]*Statistics
}

func newTSState(relative uint8) *tsState {
	return &tsState{relative: relative, demux: ts.NewDemuxer(), services: map[uint16]*Service{}, stats: map[uint16]*Statistics{}}
}
func (t *tsState) service(id uint16) *Service {
	s := t.services[id]
	if s == nil {
		s = &Service{ID: id, RelativeTS: t.relative, Components: []Component{}}
		t.services[id] = s
	}
	return s
}
func (t *tsState) feed(p ts.Packet, elapsed time.Duration) uint64 {
	stats := t.stats[p.PID()]
	if stats == nil {
		stats = &Statistics{}
		t.stats[p.PID()] = stats
	}
	if p.TransportErrorIndicator() {
		stats.TransportErrors++
	}
	if p.HasPayload() && p.ValidPayloadOffset() && !p.TransportErrorIndicator() {
		stats.observe(p[3]>>6, elapsed)
	}
	if p[3]>>6 != 0 {
		return 0
	}
	sections, err := t.demux.FeedWithPID(p)
	var failures uint64
	if err != nil {
		failures++
	}
	for _, item := range sections {
		section := item.Section
		if len(section) < 8 || section[5]&1 == 0 {
			continue
		}
		switch section.TableID() {
		case ts.TableIDPAT:
			pat, e := ts.ParsePAT(section)
			if e != nil {
				failures++
				continue
			}
			for id, pid := range pat.Programs {
				s := t.service(id)
				s.Listed = true
				if s.MapID != nil && *s.MapID != pid {
					s.MapReceived = false
					s.MapComplete = false
					s.Components = []Component{}
					s.CA = nil
					s.CASignaled = false
					s.PCRPID = nil
				}
				s.MapID = &pid
				v := pat.TransportStreamID
				s.StreamID = &v
			}
		case ts.TableIDSDT0:
			sdt, e := ts.ParseSDT(section)
			if e != nil {
				failures++
				continue
			}
			for _, entry := range sdt.Services {
				s := t.service(entry.ServiceID)
				sid, nid, ca := sdt.TransportStreamID, sdt.OriginalNetworkID, entry.FreeCAMode
				s.StreamID = &sid
				s.NetworkID = &nid
				s.CADeclared = &ca
				for _, d := range entry.Descriptors {
					if d.Tag() == ts.DescriptorTagService {
						if v, e := ts.ParseServiceDescriptor(d); e == nil {
							s.Name = v.ServiceName
							s.Provider = v.ServiceProviderName
							typ := v.ServiceType
							s.Type = &typ
						}
					}
				}
			}
		case ts.TableIDPMT:
			pmt, e := ts.ParsePMT(section)
			if e != nil {
				failures++
				continue
			}
			s := t.service(pmt.ProgramNumber)
			s.MapReceived = true
			s.MapComplete = true
			pid, pcr := item.PID, pmt.PCRPID
			s.MapID = &pid
			s.PCRPID = &pcr
			s.CA = tsCA(pmt.Descriptors)
			s.CASignaled = len(s.CA) > 0
			s.Components = []Component{}
			for _, es := range pmt.Elements {
				kind, codec := tsKind(es.StreamType)
				c := Component{Kind: kind, Codec: codec, ID: es.ElementaryPID, CA: tsCA(es.Descriptors)}
				for _, d := range es.Descriptors {
					b := d.Data()
					switch d.Tag() {
					case 0x52:
						if len(b) >= 1 {
							v := uint16(b[0])
							c.Tag = &v
						}
					case 0x0a:
						if len(b) >= 3 {
							c.Language = string(b[:3])
						}
					case ts.DescriptorTagAudioComponent:
						if len(b) >= 9 {
							c.Language = string(b[6:9])
						}
					case ts.DescriptorTagDataComponent:
						if len(b) >= 2 {
							id := uint16(b[0])<<8 | uint16(b[1])
							if id == 8 || id == 0x12 {
								c.Kind = "subtitle"
							} else if id == 0xc {
								c.Kind = "data"
							}
							c.Codec = fmt.Sprintf("ARIB data 0x%04x", id)
						}
					}
				}
				c.CASignaled = len(c.CA) > 0
				s.Components = append(s.Components, c)
			}
		}
	}
	return failures
}
func tsCA(ds []ts.Descriptor) []ConditionalAccess {
	var out []ConditionalAccess
	for _, d := range ds {
		b := d.Data()
		if d.Tag() == ts.DescriptorTagCA && len(b) >= 4 {
			out = append(out, ConditionalAccess{SystemID: uint16(b[0])<<8 | uint16(b[1]), PID: uint16(b[2]&0x1f)<<8 | uint16(b[3])})
		}
	}
	return out
}
func tsKind(t byte) (string, string) {
	switch t {
	case 1, 2:
		return "video", "MPEG-2"
	case 0x1b:
		return "video", "H.264"
	case 0x24:
		return "video", "HEVC"
	case 3, 4:
		return "audio", "MPEG audio"
	case 0x0f:
		return "audio", "AAC"
	case 0x11:
		return "audio", "AAC LATM"
	case 0x0b, 0x0d:
		return "data", "DSM-CC"
	default:
		return "other", fmt.Sprintf("stream_type 0x%02x", t)
	}
}
func (t *tsState) finish(r *Report) {
	fillCA := func(ca []ConditionalAccess) {
		for i := range ca {
			if ca[i].PID == ts.PIDNull {
				continue
			}
			if st := t.stats[ca[i].PID]; st != nil {
				ca[i].Packets = st.Packets
			}
		}
	}
	for _, s := range t.services {
		if t.demux.PATReady() && !t.demux.HasService(s.ID) {
			continue
		}
		fillCA(s.CA)
		for i := range s.Components {
			c := &s.Components[i]
			if st := t.stats[c.ID]; st != nil {
				c.Stats = *st
			}
			fillCA(c.CA)
		}
		r.Services = append(r.Services, s)
	}
}
