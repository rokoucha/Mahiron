package probe

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"time"

	"github.com/21S1298001/mahiron/mmt"
)

type mptPart struct {
	version    byte
	components []Component
	keys       []mmt.PacketKey
	ca         bool
}

type tlvState struct {
	mptParts      map[*Service]map[byte]mptPart
	flows         mmt.FlowTracker
	assembler     mmt.MessageAssembler
	services      map[string]*Service
	stats         map[mmt.PacketKey]*Statistics
	componentKeys map[*Service][]mmt.PacketKey
}

func newTLVState() *tlvState {
	return &tlvState{mptParts: map[*Service]map[byte]mptPart{}, services: map[string]*Service{}, stats: map[mmt.PacketKey]*Statistics{}, componentKeys: map[*Service][]mmt.PacketKey{}}
}
func flowName(f mmt.IPDataFlow) string {
	return fmt.Sprintf("%s -> %s", f.Source, netip.AddrPortFrom(f.Destination, f.DestinationPort))
}
func (t *tlvState) service(flow mmt.IPDataFlow, id uint16) *Service {
	key := fmt.Sprintf("%s/%d", flowName(flow), id)
	s := t.services[key]
	if s == nil {
		s = &Service{ID: id, Flow: flowName(flow), Components: []Component{}}
		t.services[key] = s
	}
	return s
}
func (t *tlvState) feed(p mmt.TLVPacket, elapsed time.Duration) uint64 {
	if p.Type() == mmt.TLVPacketTypeNull || p.Type() == mmt.TLVPacketTypeTransmissionControl {
		return 0
	}
	u, err := mmt.ParseUDPPacket(p)
	if err != nil {
		return 1
	}
	flow, ok := t.flows.Flow(u)
	if !ok || flow.DestinationPort == 123 {
		return 0
	}
	pkt, err := mmt.ParseMMTPPacket(u.Payload)
	if err != nil {
		return 1
	}
	key := mmt.PacketKey{Flow: flow, PacketID: pkt.PacketID}
	stats := t.stats[key]
	if stats == nil {
		stats = &Statistics{}
		t.stats[key] = stats
	}
	stats.observe(mmtScrambling(pkt.Extension), elapsed)
	if pkt.PayloadType != mmt.MMTPPayloadTypeSignaling {
		return 0
	}
	messages, err := t.assembler.Feed(flow, pkt)
	if err != nil {
		return 1
	}
	var failures uint64
	for _, msg := range messages {
		if len(msg) < 2 {
			failures++
			continue
		}
		switch msg.ID() {
		case mmt.MessageIDPA:
			pa, e := mmt.ParsePAMessage(msg)
			if e != nil {
				failures++
				continue
			}
			for _, table := range pa.Tables {
				if table.TableID() == mmt.TableIDPLT {
					plt, e := mmt.ParsePLT(table)
					if e != nil {
						failures++
						continue
					}
					for _, p := range plt.Packages {
						f := locationFlow(flow, p.Location)
						t.service(f, p.ServiceID()).Listed = true
					}
				} else if mmt.IsMPT(table.TableID()) {
					mpt, e := mmt.ParseMPT(table)
					if e != nil {
						failures++
						continue
					}
					s := t.service(flow, mpt.ServiceID())
					s.MapReceived = true
					mapID := pkt.PacketID
					s.MapID = &mapID
					part := mptPart{version: mpt.Version, components: []Component{}, ca: mmtCA(mpt.Descriptors)}
					for _, a := range mpt.Assets {
						for _, loc := range a.Locations {
							f := locationFlow(flow, loc)
							c := Component{ID: loc.PacketID, AssetID: hex.EncodeToString(a.AssetID), Flow: flowName(f), Kind: mmtKind(a.AssetType), Codec: a.AssetType, Location: &loc.LocationType, URL: loc.URL, CASignaled: mmtCA(a.Descriptors)}
							if loc.LocationType == mmt.LocationTypeMPEG2TS || loc.LocationType == mmt.LocationTypeMPEG2TSIP6 {
								c.ID = loc.PID
							}
							if loc.LocationType >= mmt.LocationTypeMPEG2TS {
								c.Flow = ""
							}
							for _, d := range a.Descriptors {
								switch d.Tag {
								case mmt.DescriptorTagMHStreamIdentifier:
									if v, e := mmt.ParseMHStreamIdentifierDescriptor(d); e == nil {
										c.Tag = &v
									}
								case mmt.DescriptorTagMHAudioComponent:
									if v, e := mmt.ParseMHAudioComponentDescriptor(d); e == nil {
										c.Language = v.Language
									}
								case mmt.DescriptorTagVideoComponent:
									if v, e := mmt.ParseVideoComponentDescriptor(d); e == nil {
										c.Language = v.Language
									}
								}
							}
							part.components = append(part.components, c)
							part.keys = append(part.keys, mmt.PacketKey{Flow: f, PacketID: loc.PacketID})
						}
					}
					t.storeMPT(s, mpt, part)
				}
			}
		case mmt.MessageIDM2Section:
			section, e := msg.Section()
			if e != nil {
				failures++
				continue
			}
			if section.TableID() != mmt.TableIDMHSDTActual {
				continue
			}
			sdt, e := mmt.ParseMHSDT(section)
			if e != nil {
				failures++
				continue
			}
			if !sdt.CurrentNextIndicator {
				continue
			}
			for _, entry := range sdt.Services {
				serviceFlow := flow
				for _, d := range entry.Descriptors {
					if d.Tag == mmt.DescriptorTagIPDataFlow {
						entries, e := mmt.ParseIPDataFlowDescriptor(d)
						if e != nil {
							failures++
							continue
						}
						for _, entry := range entries {
							if entry.ID == 0 {
								serviceFlow = entry.Flow
							}
						}
					}
				}
				s := t.service(serviceFlow, entry.ServiceID)
				sid, nid, ca := sdt.TLVStreamID, sdt.OriginalNetworkID, entry.FreeCAMode
				s.StreamID = &sid
				s.NetworkID = &nid
				s.CADeclared = &ca
				for _, d := range entry.Descriptors {
					if d.Tag == mmt.DescriptorTagMHService {
						if v, e := mmt.ParseMHServiceDescriptor(d); e == nil {
							s.Name = v.ServiceName
							s.Provider = v.ServiceProviderName
							typ := v.ServiceType
							s.Type = &typ
						}
					}
				}
			}
		}
	}
	return failures
}
func locationFlow(current mmt.IPDataFlow, l mmt.GeneralLocationInfo) mmt.IPDataFlow {
	if l.LocationType == mmt.LocationTypeSameFlow {
		return current
	}
	return mmt.IPDataFlow{Source: l.Source, Destination: l.Destination, DestinationPort: l.DestinationPort}
}
func mmtKind(codec string) string {
	switch codec {
	case "hev1", "hvc1", "avc1":
		return "video"
	case "mp4a":
		return "audio"
	case "stpp":
		return "subtitle"
	case "aapp":
		return "data"
	default:
		return "other"
	}
}

// ARIB STD-B60 6.4.1.2 and STD-B61 Vol.1 Part 1 3.2.8.
func mmtScrambling(ext *mmt.MMTPHeaderExtension) byte {
	if ext == nil {
		return 0
	}
	if ext.Type != 0 {
		return 1
	}
	for b := ext.Data; len(b) > 0; {
		if len(b) < 4 {
			return 1
		}
		typ := uint16(b[0]&0x7f)<<8 | uint16(b[1])
		n := int(b[2])<<8 | int(b[3])
		last := b[0]&0x80 != 0
		b = b[4:]
		if n > len(b) {
			return 1
		}
		if typ == 1 {
			if n < 1 {
				return 1
			}
			return (b[0] >> 3) & 3
		}
		b = b[n:]
		if last {
			break
		}
	}
	return 0
}
func (t *tlvState) finish(r *Report) {
	for _, s := range t.services {
		for i, k := range t.componentKeys[s] {
			if s.Components[i].Location != nil && *s.Components[i].Location >= mmt.LocationTypeMPEG2TS {
				continue
			}
			if st := t.stats[k]; st != nil {
				s.Components[i].Stats = *st
			}
		}
		r.Services = append(r.Services, s)
	}
}

func mmtCA(ds []mmt.Descriptor) bool {
	for _, d := range ds {
		if d.Tag == mmt.DescriptorTagAccessControl || d.Tag == mmt.DescriptorTagScrambler {
			return true
		}
	}
	return false
}

func (t *tlvState) storeMPT(s *Service, mpt *mmt.MPT, part mptPart) {
	parts := t.mptParts[s]
	_, hadFull := parts[mmt.TableIDMPT]
	first, hadFirst := parts[0x11]
	if parts == nil || mpt.TableID == mmt.TableIDMPT || hadFull || (mpt.TableID == 0x11 && hadFirst && first.version != mpt.Version && mpt.Mode != 2) {
		parts = map[byte]mptPart{}
		t.mptParts[s] = parts
	}
	parts[mpt.TableID] = part
	s.MapComplete = mpt.TableID == mmt.TableIDMPT
	s.Components = []Component{}
	s.CASignaled = false
	t.componentKeys[s] = nil
	// Subsets have no last-subset marker; retain their assets without claiming
	// the package composition is complete until a full MPT arrives.
	for id := byte(0x11); id <= mmt.TableIDMPT; id++ {
		p, ok := parts[id]
		if !ok {
			continue
		}
		s.Components = append(s.Components, p.components...)
		t.componentKeys[s] = append(t.componentKeys[s], p.keys...)
		s.CASignaled = s.CASignaled || p.ca
	}
}
