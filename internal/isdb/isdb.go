// Package isdb holds helpers that are ISDB-specific but shared by the TS
// and MMT systems: multi-section table assembly, EIT table classification,
// original_network_id classification and event-group constants.
//
// It knows neither ts, mmt nor model: table assembly works on plain long
// section header values because TS and MMT share the section format, and
// classification works on raw table_id / network ID integers so that no
// system-specific meaning leaks into the callers.
package isdb

// SectionHeader carries the long-format section header values both TS and
// MMT sections share, plus the table bookkeeping values EIT schedule
// tracking needs (the last table ID of the sub-table and the last section
// number of the current segment).
type SectionHeader struct {
	TableIDExtension   uint16
	Version            uint8
	SectionNumber      uint8
	LastSectionNumber  uint8
	CurrentNext        bool
	LastTableID        uint8
	SegmentLastSection uint8
}

// TableTracker collects every section of one table version. A table is
// ready only after every section through LastSectionNumber has arrived; a
// section with a different extension or version resets the state.
type TableTracker struct {
	initialized bool
	extension   uint16
	version     uint8
	last        uint8
	sections    map[uint8]struct{}
}

// Add records one section header. It reports whether the tracker reset on
// this section and whether the table is now complete.
func (t *TableTracker) Add(h SectionHeader) (reset bool, ready bool) {
	if h.SectionNumber > h.LastSectionNumber {
		return false, t.Ready()
	}
	if !h.CurrentNext {
		return false, t.Ready()
	}
	if !t.initialized || t.extension != h.TableIDExtension || t.version != h.Version {
		t.initialized = true
		t.extension = h.TableIDExtension
		t.version = h.Version
		t.last = h.LastSectionNumber
		t.sections = make(map[uint8]struct{})
		reset = true
	}
	if t.sections == nil {
		t.sections = make(map[uint8]struct{})
	}
	t.sections[h.SectionNumber] = struct{}{}
	return reset, t.Ready()
}

// Ready reports whether all sections of the current table version arrived.
func (t *TableTracker) Ready() bool {
	if !t.initialized {
		return false
	}
	for i := uint8(0); ; i++ {
		if _, ok := t.sections[i]; !ok {
			return false
		}
		if i == t.last {
			return true
		}
	}
}

// Missing returns the section numbers still absent, for log-only diagnosis.
func (t *TableTracker) Missing() []int {
	if !t.initialized {
		return nil
	}
	var out []int
	for i := uint8(0); ; i++ {
		if _, ok := t.sections[i]; !ok {
			out = append(out, int(i))
		}
		if i == t.last {
			return out
		}
	}
}

// EITKind classifies an EIT table_id.
type EITKind int

const (
	EITKindOther EITKind = iota
	// EITKindPresentFollowing is the current/next table.
	EITKindPresentFollowing
	// EITKindScheduleBasic carries short event, content and component
	// descriptors.
	EITKindScheduleBasic
	// EITKindScheduleExtended carries extended event descriptors.
	EITKindScheduleExtended
)

// ClassifyTSEITTableID classifies a TS EIT table_id per TR-B15: 0x4E/0x4F
// are p/f, 0x50-0x57 basic schedule, 0x58-0x5F extended schedule.
func ClassifyTSEITTableID(tableID uint8) EITKind {
	switch {
	case tableID == 0x4E || tableID == 0x4F:
		return EITKindPresentFollowing
	case tableID >= 0x50 && tableID <= 0x57:
		return EITKindScheduleBasic
	case tableID >= 0x58 && tableID <= 0x5F:
		return EITKindScheduleExtended
	default:
		return EITKindOther
	}
}

// ClassifyMMTEITTableID classifies an MH-EIT table_id per TR-B39: 0x8B is
// p/f, 0x8C-0x93 basic schedule, 0x94-0x9B extended schedule.
func ClassifyMMTEITTableID(tableID uint8) EITKind {
	switch {
	case tableID == 0x8B:
		return EITKindPresentFollowing
	case tableID >= 0x8C && tableID <= 0x93:
		return EITKindScheduleBasic
	case tableID >= 0x94 && tableID <= 0x9B:
		return EITKindScheduleExtended
	default:
		return EITKindOther
	}
}

// NetworkClass classifies an original_network_id by broadcast system.
type NetworkClass int

const (
	NetworkClassOther NetworkClass = iota
	NetworkClassTerrestrial
	NetworkClassBSSatellite
	NetworkClassCSSatellite
	NetworkClassAdvancedBS
	NetworkClassAdvancedWidebandCS
)

// ClassifyOriginalNetworkID classifies an original_network_id. 0x000B is
// advanced BS and 0x000C is advanced wideband CS; neither is covered by the
// legacy satellite predicate, so channel-scoped gathering keeps working
// unchanged for them. Only the IDs the codebase already relies on are
// classified; everything else is NetworkClassOther.
func ClassifyOriginalNetworkID(onid uint16) NetworkClass {
	switch onid {
	case 0x000B:
		return NetworkClassAdvancedBS
	case 0x000C:
		return NetworkClassAdvancedWidebandCS
	case 0x0004, 0x0006, 0x0007:
		return NetworkClassBSSatellite
	default:
		return NetworkClassOther
	}
}

// IsSatelliteOriginalNetworkID reports whether services under this network
// ID share EIT schedule tables network-wide (satellite) rather than
// per-stream. It mirrors the legacy ts predicate so existing callers keep
// their behavior while the EIT-range query takes over.
func IsSatelliteOriginalNetworkID(onid uint16) bool {
	switch ClassifyOriginalNetworkID(onid) {
	case NetworkClassBSSatellite, NetworkClassCSSatellite:
		return true
	default:
		return false
	}
}
