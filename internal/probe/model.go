package probe

import "time"

type Report struct {
	TimeBasis    string        `json:"time_basis"`
	StopReason   string        `json:"stop_reason"`
	PacketStride int           `json:"packet_stride,omitempty"`
	SyncLosses   uint64        `json:"sync_losses,omitempty"`
	Format       string        `json:"format"`
	Duration     time.Duration `json:"duration_ns"`
	Packets      uint64        `json:"packets"`
	ParseErrors  uint64        `json:"parse_errors"`
	Services     []*Service    `json:"services"`
	Warnings     []string      `json:"warnings,omitempty"`
}

type Service struct {
	MapID       *uint16             `json:"map_packet_id,omitempty"`
	PCRPID      *uint16             `json:"pcr_pid,omitempty"`
	Flow        string              `json:"flow,omitempty"`
	Scrambling  string              `json:"scrambling"`
	CASignaled  bool                `json:"ca_signaled"`
	ID          uint16              `json:"service_id"`
	RelativeTS  uint8               `json:"relative_ts,omitempty"`
	StreamID    *uint16             `json:"stream_id,omitempty"`
	NetworkID   *uint16             `json:"original_network_id,omitempty"`
	Name        string              `json:"name,omitempty"`
	Provider    string              `json:"provider,omitempty"`
	Type        *byte               `json:"service_type,omitempty"`
	Listed      bool                `json:"listed"`
	MapComplete bool                `json:"map_complete"`
	MapReceived bool                `json:"map_received"`
	CADeclared  *bool               `json:"ca_declared,omitempty"`
	CA          []ConditionalAccess `json:"ca,omitempty"`
	Components  []Component         `json:"components"`
}

type ConditionalAccess struct {
	SystemID uint16 `json:"system_id"`
	PID      uint16 `json:"ecm_pid"`
	Packets  uint64 `json:"packets"`
}

type Component struct {
	Scrambling string              `json:"scrambling"`
	CASignaled bool                `json:"ca_signaled"`
	Location   *byte               `json:"location_type,omitempty"`
	URL        string              `json:"url,omitempty"`
	Kind       string              `json:"kind"`
	Codec      string              `json:"codec"`
	ID         uint16              `json:"packet_id"`
	AssetID    string              `json:"asset_id,omitempty"`
	Flow       string              `json:"flow,omitempty"`
	Language   string              `json:"language,omitempty"`
	Tag        *uint16             `json:"component_tag,omitempty"`
	CA         []ConditionalAccess `json:"ca,omitempty"`
	Stats      Statistics          `json:"statistics"`
}

type Statistics struct {
	Packets         uint64         `json:"packets"`
	Scrambled       uint64         `json:"scrambled"`
	Unknown         uint64         `json:"unknown"`
	TransportErrors uint64         `json:"transport_errors"`
	FirstScrambled  *time.Duration `json:"first_scrambled_ns,omitempty"`
	LastScrambled   *time.Duration `json:"last_scrambled_ns,omitempty"`
}

func (s *Statistics) observe(control byte, elapsed time.Duration) {
	s.Packets++
	switch control {
	case 2, 3:
		s.Scrambled++
		if s.FirstScrambled == nil {
			v := elapsed
			s.FirstScrambled = &v
		}
		v := elapsed
		s.LastScrambled = &v
	case 0:
	default:
		s.Unknown++
	}
}

func (s Statistics) Status() string {
	if s.Scrambled > 0 {
		return "scrambling remains"
	}
	if s.Packets == 0 || s.Unknown > 0 || s.TransportErrors > 0 {
		return "unknown"
	}
	return "no scrambling observed"
}
