package ts

// ServiceInfo represents a scanned service. It is the legacy TS-side scan
// output kept only for the remote logo path and the Mirakurun conversion;
// new code uses the internal broadcast model (model.Service) instead.
type ServiceInfo struct {
	Nid                 uint16  `json:"nid"`
	Tsid                uint16  `json:"tsid"`
	Sid                 uint16  `json:"sid"`
	Name                string  `json:"name"`
	Type                uint8   `json:"type"`
	EITScheduleFlag     bool    `json:"eitScheduleFlag"`
	EITPresentFollowing bool    `json:"eitPresentFollowing"`
	LogoId              int64   `json:"logoId"`
	LogoVersion         *uint16 `json:"logoVersion,omitempty"`
	LogoDownloadDataId  *uint16 `json:"logoDownloadDataId,omitempty"`
	RemoteControlKeyId  *uint8  `json:"remoteControlKeyId,omitempty"`
}
