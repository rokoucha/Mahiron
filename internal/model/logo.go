package model

// Logo is one broadcast logo image delivered by a session as normalized PNG
// bytes; a Deleted logo carries no Data. A CDT/MH-CDT logo is matched to
// services by scan-time reference; a common-data logo names them in Services.
type Logo struct {
	NetworkID      uint16
	LogoID         uint16
	Version        uint16
	DownloadDataID uint16
	LogoType       uint8
	Data           []byte
	Deleted        bool
	Services       []ServiceKey
}

// CommonDataAnnouncement is an SDTT announcement that a service carries the
// ISDB-S all-receivers common data (satellite services' logos).
type CommonDataAnnouncement struct {
	Service    ServiceKey
	DownloadID uint32
	VersionID  uint16
}
