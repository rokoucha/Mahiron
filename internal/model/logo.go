package model

// Logo is one broadcast logo image delivered by a session. Sessions hand
// over the normalized PNG bytes (2K logos carry the common fixed palette);
// matching an image to a service stays with the service manager, which owns
// the scan-time logo references. A Deleted logo carries no Data.
type Logo struct {
	NetworkID      uint16
	LogoID         uint16
	Version        uint16
	DownloadDataID uint16
	LogoType       uint8
	Data           []byte
	Deleted        bool
}
