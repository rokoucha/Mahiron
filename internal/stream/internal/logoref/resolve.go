// Package logoref resolves indirect CDT logo references after SDT assembly.
package logoref

import "github.com/21S1298001/mahiron/internal/model"

// Direct contains the download information carried by a direct CDT reference.
type Direct struct {
	Version        uint16
	DownloadDataID uint16
}

// Resolve fills indirect references from direct references in the same table.
// A later entry may have replaced a service without a logo descriptor.
func Resolve(services map[uint16]*model.Service, direct map[uint16]Direct, indirect map[uint16]uint16) {
	for serviceID, logoID := range indirect {
		logo, ok := direct[logoID]
		service := services[serviceID]
		if !ok || service == nil || service.Logo == nil {
			continue
		}
		version, downloadDataID := logo.Version, logo.DownloadDataID
		service.Logo.Version, service.Logo.DownloadDataID = &version, &downloadDataID
	}
}
