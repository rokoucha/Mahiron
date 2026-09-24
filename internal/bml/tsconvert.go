package bml

import (
	"github.com/21S1298001/mahiron/ts"
)

// bxmlInfoFromTS converts a parsed TS BXML descriptor into the parser-free
// public type. A nil input stays nil so absent descriptors keep encoding as
// null.
func bxmlInfoFromTS(info *ts.AdditionalAribBXMLInfo) *BXMLInfo {
	if info == nil {
		return nil
	}
	result := &BXMLInfo{
		TransmissionFormat: info.TransmissionFormat,
		EntryPointFlag:     info.EntryPointFlag,
	}
	if entry := info.EntryPointInfo; entry != nil {
		result.EntryPointInfo = &BXMLEntryPoint{
			AutoStartFlag:      entry.AutoStartFlag,
			DocumentResolution: entry.DocumentResolution,
			UseXML:             entry.UseXML,
			DefaultVersionFlag: entry.DefaultVersionFlag,
			IndependentFlag:    entry.IndependentFlag,
			StyleForTVFlag:     entry.StyleForTVFlag,
			BMLMajorVersion:    entry.BMLMajorVersion,
			BMLMinorVersion:    entry.BMLMinorVersion,
		}
		if entry.BXMLMajorVersion != nil {
			value := *entry.BXMLMajorVersion
			result.EntryPointInfo.BXMLMajorVersion = &value
		}
		if entry.BXMLMinorVersion != nil {
			value := *entry.BXMLMinorVersion
			result.EntryPointInfo.BXMLMinorVersion = &value
		}
	}
	if carousel := info.AdditionalAribCarouselInfo; carousel != nil {
		result.AdditionalAribCarouselInfo = &BXMLCarousel{
			DataEventID:           carousel.DataEventID,
			EventSectionFlag:      carousel.EventSectionFlag,
			OnDemandRetrievalFlag: carousel.OnDemandRetrievalFlag,
			FileStorableFlag:      carousel.FileStorableFlag,
			StartPriority:         carousel.StartPriority,
		}
	}
	return result
}

// ModuleMetadataFromTS converts parsed TS DII module metadata into the
// parser-free public type. resource uses it for the Info fallback when a
// module carries no converted metadata yet.
func ModuleMetadataFromTS(metadata ts.DSMCCModuleMetadata) ModuleMetadata {
	return ModuleMetadata{
		Type:                     metadata.Type,
		Name:                     metadata.Name,
		CRC32:                    metadata.CRC32,
		EstimatedDownloadSeconds: metadata.EstimatedDownloadSeconds,
		CachingPriority:          metadata.CachingPriority,
		ExpireMode:               metadata.ExpireMode,
		ExpireData:               append([]byte(nil), metadata.ExpireData...),
		ActivationMode:           metadata.ActivationMode,
		ActivationData:           append([]byte(nil), metadata.ActivationData...),
		CompressionType:          metadata.CompressionType,
		OriginalSize:             metadata.OriginalSize,
	}
}
