package databroadcast

import (
	"github.com/21S1298001/mahiron/ts"
)

func clonePMT(pmt *DataBroadcastPMT) *DataBroadcastPMT {
	if pmt == nil {
		return nil
	}
	clone := *pmt
	clone.Components = cloneComponents(pmt.Components)
	return &clone
}

func cloneComponents(components []DataBroadcastComponent) []DataBroadcastComponent {
	result := make([]DataBroadcastComponent, len(components))
	for i, component := range components {
		result[i] = component
		if component.DataComponentID != nil {
			value := *component.DataComponentID
			result[i].DataComponentID = &value
		}
		result[i].BXMLInfo = cloneBXMLInfo(component.BXMLInfo)
		result[i].Modules = cloneModules(component.Modules)
	}
	return result
}

func cloneBXMLInfo(info *ts.AdditionalAribBXMLInfo) *ts.AdditionalAribBXMLInfo {
	if info == nil {
		return nil
	}
	clone := *info
	if info.EntryPointInfo != nil {
		entry := *info.EntryPointInfo
		if entry.BXMLMajorVersion != nil {
			value := *entry.BXMLMajorVersion
			entry.BXMLMajorVersion = &value
		}
		if entry.BXMLMinorVersion != nil {
			value := *entry.BXMLMinorVersion
			entry.BXMLMinorVersion = &value
		}
		clone.EntryPointInfo = &entry
	}
	if info.AdditionalAribCarouselInfo != nil {
		carousel := *info.AdditionalAribCarouselInfo
		clone.AdditionalAribCarouselInfo = &carousel
	}
	return &clone
}

func cloneModules(modules []DataBroadcastModule) []DataBroadcastModule {
	result := make([]DataBroadcastModule, len(modules))
	for i, module := range modules {
		result[i] = module
		result[i].Info = append([]byte(nil), module.Info...)
		result[i].Data = append([]byte(nil), module.Data...)
		if module.Metadata != nil {
			metadata := *module.Metadata
			metadata.ExpireData = append([]byte(nil), metadata.ExpireData...)
			metadata.ActivationData = append([]byte(nil), metadata.ActivationData...)
			result[i].Metadata = &metadata
		}
	}
	return result
}

func cloneProgramInfo(info *DataBroadcastProgramInfo) *DataBroadcastProgramInfo {
	if info == nil {
		return nil
	}
	clone := *info
	clone.EventIDs = append([]uint16(nil), info.EventIDs...)
	return &clone
}

func cloneCurrentTime(current *DataBroadcastCurrentTime) *DataBroadcastCurrentTime {
	if current == nil {
		return nil
	}
	clone := *current
	return &clone
}

func cloneBIT(bit *DataBroadcastBIT) *DataBroadcastBIT {
	if bit == nil {
		return nil
	}
	clone := *bit
	clone.Broadcasters = make([]DataBroadcastBroadcaster, len(bit.Broadcasters))
	for i, b := range bit.Broadcasters {
		clone.Broadcasters[i] = b
		clone.Broadcasters[i].Services = append([]DataBroadcastService(nil), b.Services...)
		clone.Broadcasters[i].Affiliations = append([]byte(nil), b.Affiliations...)
		clone.Broadcasters[i].AffiliationBroadcasters = append([]DataBroadcastAffiliatedBroadcaster(nil), b.AffiliationBroadcasters...)
		if b.BroadcasterName != nil {
			clone.Broadcasters[i].BroadcasterName = ptr(*b.BroadcasterName)
		}
		if b.TerrestrialBroadcasterID != nil {
			clone.Broadcasters[i].TerrestrialBroadcasterID = ptr(*b.TerrestrialBroadcasterID)
		}
	}
	return &clone
}

func clonePCR(pcr *DataBroadcastPCR) *DataBroadcastPCR {
	if pcr == nil {
		return nil
	}
	clone := *pcr
	return &clone
}
