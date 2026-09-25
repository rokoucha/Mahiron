package service

import (
	"math"
	"sort"
	"strconv"

	"github.com/21S1298001/mahiron/internal/config"
)

func (s *Manager) orderServices(services []*Service) []*Service {
	ordered := append([]*Service(nil), services...)
	order := serviceOrder(s.channels)
	sort.SliceStable(ordered, func(i, j int) bool {
		return order.less(ordered[i], ordered[j])
	})
	return ordered
}

type serviceDisplayOrder struct {
	channelTypeOrder map[string]int
}

func serviceOrder(channels config.ChannelsConfig) serviceDisplayOrder {
	channelTypeOrder := make(map[string]int)
	for _, channel := range channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		if _, ok := channelTypeOrder[channel.Type]; !ok {
			channelTypeOrder[channel.Type] = len(channelTypeOrder)
		}
	}
	return serviceDisplayOrder{channelTypeOrder: channelTypeOrder}
}

func (o serviceDisplayOrder) less(a, b *Service) bool {
	if cmp := compareInts(o.channelTypeSortNumber(a), o.channelTypeSortNumber(b)); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareStrings(a.ChannelType, b.ChannelType); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareRemoteControlKeys(a.RemoteControlKey, b.RemoteControlKey); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareUint16s(a.Key.ServiceID, b.Key.ServiceID); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareUint16s(a.Key.NetworkID, b.Key.NetworkID); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareUint16s(a.Key.StreamID, b.Key.StreamID); cmp != 0 {
		return cmp < 0
	}
	if cmp := compareInt64s(serviceSortID(a), serviceSortID(b)); cmp != 0 {
		return cmp < 0
	}
	return a.Id < b.Id
}

func (o serviceDisplayOrder) channelTypeSortNumber(service *Service) int {
	if n, ok := o.channelTypeOrder[service.ChannelType]; ok {
		return n
	}
	return math.MaxInt
}

// compareRemoteControlKeys sorts services without a remote control key last.
func compareRemoteControlKeys(a, b *uint8) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	return compareInts(int(*a), int(*b))
}

func serviceSortID(service *Service) int64 {
	if n, err := strconv.ParseInt(service.Id, 10, 64); err == nil {
		return n
	}
	return service.ItemId()
}

func compareInts(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt64s(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareUint16s(a, b uint16) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
