package schedule

import (
	"encoding/binary"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/ts"
)

type operationDescriptor struct {
	tag  uint16
	data []byte
}
type operationService struct{ network, service uint16 }

// ObserveBIT uses the current broadcaster's explicit individual parameters.
// A broadcaster without individual SI has no parameter descriptor. Future or
// malformed parameters do not establish non-transmission.
// Extended schedule is individual SI, not a first-loop common default
// (TR-B15 Tables 31-26/42; TR-B39 Tables 34.15-6/20).
func (c *Collector) ObserveBIT(section ts.Section, now time.Time, emit func(model.ScheduleUpdate) error) error {
	bit, err := ts.ParseBIT(section)
	if err != nil || !bit.CurrentNext {
		return nil
	}
	for _, broadcaster := range bit.Broadcasters {
		descriptors := make([]operationDescriptor, 0, len(broadcaster.Descriptors))
		for _, d := range broadcaster.Descriptors {
			descriptors = append(descriptors, operationDescriptor{uint16(d.Tag()), append([]byte(nil), d.Data()...)})
		}
		// TR-B14 BIT has a single broadcaster loop and no service list.
		// Its extended broadcaster descriptor identifies terrestrial operation;
		// membership and media types come from the actual SDT, not a type name.
		terrestrial := false
		for _, d := range broadcaster.Descriptors {
			if d.Tag() == ts.DescriptorTagExtendedBroadcaster {
				if extended, err := ts.ParseExtendedBroadcasterDescriptor(d); err == nil {
					terrestrial = extended.BroadcasterType == 1 || extended.BroadcasterType == 2
				}
			}
		}
		if terrestrial && len(bit.Broadcasters) == 1 {
			c.mu.Lock()
			if c.terrestrialParameters == nil {
				c.terrestrialParameters = make(map[uint16][]operationDescriptor)
			}
			c.terrestrialParameters[bit.OriginalNetworkID] = descriptors
			services := c.actualServiceList(bit.OriginalNetworkID)
			c.mu.Unlock()
			descriptors = append(append([]operationDescriptor(nil), descriptors...), operationDescriptor{0x41, services})
		}
		if err := c.observeOperation(bit.OriginalNetworkID, descriptors, 0x41, 0xD7, 0x58, now, emit); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) ObserveMHBIT(section mmt.Section, now time.Time, emit func(model.ScheduleUpdate) error) error {
	bit, err := mmt.ParseMHBIT(section)
	if err != nil || !bit.CurrentNextIndicator {
		return nil
	}
	for _, broadcaster := range bit.Broadcasters {
		descriptors := make([]operationDescriptor, 0, len(broadcaster.Descriptors))
		for _, d := range broadcaster.Descriptors {
			descriptors = append(descriptors, operationDescriptor{d.Tag, d.Data})
		}
		if err := c.observeOperation(bit.TableIDExtension, descriptors, 0x800D, 0x8017, 0x94, now, emit); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) observeOperation(network uint16, descriptors []operationDescriptor, listTag, paramTag uint16, extended byte, now time.Time, emit func(model.ScheduleUpdate) error) error {
	day := int((now.Unix()+9*3600)/86400) + 40587
	selectedDay := -1
	var required map[byte]bool
	var services []byte
	hasParameter := false
	for _, d := range descriptors {
		if d.tag == listTag {
			if len(d.data)%3 != 0 {
				return nil
			}
			services = append(services, d.data...)
		}
		if d.tag == paramTag {
			hasParameter = true
		}
		if d.tag != paramTag || len(d.data) < 3 {
			continue
		}
		effective := day + int(int16(binary.BigEndian.Uint16(d.data[1:3])-uint16(day)))
		if effective > day || effective < selectedDay {
			continue
		}
		selectedDay = effective
		parsed, ok := parseExtendedOperation(d.data[3:], extended)
		if !ok {
			required = nil
			continue
		}
		required = parsed
	}
	if !hasParameter {
		required = make(map[byte]bool)
	}

	c.mu.Lock()
	if c.operations == nil {
		c.operations = make(map[operationService]bool)
	}
	var updates []model.ScheduleUpdate
	for off := 0; off+3 <= len(services); off += 3 {
		sid := binary.BigEndian.Uint16(services[off:])
		var media byte
		switch services[off+2] {
		case 0x01, 0xA1:
			media = 1
		case 0x02, 0xA2:
			media = 2
		case 0xC0, 0xA3, 0xA8, 0xAA:
			media = 3
		default:
			continue
		}
		key := operationService{network, sid}
		previous, known := c.operations[key]
		if known == (required != nil) && (!known || previous == required[media]) {
			continue
		}
		if required == nil {
			delete(c.operations, key)
		} else {
			c.operations[key] = required[media]
		}
		for service, state := range c.services {
			if service.NetworkID != network || service.ServiceID != sid || state.current.Events == nil {
				continue
			}
			update := state.current
			update.ExtendedComplete = state.tracker.ExtendedComplete() && (state.tracker.HasExtended() || required != nil && !required[media])
			update.Reception = nil
			updates = append(updates, update)
		}
	}
	c.mu.Unlock()
	for _, update := range updates {
		if emit != nil {
			if err := emit(update); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseExtendedOperation(data []byte, extended byte) (map[byte]bool, bool) {
	required := make(map[byte]bool)
	for len(data) > 0 {
		if len(data) < 2 || int(data[1]) > len(data)-2 {
			return nil, false
		}
		id, body := data[0], data[2:2+int(data[1])]
		data = data[2+int(data[1]):]
		if id != extended && id != 0x50 && id != 0x8C {
			continue
		}
		for len(body) > 0 {
			if len(body) < 4 {
				return nil, false
			}
			count := int(body[3] & 3)
			if len(body) < 4+2*count {
				return nil, false
			}
			days, ok := bcd(int(body[1]), 2)
			if !ok || days < 1 || days > 32 {
				return nil, false
			}
			cycle, ok := bcd(int(body[2])<<4|int(body[3]>>4), 3)
			if !ok || cycle < 1 || cycle > 360 {
				return nil, false
			}
			for index := 0; index < count; index++ {
				if _, ok := bcd(int(body[4+2*index]), 2); !ok {
					return nil, false
				}
				if cycle, ok := bcd(int(body[5+2*index]), 2); !ok || cycle == 0 {
					return nil, false
				}
			}
			media := body[0] >> 6
			if media != 0 && id == extended {
				required[media] = true
			}
			body = body[4+2*count:]
		}
	}
	return required, true
}

func bcd(value, digits int) (int, bool) {
	result, mult := 0, 1
	for range digits {
		digit := value & 15
		if digit > 9 {
			return 0, false
		}
		result += digit * mult
		mult *= 10
		value >>= 4
	}
	return result, true
}

// ObserveSDT supplies terrestrial service membership when BIT has no service list.
func (c *Collector) ObserveSDT(section ts.Section, now time.Time, emit func(model.ScheduleUpdate) error) error {
	if len(section) < 8 || section.TableID() != ts.TableIDSDT0 {
		return nil
	}
	header, err := ts.ParseSectionHeader(section)
	if err != nil || !header.CurrentNextIndicator {
		return nil
	}
	sdt, err := ts.ParseSDT(section)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	if c.actualServices == nil {
		c.actualServices = make(map[operationService]byte)
	}
	for _, service := range sdt.Services {
		for _, d := range service.Descriptors {
			if d.Tag() != ts.DescriptorTagService {
				continue
			}
			if descriptor, err := ts.ParseServiceDescriptor(d); err == nil {
				c.actualServices[operationService{sdt.OriginalNetworkID, service.ServiceID}] = descriptor.ServiceType
			}
		}
	}
	descriptors := append([]operationDescriptor(nil), c.terrestrialParameters[sdt.OriginalNetworkID]...)
	services := c.actualServiceList(sdt.OriginalNetworkID)
	c.mu.Unlock()
	if len(descriptors) == 0 {
		return nil
	}
	descriptors = append(descriptors, operationDescriptor{0x41, services})
	return c.observeOperation(sdt.OriginalNetworkID, descriptors, 0x41, 0xD7, 0x58, now, emit)
}

// Caller holds mu.
func (c *Collector) actualServiceList(network uint16) []byte {
	var services []byte
	for key, kind := range c.actualServices {
		if key.network == network {
			services = append(services, byte(key.service>>8), byte(key.service), kind)
		}
	}
	return services
}
