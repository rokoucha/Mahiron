package mmt

import "sort"

// Descriptor tags of MMT-SI (ARIB STD-B60, Table 4-10).
const (
	DescriptorTagAccessControl      = 0x8004
	DescriptorTagScrambler          = 0x8005
	DescriptorTagMHEventGroup       = 0x800C
	DescriptorTagMHServiceList      = 0x800D
	DescriptorTagVideoComponent     = 0x8010
	DescriptorTagMHStreamIdentifier = 0x8011
	DescriptorTagMHContent          = 0x8012
	DescriptorTagMHParentalRating   = 0x8013
	DescriptorTagMHAudioComponent   = 0x8014
	DescriptorTagMHSeries           = 0x8016
	DescriptorTagMHBroadcasterName  = 0x8018
	DescriptorTagMHService          = 0x8019
	DescriptorTagMHLogoTransmission = 0x8025
	DescriptorTagMHShortEvent       = 0xF001
	DescriptorTagMHExtendedEvent    = 0xF002
)

// Descriptor is an MMT-SI descriptor. Its tag is 16 bits wide, and its
// length field is 8, 16 or 32 bits wide depending on the tag.
type Descriptor struct {
	Tag  uint16
	Data []byte
}

func descriptorLengthSize(tag uint16) int {
	switch {
	case tag >= 0x4000 && tag <= 0x6FFF, tag >= 0xF000:
		return 2
	case tag >= 0x7000 && tag <= 0x7FFF:
		return 4
	default:
		return 1
	}
}

// parseDescriptors splits an MMT-SI descriptor loop. Data aliases b.
func parseDescriptors(b []byte) ([]Descriptor, error) {
	var descriptors []Descriptor
	for len(b) > 0 {
		if len(b) < 2 {
			return nil, ErrInvalidSection
		}
		tag := uint16(b[0])<<8 | uint16(b[1])
		size := descriptorLengthSize(tag)
		if len(b) < 2+size {
			return nil, ErrInvalidSection
		}
		length := 0
		for _, v := range b[2 : 2+size] {
			length = length<<8 | int(v)
		}
		off := 2 + size
		if length > len(b)-off {
			return nil, ErrInvalidSection
		}
		descriptors = append(descriptors, Descriptor{Tag: tag, Data: b[off : off+length]})
		b = b[off+length:]
	}
	return descriptors, nil
}

// reader reads big-endian fields and records the first out-of-range read.
type reader struct {
	b  []byte
	ok bool
}

func newReader(b []byte) *reader { return &reader{b: b, ok: true} }

func (r *reader) bytes(n int) []byte {
	if !r.ok || n > len(r.b) {
		r.ok = false
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() byte {
	if b := r.bytes(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if b := r.bytes(2); b != nil {
		return uint16(b[0])<<8 | uint16(b[1])
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.bytes(4); b != nil {
		return be32(b)
	}
	return 0
}

func (r *reader) err() error {
	if !r.ok {
		return ErrInvalidSection
	}
	return nil
}

// MHServiceDescriptor is the MH-service descriptor (0x8019).
type MHServiceDescriptor struct {
	ServiceType         byte
	ServiceProviderName string
	ServiceName         string
}

func ParseMHServiceDescriptor(d Descriptor) (*MHServiceDescriptor, error) {
	if d.Tag != DescriptorTagMHService {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	out := &MHServiceDescriptor{ServiceType: r.u8()}
	out.ServiceProviderName = string(r.bytes(int(r.u8())))
	out.ServiceName = string(r.bytes(int(r.u8())))
	return out, r.err()
}

// MHShortEventDescriptor is the MH-short event descriptor (0xF001).
type MHShortEventDescriptor struct {
	Language  string
	EventName string
	Text      string
}

func ParseMHShortEventDescriptor(d Descriptor) (*MHShortEventDescriptor, error) {
	if d.Tag != DescriptorTagMHShortEvent {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	out := &MHShortEventDescriptor{Language: string(r.bytes(3))}
	out.EventName = string(r.bytes(int(r.u8())))
	out.Text = string(r.bytes(int(r.u16())))
	return out, r.err()
}

// MHExtendedEventDescriptor is one MH-extended event descriptor (0xF002).
// Strings are kept as bytes, since an item or text may split mid-UTF-8
// across descriptors, until JoinExtendedEvent joins them.
type MHExtendedEventDescriptor struct {
	DescriptorNumber     byte
	LastDescriptorNumber byte
	Language             string
	Items                []MHExtendedEventItem
	Text                 []byte
}

type MHExtendedEventItem struct {
	Description []byte
	Item        []byte
}

func ParseMHExtendedEventDescriptor(d Descriptor) (*MHExtendedEventDescriptor, error) {
	if d.Tag != DescriptorTagMHExtendedEvent {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	numbers := r.u8()
	out := &MHExtendedEventDescriptor{
		DescriptorNumber:     numbers >> 4,
		LastDescriptorNumber: numbers & 0x0f,
		Language:             string(r.bytes(3)),
	}
	items := newReader(r.bytes(int(r.u16())))
	for items.ok && len(items.b) > 0 {
		item := MHExtendedEventItem{Description: items.bytes(int(items.u8()))}
		item.Item = items.bytes(int(items.u16()))
		out.Items = append(out.Items, item)
	}
	out.Text = r.bytes(int(r.u16()))
	if err := items.err(); err != nil {
		return nil, err
	}
	return out, r.err()
}

// ExtendedEvent is the joined content of a set of MH-extended event
// descriptors.
type ExtendedEvent struct {
	Language string
	Items    []ExtendedEventItem
	Text     string
}

type ExtendedEventItem struct {
	Description string
	Item        string
}

// JoinExtendedEvent joins MH-extended event descriptors in descriptor_number
// order. An item without a description continues the previous item.
func JoinExtendedEvent(descriptors []*MHExtendedEventDescriptor) ExtendedEvent {
	sorted := append([]*MHExtendedEventDescriptor(nil), descriptors...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].DescriptorNumber < sorted[j].DescriptorNumber })
	var out ExtendedEvent
	var items []MHExtendedEventItem
	var text []byte
	for i, d := range sorted {
		if i == 0 {
			out.Language = d.Language
		}
		for _, item := range d.Items {
			if len(item.Description) == 0 && len(items) > 0 {
				last := &items[len(items)-1]
				last.Item = append(last.Item, item.Item...)
				continue
			}
			items = append(items, MHExtendedEventItem{Description: item.Description, Item: append([]byte(nil), item.Item...)})
		}
		text = append(text, d.Text...)
	}
	for _, item := range items {
		out.Items = append(out.Items, ExtendedEventItem{Description: string(item.Description), Item: string(item.Item)})
	}
	out.Text = string(text)
	return out
}

// ContentGenre is one entry of the MH-content descriptor. The nibbles follow
// ARIB STD-B10 Annex H as in ISDB-T/S.
type ContentGenre struct {
	Level1, Level2 byte
	User1, User2   byte
}

// ParseMHContentDescriptor parses the MH-content descriptor (0x8012).
func ParseMHContentDescriptor(d Descriptor) ([]ContentGenre, error) {
	if d.Tag != DescriptorTagMHContent || len(d.Data)%2 != 0 {
		return nil, ErrInvalidSection
	}
	genres := make([]ContentGenre, 0, len(d.Data)/2)
	for b := d.Data; len(b) > 0; b = b[2:] {
		genres = append(genres, ContentGenre{Level1: b[0] >> 4, Level2: b[0] & 0x0f, User1: b[1] >> 4, User2: b[1] & 0x0f})
	}
	return genres, nil
}

// VideoComponentDescriptor is the video component descriptor (0x8010). The
// coded values follow ARIB STD-B60 Tables 7-48 to 7-51.
type VideoComponentDescriptor struct {
	Resolution              byte
	AspectRatio             byte
	Progressive             bool
	FrameRate               byte
	ComponentTag            uint16
	TransferCharacteristics byte
	Language                string
	Text                    string
}

func ParseVideoComponentDescriptor(d Descriptor) (*VideoComponentDescriptor, error) {
	if d.Tag != DescriptorTagVideoComponent {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	format, scan := r.u8(), r.u8()
	out := &VideoComponentDescriptor{
		Resolution:              format >> 4,
		AspectRatio:             format & 0x0f,
		Progressive:             scan&0x80 != 0,
		FrameRate:               scan & 0x1f,
		ComponentTag:            r.u16(),
		TransferCharacteristics: r.u8() >> 4,
		Language:                string(r.bytes(3)),
	}
	out.Text = string(r.b)
	return out, r.err()
}

// MHAudioComponentDescriptor is the MH-audio component descriptor (0x8014).
type MHAudioComponentDescriptor struct {
	StreamContent     byte
	ComponentType     byte
	ComponentTag      uint16
	StreamType        byte
	SimulcastGroupTag byte
	ESMultiLingual    bool
	MainComponent     bool
	QualityIndicator  byte
	SamplingRate      byte // coded value; see SamplingRateHz
	Language          string
	Language2         string
	Text              string
}

func ParseMHAudioComponentDescriptor(d Descriptor) (*MHAudioComponentDescriptor, error) {
	if d.Tag != DescriptorTagMHAudioComponent {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	out := &MHAudioComponentDescriptor{
		StreamContent:     r.u8() & 0x0f,
		ComponentType:     r.u8(),
		ComponentTag:      r.u16(),
		StreamType:        r.u8(),
		SimulcastGroupTag: r.u8(),
	}
	flags := r.u8()
	out.ESMultiLingual = flags&0x80 != 0
	out.MainComponent = flags&0x40 != 0
	out.QualityIndicator = (flags >> 4) & 0x03
	out.SamplingRate = (flags >> 1) & 0x07
	out.Language = string(r.bytes(3))
	if out.ESMultiLingual {
		out.Language2 = string(r.bytes(3))
	}
	out.Text = string(r.b)
	return out, r.err()
}

// SamplingRateHz returns the sampling frequency in Hz, or 0 when the coded
// value is reserved (ARIB STD-B60, Table 7-62).
func (d *MHAudioComponentDescriptor) SamplingRateHz() int {
	return [8]int{0, 16000, 22050, 24000, 0, 32000, 44100, 48000}[d.SamplingRate]
}

// MHEventGroupDescriptor is the MH-event group descriptor (0x800C).
type MHEventGroupDescriptor struct {
	GroupType byte
	Events    []GroupEvent
	// OtherNetworkEvents is set for group types 4 and 5 (relay and
	// movement to other networks).
	OtherNetworkEvents []GroupEvent
	PrivateData        []byte
}

type GroupEvent struct {
	OriginalNetworkID uint16
	TLVStreamID       uint16
	ServiceID         uint16
	EventID           uint16
}

func ParseMHEventGroupDescriptor(d Descriptor) (*MHEventGroupDescriptor, error) {
	if d.Tag != DescriptorTagMHEventGroup {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	head := r.u8()
	out := &MHEventGroupDescriptor{GroupType: head >> 4}
	for range head & 0x0f {
		out.Events = append(out.Events, GroupEvent{ServiceID: r.u16(), EventID: r.u16()})
	}
	if out.GroupType == 4 || out.GroupType == 5 {
		if len(r.b)%8 != 0 {
			return nil, ErrInvalidSection
		}
		for len(r.b) > 0 {
			out.OtherNetworkEvents = append(out.OtherNetworkEvents, GroupEvent{
				OriginalNetworkID: r.u16(), TLVStreamID: r.u16(), ServiceID: r.u16(), EventID: r.u16(),
			})
		}
	} else {
		out.PrivateData = r.b
	}
	return out, r.err()
}

// MHSeriesDescriptor is the MH-series descriptor (0x8016).
type MHSeriesDescriptor struct {
	SeriesID          uint16
	RepeatLabel       byte
	ProgramPattern    byte
	ExpireDateValid   bool
	ExpireDate        uint16 // MJD
	EpisodeNumber     uint16
	LastEpisodeNumber uint16
	SeriesName        string
}

func ParseMHSeriesDescriptor(d Descriptor) (*MHSeriesDescriptor, error) {
	if d.Tag != DescriptorTagMHSeries {
		return nil, ErrInvalidSection
	}
	r := newReader(d.Data)
	out := &MHSeriesDescriptor{SeriesID: r.u16()}
	flags := r.u8()
	out.RepeatLabel = flags >> 4
	out.ProgramPattern = (flags >> 1) & 0x07
	out.ExpireDateValid = flags&0x01 != 0
	out.ExpireDate = r.u16()
	episodes := r.bytes(3)
	if episodes != nil {
		out.EpisodeNumber = uint16(episodes[0])<<4 | uint16(episodes[1]>>4)
		out.LastEpisodeNumber = uint16(episodes[1]&0x0f)<<8 | uint16(episodes[2])
	}
	out.SeriesName = string(r.b)
	return out, r.err()
}

// ParseMHStreamIdentifierDescriptor returns the component_tag of the
// MH-stream identifier descriptor (0x8011), which links an MPT asset to the
// component descriptors in MH-EIT.
func ParseMHStreamIdentifierDescriptor(d Descriptor) (uint16, error) {
	if d.Tag != DescriptorTagMHStreamIdentifier || len(d.Data) < 2 {
		return 0, ErrInvalidSection
	}
	return uint16(d.Data[0])<<8 | uint16(d.Data[1]), nil
}

// ParentalRating is one country and age limit of the MH-parental rating
// descriptor.
type ParentalRating struct {
	CountryCode string
	Rating      byte
}

// ParseMHParentalRatingDescriptor parses the MH-parental rating descriptor
// (0x8013), which has the layout of the ISDB-T/S parental rating
// descriptor.
func ParseMHParentalRatingDescriptor(d Descriptor) ([]ParentalRating, error) {
	if d.Tag != DescriptorTagMHParentalRating || len(d.Data)%4 != 0 {
		return nil, ErrInvalidSection
	}
	ratings := make([]ParentalRating, 0, len(d.Data)/4)
	for b := d.Data; len(b) > 0; b = b[4:] {
		ratings = append(ratings, ParentalRating{CountryCode: string(b[:3]), Rating: b[3]})
	}
	return ratings, nil
}
