package ts

// DescriptorTagParentalRating is the TS parental rating descriptor tag.
const DescriptorTagParentalRating = 0x55

// ParentalRating is one decoded parental rating entry: an ISO 639-2
// country code and the raw rating byte. Broadcasters encode the minimum
// age as rating+3 (0x00 means undefined); interpretation is left to the
// converters so this layer never invents meaning.
type ParentalRating struct {
	Country string
	Rating  uint8
}

// ParseParentalRatings decodes a parental rating descriptor into its
// country/rating entries. Malformed descriptors yield no entries.
func ParseParentalRatings(d Descriptor) []ParentalRating {
	if d.Tag() != DescriptorTagParentalRating {
		return nil
	}
	data := d.Data()
	if len(data) == 0 || len(data)%4 != 0 {
		return nil
	}
	out := make([]ParentalRating, 0, len(data)/4)
	for i := 0; i+4 <= len(data); i += 4 {
		out = append(out, ParentalRating{Country: string(data[i : i+3]), Rating: data[i+3]})
	}
	return out
}
