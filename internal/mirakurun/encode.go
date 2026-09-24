package mirakurun

import (
	"sort"

	"github.com/go-faster/jx"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// EncodeProgram writes a program in the same field order as the generated
// encoding, except that extended items are written sorted by key. The
// generated encoding ranges over the extended map, so its key order changes
// from run to run; this writer keeps the output byte-for-byte stable. It must
// stay in sync with the generated Program encoding:
// TestEncodeProgramMatchesGenerated fails when the two disagree on programs
// without extended items.
func EncodeProgram(e *jx.Encoder, s *apigen.Program) {
	e.ObjStart()
	{
		e.FieldStart("id")
		s.ID.Encode(e)
	}
	{
		e.FieldStart("eventId")
		s.EventId.Encode(e)
	}
	{
		e.FieldStart("serviceId")
		s.ServiceId.Encode(e)
	}
	{
		e.FieldStart("networkId")
		s.NetworkId.Encode(e)
	}
	{
		e.FieldStart("startAt")
		s.StartAt.Encode(e)
	}
	{
		e.FieldStart("duration")
		e.Int(s.Duration)
	}
	{
		e.FieldStart("isFree")
		e.Bool(s.IsFree)
	}
	{
		if s.Name.Set {
			e.FieldStart("name")
			s.Name.Encode(e)
		}
	}
	{
		if s.Description.Set {
			e.FieldStart("description")
			s.Description.Encode(e)
		}
	}
	{
		if s.Genres != nil {
			e.FieldStart("genres")
			e.ArrStart()
			for _, elem := range s.Genres {
				elem.Encode(e)
			}
			e.ArrEnd()
		}
	}
	{
		if s.Video.Set {
			e.FieldStart("video")
			s.Video.Encode(e)
		}
	}
	{
		if s.Audios != nil {
			e.FieldStart("audios")
			e.ArrStart()
			for _, elem := range s.Audios {
				elem.Encode(e)
			}
			e.ArrEnd()
		}
	}
	{
		if s.Extended.Set {
			e.FieldStart("extended")
			encodeExtendedSorted(e, &s.Extended.Value)
		}
	}
	{
		if s.RelatedItems != nil {
			e.FieldStart("relatedItems")
			e.ArrStart()
			for _, elem := range s.RelatedItems {
				elem.Encode(e)
			}
			e.ArrEnd()
		}
	}
	{
		if s.Series.Set {
			e.FieldStart("series")
			s.Series.Encode(e)
		}
	}
	e.ObjEnd()
}

// MarshalProgram encodes a program with EncodeProgram and returns the bytes.
func MarshalProgram(p *apigen.Program) []byte {
	e := &jx.Encoder{}
	EncodeProgram(e, p)
	return e.Bytes()
}

// encodeExtendedSorted writes extended items sorted by key so that repeated
// encodings of the same program produce identical bytes.
func encodeExtendedSorted(e *jx.Encoder, extended *apigen.ProgramExtended) {
	keys := make([]string, 0, len(*extended))
	for k := range *extended {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.ObjStart()
	for _, k := range keys {
		e.FieldStart(k)
		e.Str((*extended)[k])
	}
	e.ObjEnd()
}
