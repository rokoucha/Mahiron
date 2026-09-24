package mirakurun

import (
	"sort"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/go-faster/jx"
)

// MarshalProgram encodes a program the way the API serves it.
func MarshalProgram(p *apigen.Program) []byte {
	e := &jx.Encoder{}
	p.Encode(e)
	return e.Bytes()
}

// extendedToAPI encodes the extended description as a JSON object in key
// order. The API schema leaves extended untyped so that the generated code
// writes these bytes as they are instead of ranging over a map.
func extendedToAPI(extended map[string]string) jx.Raw {
	if len(extended) == 0 {
		return nil
	}
	keys := make([]string, 0, len(extended))
	for key := range extended {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	e := &jx.Encoder{}
	e.ObjStart()
	for _, key := range keys {
		e.FieldStart(key)
		e.Str(extended[key])
	}
	e.ObjEnd()
	return jx.Raw(e.Bytes())
}

// extendedFromAPI decodes an extended description. Anything but an object of
// strings counts as absent.
func extendedFromAPI(raw jx.Raw) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	extended := map[string]string{}
	if err := jx.DecodeBytes(raw).Obj(func(d *jx.Decoder, key string) error {
		value, err := d.Str()
		if err != nil {
			return err
		}
		extended[key] = value
		return nil
	}); err != nil || len(extended) == 0 {
		return nil
	}
	return extended
}
