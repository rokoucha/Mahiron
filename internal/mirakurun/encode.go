package mirakurun

import (
	"github.com/21S1298001/mahiron/internal/model"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/go-faster/jx"
)

// MarshalProgram encodes a program the way the API serves it.
func MarshalProgram(p *apigen.Program) []byte {
	e := &jx.Encoder{}
	p.Encode(e)
	return e.Bytes()
}

// extendedToAPI encodes the extended descriptions as one JSON object whose
// keys come in broadcast order. The API schema leaves extended untyped so
// that the generated code writes these bytes as they are instead of ranging
// over a map. A heading repeated in another language keeps its first text.
func extendedToAPI(blocks []model.ExtendedBlock) jx.Raw {
	seen := map[string]struct{}{}
	e := &jx.Encoder{}
	e.ObjStart()
	for _, block := range blocks {
		for _, item := range block.Items {
			if _, ok := seen[item.Name]; ok {
				continue
			}
			seen[item.Name] = struct{}{}
			e.FieldStart(item.Name)
			e.Str(item.Text)
		}
	}
	e.ObjEnd()
	if len(seen) == 0 {
		return nil
	}
	return jx.Raw(e.Bytes())
}

// extendedFromAPI decodes an extended description into one block, keeping
// the item order. Anything but an object of strings counts as absent.
func extendedFromAPI(raw jx.Raw) []model.ExtendedBlock {
	if len(raw) == 0 {
		return nil
	}
	var items []model.ExtendedItem
	if err := jx.DecodeBytes(raw).Obj(func(d *jx.Decoder, key string) error {
		value, err := d.Str()
		if err != nil {
			return err
		}
		items = append(items, model.ExtendedItem{Name: key, Text: value})
		return nil
	}); err != nil || len(items) == 0 {
		return nil
	}
	return []model.ExtendedBlock{{Items: items}}
}
