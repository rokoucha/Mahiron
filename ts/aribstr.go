package ts

import "github.com/21S1298001/Mahiron5/ts/aribstr"

// DecodeARIBString decodes an ARIB STD-B24 encoded byte sequence to a UTF-8 string.
func DecodeARIBString(b []byte) (string, error) {
	return aribstr.Decode(b)
}
