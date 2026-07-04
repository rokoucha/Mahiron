package aribstr

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
)

type aribGraphicSet byte

const (
	aribSetKanji aribGraphicSet = iota
	aribSetAlphanumeric
	aribSetHiragana
	aribSetKatakana
	aribSetJISX0201Katakana
	aribSetProportionalAlphanumeric
	aribSetProportionalHiragana
	aribSetProportionalKatakana
	aribSetJISCompatibleKanjiPlane1
	aribSetJISCompatibleKanjiPlane2
	aribSetAdditionalSymbols
	aribSetDRCS0
	aribSetDRCS1Byte
	aribSetMacro
)

type aribStringDecoder struct {
	g           [4]aribGraphicSet
	gl          int
	gr          int
	narrowAlnum bool
}

// Decode decodes an ARIB STD-B24 encoded byte sequence to a UTF-8 string.
//
// SI strings in real broadcasts use the ARIB 8-bit code extension model.  This
// decoder keeps enough G-set state to handle the common EPG/service-name
// character sets and skips presentation-only controls that should not appear in
// textual metadata.
func Decode(b []byte) (string, error) {
	d := aribStringDecoder{
		g:  [4]aribGraphicSet{aribSetKanji, aribSetAlphanumeric, aribSetHiragana, aribSetKatakana},
		gl: 0,
		gr: 2,
	}
	var out strings.Builder
	for i := 0; i < len(b); {
		c := b[i]
		switch {
		case c == 0x20 || c == 0xa0:
			if d.narrowAlnum {
				out.WriteByte(' ')
			} else {
				out.WriteRune('\u3000')
			}
			i++
		case c <= 0x1f:
			next := d.decodeC0(b, i, &out)
			if next <= i {
				i++
			} else {
				i = next
			}
		case c >= 0x80 && c <= 0x9f:
			i = d.skipC1Control(b, i)
		case c == 0xfe && (d.g[d.gl] == aribSetAlphanumeric || d.g[d.gl] == aribSetProportionalAlphanumeric):
			out.WriteRune('・')
			i++
		case c >= 0x21 && c <= 0x7e:
			s, next := d.decodeGraphic(b, i, d.g[d.gl], false)
			out.WriteString(s)
			i = next
		case c >= 0xa1 && c <= 0xfe:
			s, next := d.decodeGraphic(b, i, d.g[d.gr], true)
			out.WriteString(s)
			i = next
		default:
			out.WriteRune(utf8.RuneError)
			i++
		}
	}
	return out.String(), nil
}

func (d *aribStringDecoder) decodeC0(b []byte, i int, out *strings.Builder) int {
	switch b[i] {
	case 0x0d:
		out.WriteByte('\n')
		return i + 1
	case 0x0e:
		d.gl = 1
		return i + 1
	case 0x0f:
		d.gl = 0
		return i + 1
	case 0x16:
		return min(i+2, len(b))
	case 0x19:
		if i+1 >= len(b) {
			out.WriteRune(utf8.RuneError)
			return len(b)
		}
		s, next := d.decodeGraphic(b, i+1, d.g[2], b[i+1] >= 0xa1)
		out.WriteString(s)
		return next
	case 0x1b:
		return d.decodeEscape(b, i)
	case 0x1c:
		return min(i+3, len(b))
	case 0x1d:
		if i+1 >= len(b) {
			out.WriteRune(utf8.RuneError)
			return len(b)
		}
		s, next := d.decodeGraphic(b, i+1, d.g[3], b[i+1] >= 0xa1)
		out.WriteString(s)
		return next
	default:
		return i + 1
	}
}

func (d *aribStringDecoder) decodeEscape(b []byte, i int) int {
	if i+1 >= len(b) {
		return len(b)
	}
	switch b[i+1] {
	case 0x6e:
		d.gl = 2
		return i + 2
	case 0x6f:
		d.gl = 3
		return i + 2
	case 0x7e:
		d.gr = 1
		return i + 2
	case 0x7d:
		d.gr = 2
		return i + 2
	case 0x7c:
		d.gr = 3
		return i + 2
	case 0x28, 0x29, 0x2a, 0x2b:
		g := int(b[i+1] - 0x28)
		if i+2 >= len(b) {
			return len(b)
		}
		if b[i+2] == 0x20 {
			if i+3 >= len(b) {
				return len(b)
			}
			d.g[g] = designateDRCS1Byte(b[i+3])
			return i + 4
		}
		d.g[g] = designateGSet(b[i+2], false)
		return i + 3
	case 0x24:
		if i+2 >= len(b) {
			return len(b)
		}
		if b[i+2] >= 0x28 && b[i+2] <= 0x2b {
			g := int(b[i+2] - 0x28)
			if i+3 >= len(b) {
				return len(b)
			}
			d.g[g] = designateGSet(b[i+3], true)
			return i + 4
		}
		d.g[0] = designateGSet(b[i+2], true)
		return i + 3
	default:
		return i + 2
	}
}

func designateGSet(final byte, multiByte bool) aribGraphicSet {
	switch final {
	case 0x42:
		return aribSetKanji
	case 0x4a:
		return aribSetAlphanumeric
	case 0x30:
		return aribSetHiragana
	case 0x31:
		return aribSetKatakana
	case 0x36:
		return aribSetProportionalAlphanumeric
	case 0x37:
		return aribSetProportionalHiragana
	case 0x38:
		return aribSetProportionalKatakana
	case 0x39:
		return aribSetJISCompatibleKanjiPlane1
	case 0x3a:
		return aribSetJISCompatibleKanjiPlane2
	case 0x49:
		return aribSetJISX0201Katakana
	case 0x3b:
		return aribSetAdditionalSymbols
	case 0x40:
		return aribSetDRCS0
	case 0x70:
		return aribSetMacro
	default:
		if multiByte {
			return aribSetKanji
		}
		return aribSetAlphanumeric
	}
}

func designateDRCS1Byte(final byte) aribGraphicSet {
	if final >= 0x41 && final <= 0x4f {
		return aribSetDRCS1Byte
	}
	return aribSetDRCS1Byte
}

func (d *aribStringDecoder) decodeGraphic(b []byte, i int, set aribGraphicSet, gr bool) (string, int) {
	first := normalizeGraphicByte(b[i], gr)
	switch set {
	case aribSetKanji, aribSetJISCompatibleKanjiPlane1, aribSetJISCompatibleKanjiPlane2, aribSetAdditionalSymbols, aribSetDRCS0:
		if i+1 >= len(b) {
			if set == aribSetAdditionalSymbols || set == aribSetDRCS0 {
				return "", len(b)
			}
			return string(utf8.RuneError), len(b)
		}
		second := normalizeGraphicByte(b[i+1], b[i+1] >= 0xa1)
		if (set == aribSetAdditionalSymbols || set == aribSetDRCS0) && (second < 0x21 || second > 0x7e) {
			return "", i + 2
		}
		if set == aribSetKanji && d.narrowAlnum && first == 0x21 && second == 0x21 {
			return " ", i + 2
		}
		if set == aribSetKanji && d.narrowAlnum && first == 0x23 {
			return decodeARIBNarrowJISRoman(second), i + 2
		}
		switch set {
		case aribSetAdditionalSymbols:
			return decodeARIBAdditionalSymbol(first, second), i + 2
		case aribSetDRCS0:
			return decodeARIBDRCS0(first, second), i + 2
		case aribSetJISCompatibleKanjiPlane1:
			return decodeARIBJISCompatibleKanji(1, first, second), i + 2
		case aribSetJISCompatibleKanjiPlane2:
			return decodeARIBJISCompatibleKanji(2, first, second), i + 2
		default:
			return decodeJISX0208Graphic(first, second), i + 2
		}
	case aribSetAlphanumeric, aribSetProportionalAlphanumeric:
		return string(d.decodeARIBAlnum(first)), i + 1
	case aribSetHiragana, aribSetProportionalHiragana:
		return decodeARIBHiragana(first), i + 1
	case aribSetKatakana, aribSetProportionalKatakana:
		return decodeARIBKatakana(first), i + 1
	case aribSetJISX0201Katakana:
		return string(decodeJISX0201Katakana(first)), i + 1
	case aribSetDRCS1Byte:
		return string(utf8.RuneError), i + 1
	case aribSetMacro:
		return "", i + 1
	default:
		return string(utf8.RuneError), i + 1
	}
}

func normalizeGraphicByte(b byte, gr bool) byte {
	if gr {
		return b & 0x7f
	}
	return b
}

func (d *aribStringDecoder) decodeARIBAlnum(b byte) rune {
	if b == 0x20 {
		return '\u3000'
	}
	if d.narrowAlnum && b >= 0x21 && b <= 0x7e {
		return rune(b)
	}
	if b == 0x22 {
		return '”'
	}
	if b >= 0x30 && b <= 0x39 {
		if d.narrowAlnum {
			return '0' + rune(b-0x30)
		}
		return '０' + rune(b-0x30)
	}
	if b >= 0x41 && b <= 0x5a {
		return 'Ａ' + rune(b-0x41)
	}
	if b >= 0x61 && b <= 0x7a {
		return 'ａ' + rune(b-0x61)
	}
	if b >= 0x21 && b <= 0x7e {
		return 0xff01 + rune(b-0x21)
	}
	return utf8.RuneError
}

func decodeJISX0201Katakana(b byte) rune {
	if b >= 0x21 && b <= 0x5f {
		return 0xff61 + rune(b-0x21)
	}
	return utf8.RuneError
}

func decodeARIBHiragana(b byte) string {
	if s, ok := aribHiraganaOverrides[b]; ok {
		return s
	}
	return decodeJISX0208Graphic(0x24, b)
}

func decodeARIBKatakana(b byte) string {
	if s, ok := aribKatakanaOverrides[b]; ok {
		return s
	}
	return decodeJISX0208Graphic(0x25, b)
}

func decodeARIBNarrowJISRoman(b byte) string {
	if b >= 0x21 && b <= 0x7e {
		return string(rune(b))
	}
	return string(utf8.RuneError)
}

func decodeARIBAdditionalSymbol(first, second byte) string {
	if s, ok := lookupARIBTable719AdditionalSymbol(first, second); ok {
		return s
	}
	if s := decodeJISX0208Graphic(first, second); s != string(utf8.RuneError) {
		return s
	}
	return string(utf8.RuneError)
}

func decodeARIBDRCS0(first, second byte) string {
	if s, ok := lookupARIBTable719AdditionalSymbol(first, second); ok {
		return s
	}
	return string(utf8.RuneError)
}

func decodeARIBJISCompatibleKanji(plane byte, first, second byte) string {
	if s, ok := lookupARIBTable721BMPKanjiPUA(plane, first, second); ok {
		return s
	}
	if s, ok := lookupJISX0213(plane, first, second); ok {
		return s
	}
	return string(utf8.RuneError)
}

var aribHiraganaOverrides = map[byte]string{
	0x77: "ゝ",
	0x78: "ゞ",
	0x79: "ー",
	0x7a: "。",
	0x7b: "「",
	0x7c: "」",
	0x7d: "、",
	0x7e: "・",
}

var aribKatakanaOverrides = map[byte]string{
	0x77: "ヽ",
	0x78: "ヾ",
	0x79: "ー",
	0x7a: "。",
	0x7b: "「",
	0x7c: "」",
	0x7d: "、",
	0x7e: "・",
}

var aribJISX0208Overrides = map[uint16]string{
	0x213d: "—",
	0x2141: "〜",
	0x215d: "−",
}

func decodeJISX0208Graphic(first, second byte) string {
	if !aribValidRowCell(first, second) {
		return string(utf8.RuneError)
	}
	if first == 0x75 || first == 0x76 || first >= 0x7a {
		if s, ok := lookupARIBTable719AdditionalSymbol(first, second); ok {
			return s
		}
	}
	if s, ok := aribJISX0208Overrides[aribRowCellKey(first, second)]; ok {
		return s
	}
	s, err := decodeJISX0208Pair(first, second)
	if err != nil || s == "" {
		return string(utf8.RuneError)
	}
	return s
}

func (d *aribStringDecoder) skipC1Control(b []byte, i int) int {
	switch b[i] {
	case 0x89:
		d.narrowAlnum = true
		return i + 1
	case 0x8a:
		d.narrowAlnum = false
		return i + 1
	case 0x90, 0x91, 0x92, 0x97, 0x98:
		return min(i+2, len(b))
	case 0x9b:
		j := i + 1
		for j < len(b) {
			if b[j] >= 0x40 && b[j] <= 0x7e {
				return j + 1
			}
			j++
		}
		return len(b)
	case 0x9d:
		return min(i+3, len(b))
	default:
		return i + 1
	}
}

func decodeJISX0208Pair(first, second byte) (string, error) {
	input := [8]byte{0x1b, 0x24, 0x42, first, second, 0x1b, 0x28, 0x42}
	var out [utf8.UTFMax]byte
	nDst, _, err := japanese.ISO2022JP.NewDecoder().Transform(out[:], input[:], true)
	if err != nil {
		return "", err
	}
	s := string(out[:nDst])
	if strings.ContainsRune(s, utf8.RuneError) {
		return "", fmt.Errorf("jis x 0208: undecodable pair %02x %02x", first, second)
	}
	return s, nil
}
