package ts

import "testing"

func TestDecodeARIBString(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want string
	}{
		{
			name: "alphanumeric",
			in:   []byte{0x0e, 'N', 'H', 'K', ' ', 'B', 'S'},
			want: "ＮＨＫ　ＢＳ",
		},
		{
			name: "jis x 0208",
			in:   []byte{0x41, 0x6d, 0x39, 0x67},
			want: "総合",
		},
		{
			name: "fixture mixed controls",
			in: []byte{
				0x0e, 'N', 'H', 'K',
				0x0f, 0x41, 0x6d, 0x39, 0x67,
				0x0e, '1', 0xfe,
				0x0f, 0x45, 0x6c, 0x35, 0x7e,
			},
			want: "ＮＨＫ総合１・東京",
		},
		{
			name: "escape single shift alphanumeric",
			in:   []byte{0x1b, 0x7e, 0xc2, 0xd3},
			want: "ＢＳ",
		},
		{
			name: "hiragana and katakana single shift",
			in:   []byte{0xce, 0xfb, 0x1d, 0x4b, 0x1d, 0x65, 0xf9, 0x1d, 0x39},
			want: "の「ニュース",
		},
		{
			name: "additional symbols use ARIB UCS mapping",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x56, 0x7a, 0x5a, 0x7a, 0x66},
			want: "\ue0fe\ue182\ue18e",
		},
		{
			name: "additional symbols standard unicode",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7e, 0x21, 0x7e, 0x61, 0x7d, 0x6f},
			want: "Ⅰ①⁉",
		},
		{
			name: "additional symbols rows 85 and 86 use ucs mapping",
			in:   []byte{0x1b, 0x24, 0x3b, 0x75, 0x21, 0x75, 0x6e, 0x76, 0x21, 0x76, 0x2b},
			want: "\u3402\ufa4a\u9fc5\ufa6d",
		},
		{
			name: "jis compatible kanji plane 1 uses table 7-21 pua mapping",
			in:   []byte{0x1b, 0x24, 0x39, 0x2e, 0x22, 0x75, 0x3a, 0x7e, 0x66},
			want: "\ue760\ue767\ue778",
		},
		{
			name: "jis compatible kanji plane 2 uses table 7-21 pua mapping",
			in:   []byte{0x1b, 0x24, 0x3a, 0x21, 0x21, 0x2e, 0x56, 0x7e, 0x76},
			want: "\ue779\ue7ce\ue88d",
		},
		{
			name: "jis compatible kanji plane 2 unknown falls back to replacement",
			in:   []byte{0x1b, 0x24, 0x3a, 0x21, 0x22},
			want: "\ufffd",
		},
		{
			name: "additional symbols rows 90 to 94 table 7-20 values",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x21, 0x7b, 0x2b, 0x7c, 0x7b, 0x7d, 0x79, 0x7e, 0x7b},
			want: "\u26cc\u3245\u213b\u269f\u24eb",
		},
		{
			name: "additional symbols duplicate encodings normalize to public ucs",
			in: []byte{
				0x1b, 0x24, 0x3b,
				0x7b, 0x28,
				0x7c, 0x27, 0x7c, 0x28, 0x7c, 0x29, 0x7c, 0x2a,
				0x7d, 0x7b,
			},
			want: "〒年月日円☎",
		},
		{
			name: "additional symbols private duplicate encodings canonicalize",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x5a, 0x7d, 0x3e},
			want: "\ue182\ue182",
		},
		{
			name: "kanji rows 90 to 94 use additional symbol ucs mapping",
			in:   []byte{0x7e, 0x21, 0x7e, 0x61, 0x7d, 0x6f},
			want: "Ⅰ①⁉",
		},
		{
			name: "apr newline",
			in:   []byte{0x46, 0x7c, 0x4b, 0x5c, 0x38, 0x6c, 0x0d, 0x31, 0x51, 0x38, 0x6c},
			want: "日本語\n英語",
		},
		{
			name: "escape designates katakana to gr",
			in:   []byte{0x1b, 0x7c, 0xcd, 0xc3, 0xc8, 0xef, 0xf9, 0xaf},
			want: "ネットワーク",
		},
		{
			name: "preserve fullwidth spaces",
			in:   []byte{0x0e, 'N', 'H', 'K', ' ', ' ', 'B', 'S'},
			want: "ＮＨＫ　　ＢＳ",
		},
		{
			name: "dangling kanji byte",
			in:   []byte{0x41},
			want: "\ufffd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeARIBString(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("DecodeARIBString(% x) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
