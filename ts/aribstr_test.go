package ts

import "testing"

type aribStringDecodeTest struct {
	name string
	in   []byte
	want string
}

func runARIBStringDecodeTests(t *testing.T, tests []aribStringDecodeTest) {
	t.Helper()

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

func TestDecodeARIBStringBasicGraphicSets(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "alphanumeric",
			in:   []byte{0x0e, 'A', 'B', 'C', ' ', '1', '2'},
			want: "ＡＢＣ　１２",
		},
		{
			name: "jis x 0208",
			in:   []byte{0x41, 0x6d, 0x39, 0x67},
			want: "総合",
		},
		{
			name: "hiragana and katakana single shift",
			in:   []byte{0xce, 0xfb, 0x1d, 0x4b, 0x1d, 0x65, 0xf9, 0x1d, 0x39},
			want: "の「ニュース",
		},
		{
			name: "escape designates katakana to gr",
			in:   []byte{0x1b, 0x7c, 0xcd, 0xc3, 0xc8, 0xef, 0xf9, 0xaf},
			want: "ネットワーク",
		},
	})
}

func TestDecodeARIBStringControlsAndSpacing(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "fixture mixed controls",
			in: []byte{
				0x0e, 'A', 'B', 'C',
				0x0f, 0x41, 0x6d, 0x39, 0x67,
				0x0e, '1', 0xfe,
				0x0f, 0x45, 0x6c, 0x35, 0x7e,
			},
			want: "ＡＢＣ総合１・東京",
		},
		{
			name: "escape single shift alphanumeric",
			in:   []byte{0x1b, 0x7e, 0xc1, 0xd2},
			want: "ＡＲ",
		},
		{
			name: "apr newline",
			in:   []byte{0x46, 0x7c, 0x4b, 0x5c, 0x38, 0x6c, 0x0d, 0x31, 0x51, 0x38, 0x6c},
			want: "日本語\n英語",
		},
		{
			name: "preserve fullwidth spaces",
			in:   []byte{0x0e, 'A', 'B', 'C', ' ', ' ', '1', '2'},
			want: "ＡＢＣ　　１２",
		},
	})
}

func TestDecodeARIBStringAdditionalSymbols(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "public unicode mapping",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x56, 0x7a, 0x5a, 0x7a, 0x66},
			want: "\U0001f211\U0001f214\U0001f21b",
		},
		{
			name: "standard unicode",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7e, 0x21, 0x7e, 0x61, 0x7d, 0x6f},
			want: "Ⅰ①⁉",
		},
		{
			name: "rows 85 and 86 ucs mapping",
			in:   []byte{0x1b, 0x24, 0x3b, 0x75, 0x21, 0x75, 0x6e, 0x76, 0x21, 0x76, 0x2b},
			want: "\u3402\ufa4a\u9fc5\ufa6d",
		},
		{
			name: "rows 85 and 86 former pua public unicode",
			in: []byte{
				0x1b, 0x24, 0x3b,
				0x75, 0x22, 0x75, 0x2f, 0x75, 0x55, 0x75, 0x64,
			},
			want: "\U00020158\U00020bb7\U000233cc\U000242ee",
		},
		{
			name: "rows 90 to 94 table values",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x21, 0x7b, 0x2b, 0x7c, 0x7b, 0x7d, 0x79, 0x7e, 0x7b},
			want: "\u26cc\u3245\u213b\u269f\u24eb",
		},
		{
			name: "rows 90 to 94 former pua public unicode",
			in: []byte{
				0x1b, 0x24, 0x3b,
				0x7a, 0x30, 0x7a, 0x55, 0x7b, 0x3d, 0x7d, 0x31,
			},
			want: "\U0001f17f\U0001f210\U0001f157\U0001f240",
		},
		{
			name: "duplicate encodings normalize to public unicode",
			in: []byte{
				0x1b, 0x24, 0x3b,
				0x7b, 0x28,
				0x7c, 0x27, 0x7c, 0x28, 0x7c, 0x29, 0x7c, 0x2a,
				0x7d, 0x7b,
			},
			want: "〒年月日円☎",
		},
		{
			name: "duplicate former pua encodings share public unicode",
			in:   []byte{0x1b, 0x24, 0x3b, 0x7a, 0x5a, 0x7d, 0x3e},
			want: "\U0001f214\U0001f214",
		},
	})
}

func TestDecodeARIBStringKanjiAdditionalRows(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "rows 90 to 94 use additional symbol mapping",
			in:   []byte{0x7e, 0x21, 0x7e, 0x61, 0x7d, 0x6f},
			want: "Ⅰ①⁉",
		},
		{
			name: "rows 85 and 86 use additional symbol mapping",
			in:   []byte{0x75, 0x21, 0x76, 0x21},
			want: "\u3402\u9fc5",
		},
	})
}

func TestDecodeARIBStringJISCompatibleKanji(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "plane 1 table 7-21 pua mapping",
			in:   []byte{0x1b, 0x24, 0x39, 0x2e, 0x22, 0x75, 0x3a, 0x7e, 0x66},
			want: "\ue760\ue767\ue778",
		},
		{
			name: "plane 1 jis x 0213 delta mapping",
			in:   []byte{0x1b, 0x24, 0x39, 0x22, 0x33},
			want: "\u3033",
		},
		{
			name: "plane 1 jis x 0213 combining mapping",
			in:   []byte{0x1b, 0x24, 0x39, 0x24, 0x77},
			want: "\u304b\u309a",
		},
		{
			name: "plane 2 table 7-21 pua mapping",
			in:   []byte{0x1b, 0x24, 0x3a, 0x21, 0x21, 0x2e, 0x56, 0x7e, 0x76},
			want: "\ue779\ue7ce\ue88d",
		},
		{
			name: "plane 2 jis x 0213 mapping",
			in:   []byte{0x1b, 0x24, 0x3a, 0x21, 0x22},
			want: "\u4e02",
		},
	})
}

func TestDecodeARIBStringMalformedInput(t *testing.T) {
	runARIBStringDecodeTests(t, []aribStringDecodeTest{
		{
			name: "dangling kanji byte",
			in:   []byte{0x41},
			want: "\ufffd",
		},
		{
			name: "jis compatible kanji plane 2 invalid cell",
			in:   []byte{0x1b, 0x24, 0x3a, 0x21, 0x20},
			want: "\ufffd",
		},
	})
}
