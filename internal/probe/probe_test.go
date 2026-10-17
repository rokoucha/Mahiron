package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/mmt/mmttest"
	"github.com/21S1298001/mahiron/ts"
)

func tsPacket(pid uint16, control byte, payload []byte) []byte {
	p := bytes.Repeat([]byte{0xff}, 188)
	p[0] = 0x47
	p[1] = byte(pid >> 8)
	p[2] = byte(pid)
	p[3] = 0x10 | control<<6
	copy(p[4:], payload)
	return p
}
func tsSection(pid uint16, section []byte) []byte {
	p := tsPacket(pid, 0, append([]byte{0}, section...))
	p[1] |= 0x40
	return p
}
func tsFixture(t *testing.T) [][]byte {
	t.Helper()
	pat, err := ts.BuildPATSection(1, 101, 0x1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	pmt := mmttest.Section(ts.TableIDPMT, 101, 0, 0, 0, []byte{
		0xe1, 0, 0xf0, 6, 9, 4, 0, 5, 0xe1, 0xf0,
		0x1b, 0xe1, 0, 0xf0, 0,
		0x0f, 0xe1, 1, 0xf0, 0,
		6, 0xe1, 2, 0xf0, 10, 0xfd, 2, 0, 8, 9, 4, 0, 5, 0xff, 0xff,
		6, 0xe1, 3, 0xf0, 4, 0xfd, 2, 0, 0x12,
	})
	desc := []byte{0x48, 9, 1, 0, 6, 0x0e, 'T', 'E', 'S', 'T', 0x0f}
	body := append([]byte{0, 1, 0xff, 0, 101, 0xff, 0x90, byte(len(desc))}, desc...)
	sdt := mmttest.Section(ts.TableIDSDT0, 1, 0, 0, 0, body)
	return [][]byte{tsSection(0, pat), tsSection(0x1000, pmt), tsSection(ts.PIDSDT, sdt), tsPacket(0x100, 2, nil), tsPacket(0x100, 0, nil), tsPacket(0x101, 0, nil), tsPacket(0x1f0, 0, nil), tsPacket(ts.PIDNull, 0, nil)}
}
func runFixture(t *testing.T, input []byte, opt Options) *Report {
	t.Helper()
	r, err := Run(context.Background(), bytes.NewReader(input), opt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestTSResidualScramblingAndMissingComponents(t *testing.T) {
	r := runFixture(t, bytes.Join(tsFixture(t), nil), Options{})
	if r.Format != "ts" || len(r.Services) != 1 {
		t.Fatalf("report: %+v", r)
	}
	s := r.Services[0]
	if s.Name == "" || !s.MapReceived || !s.Listed || s.CADeclared == nil || !*s.CADeclared {
		t.Fatalf("service: %+v", s)
	}
	if len(s.CA) != 1 || s.CA[0].Packets != 1 {
		t.Fatalf("CA: %+v", s.CA)
	}
	video, audio, sub := s.Components[0], s.Components[1], s.Components[2]
	if len(sub.CA) != 1 || sub.CA[0].Packets != 0 {
		t.Fatalf("null PID counted as ECM: %+v", sub.CA)
	}
	if video.Stats.Packets != 2 || video.Stats.Scrambled != 1 || video.Stats.Status() != "scrambling remains" || video.Stats.FirstScrambled == nil {
		t.Fatalf("video: %+v", video)
	}
	if audio.Stats.Status() != "no scrambling observed" {
		t.Fatalf("audio: %+v", audio)
	}
	if s.Components[3].Kind != "subtitle" {
		t.Fatalf("C-profile subtitle: %+v", s.Components[3])
	}
	if sub.Kind != "subtitle" || sub.Stats.Status() != "unknown" {
		t.Fatalf("subtitle: %+v", sub)
	}
	// Filtering can remove SDT while leaving useful service/component discovery.
	packets := tsFixture(t)
	packets = append(packets[:2], packets[3:]...)
	r = runFixture(t, bytes.Join(packets, nil), Options{})
	if len(r.Services) != 1 || r.Services[0].Name != "" || !r.Services[0].MapReceived || len(r.Warnings) == 0 {
		t.Fatalf("filtered: %+v", r)
	}
}
func TestTSReservedAndCorruptPacketsAreNotReportedClear(t *testing.T) {
	state := newTSState(0)
	p := ts.Packet(tsPacket(0x100, 1, nil))
	state.feed(p, time.Second)
	p = tsPacket(0x100, 0, nil)
	p[1] |= 0x80
	state.feed(p, 2*time.Second)
	s := state.stats[0x100]
	if s.Packets != 1 || s.Unknown != 1 || s.TransportErrors != 1 || s.Status() != "unknown" {
		t.Fatalf("stats: %+v", s)
	}
}
func writeCRC(b []byte) {
	var value uint32 = 0xffffffff
	for _, v := range b[:len(b)-4] {
		value ^= uint32(v) << 24
		for range 8 {
			if value&0x80000000 != 0 {
				value = value<<1 ^ 0x04c11db7
			} else {
				value <<= 1
			}
		}
	}
	binary.BigEndian.PutUint32(b[len(b)-4:], value)
}
func tsmfFixture(t *testing.T) []byte {
	t.Helper()
	header := make([]byte, 188)
	header[0], header[2], header[3], header[4], header[5], header[6] = 0x47, 0x2f, 0x10, 0xfa, 0x86, 1
	for i := range 26 {
		header[73+i] = 0x12
	}
	writeCRC(header[4:])
	out := append([]byte(nil), header...)
	for _, p := range tsFixture(t) {
		out = append(out, p...)
		q := bytes.Clone(p)
		if ts.Packet(q).PID() == 0x100 {
			q[3] &= 0x3f
		}
		out = append(out, q...)
	}
	return out
}
func TestTSMFIsolatesReusedServiceIDsAndPIDs(t *testing.T) {
	for _, stride := range []int{188, 192, 204} {
		var input []byte
		raw := tsmfFixture(t)
		for len(raw) > 0 {
			if stride == 192 {
				input = append(input, 0, 0, 0, 0)
			}
			input = append(input, raw[:188]...)
			if stride == 204 {
				input = append(input, make([]byte, 16)...)
			}
			raw = raw[188:]
		}
		r := runFixture(t, input, Options{})
		if r.Format != "tsmf" || len(r.Services) != 2 || r.PacketStride != stride {
			t.Fatalf("stride %d: %+v", stride, r)
		}
		if r.Services[0].RelativeTS != 1 || r.Services[0].Components[0].Stats.Scrambled != 1 || r.Services[1].RelativeTS != 2 || r.Services[1].Components[0].Stats.Scrambled != 0 {
			t.Fatalf("relative TS mixed: %+v", r.Services)
		}
		r = runFixture(t, input, Options{RelativeTS: 2})
		if len(r.Services) != 1 || r.Services[0].RelativeTS != 2 {
			t.Fatalf("selection: %+v", r)
		}
	}
}
func mmtFixture(port uint16, scrambled bool) []byte {
	src, dst := [16]byte{0x20, 1}, [16]byte{0xff, 2}
	wrap := func(id uint16, payloadType byte, payload, ext []byte) []byte {
		return mmttest.IPv6UDP(src, dst, 1000, port, mmttest.MMTP{PacketID: id, PayloadType: payloadType, Payload: payload, Extension: ext}.Bytes())
	}
	// An asset at packet_id 0x100 in the same flow.
	asset := []byte{0, 0, 0, 0, 0, 1, 1, 'h', 'e', 'v', '1', 0, 1, 0, 1, 0, 0, 0}
	body := append([]byte{0, 2, 0, 101, 0, 0, 1}, asset...)
	pa := mmttest.PAMessage(0, mmttest.Table(mmt.TableIDMPT, 0, body))
	out := wrap(0, 2, mmttest.SignalingPayload(0, 0, pa), nil)
	descriptor := mmttest.Descriptor(mmt.DescriptorTagMHService, []byte{1, 0, 4, 'T', 'e', 's', 't'})
	flowData := append([]byte{0x81, 0}, src[:]...)
	flowData = append(flowData, dst[:]...)
	flowData = append(flowData, 3, 0xe8, byte(port>>8), byte(port))
	flowDescriptor := mmttest.Descriptor(mmt.DescriptorTagIPDataFlow, flowData)
	sdtBody := append([]byte{0, 1, 0xff, 0, 101, 0xff}, mmttest.Loop(descriptor, flowDescriptor)...)
	section := mmttest.Section(mmt.TableIDMHSDTActual, 1, 0, 0, 0, sdtBody)
	originalPort := port
	port = 2000 // Common SI flow differs from the service media flow.
	out = append(out, wrap(mmt.PacketIDMHSDT, 2, mmttest.SignalingPayload(0, 0, mmttest.M2SectionMessage(0, section)), nil)...)
	port = originalPort
	var ext []byte
	if scrambled {
		ext = []byte{0x80, 1, 0, 1, 0x10}
	}
	return append(out, wrap(0x100, 0, []byte{1, 2, 3}, ext)...)
}
func TestTLVSeparatesFlowsAndObservesMMTScrambling(t *testing.T) {
	input := append(mmtFixture(1000, true), mmtFixture(1001, false)...)
	r := runFixture(t, input, Options{})
	if r.Format != "tlv" || len(r.Services) != 2 {
		t.Fatalf("report: %+v", r)
	}
	var scrambled, clear int
	for _, s := range r.Services {
		if !s.MapReceived || s.Name != "Test" || len(s.Components) != 1 {
			t.Fatalf("service: %+v", s)
		}
		c := s.Components[0]
		if c.Kind != "video" || c.Stats.Packets != 1 {
			t.Fatalf("component: %+v", c)
		}
		if c.Stats.Scrambled == 1 {
			scrambled++
		} else {
			clear++
		}
	}
	if scrambled != 1 || clear != 1 {
		t.Fatalf("mixed flows: %+v", r.Services)
	}
}
func TestMMTScramblingExtensions(t *testing.T) {
	for _, tc := range []struct {
		ext  *mmt.MMTPHeaderExtension
		want byte
	}{
		{nil, 0}, {&mmt.MMTPHeaderExtension{Type: 99}, 1},
		{&mmt.MMTPHeaderExtension{Data: []byte{0x80, 1, 0, 1, 0x18}}, 3},
		{&mmt.MMTPHeaderExtension{Data: []byte{0, 2, 0, 1, 0, 0x80, 1, 0, 1, 0x10}}, 2},
		{&mmt.MMTPHeaderExtension{Data: []byte{0x80, 1, 0, 2, 0x10}}, 1},
	} {
		if got := mmtScrambling(tc.ext); got != tc.want {
			t.Errorf("%+v: got %d want %d", tc.ext, got, tc.want)
		}
	}
}
func TestUnknownInputRejected(t *testing.T) {
	if _, err := Run(context.Background(), bytes.NewReader([]byte("not a stream")), Options{}); err == nil {
		t.Fatal("unknown input accepted")
	}
}

func TestMPTSubsetsDoNotHideScrambling(t *testing.T) {
	var input []byte
	for i, codec := range []string{"hev1", "mp4a"} {
		id := uint16(0x100 + i)
		asset := []byte{0, 0, 0, 0, 0, 1, byte(i), 0, 0, 0, 0, 0, 1, 0, byte(id >> 8), byte(id), 0, 0}
		copy(asset[7:11], codec)
		body := append([]byte{2, 2, 0, 101, 0, 0, 1}, asset...)
		pa := mmttest.PAMessage(0, mmttest.Table(byte(0x11+i), 0, body))
		src, dst := [16]byte{0x20, 1}, [16]byte{0xff, 2}
		input = append(input, mmttest.IPv6UDP(src, dst, 1000, 1000, mmttest.MMTP{PacketID: 0, PayloadType: 2, Payload: mmttest.SignalingPayload(0, 0, pa)}.Bytes())...)
		var ext []byte
		if i == 0 {
			ext = []byte{0x80, 1, 0, 1, 0x10}
		}
		input = append(input, mmttest.IPv6UDP(src, dst, 1000, 1000, mmttest.MMTP{PacketID: id, PayloadType: 0, Extension: ext, Payload: []byte{1}}.Bytes())...)
	}
	r := runFixture(t, input, Options{})
	if len(r.Services) != 1 || len(r.Services[0].Components) != 2 || r.Services[0].Scrambling != "scrambling remains" {
		t.Fatalf("subset lost: %+v components=%+v", r.Services[0], r.Services[0].Components)
	}
}

func TestFilteredPATExcludesOtherSDTServices(t *testing.T) {
	packets := tsFixture(t)
	// A service filter rewrites PAT but can preserve the entire SDT.
	section := mmttest.Section(ts.TableIDSDT0, 1, 0, 0, 0, []byte{0, 1, 0xff, 0, 101, 0xff, 0x80, 0, 0, 202, 0xff, 0x80, 0})
	packets[2] = tsSection(ts.PIDSDT, section)
	r := runFixture(t, bytes.Join(packets, nil), Options{})
	if len(r.Services) != 1 || r.Services[0].ID != 101 {
		t.Fatalf("SDT-only service leaked: %+v", r.Services)
	}
	for _, w := range r.Warnings {
		if strings.Contains(w, "PMT/MPT not received") || strings.Contains(w, "relative TS 0") {
			t.Fatalf("misleading warning: %s", w)
		}
	}
	// Without PAT, SDT still supplies useful partial discovery.
	r = runFixture(t, packets[2], Options{})
	if len(r.Services) != 2 {
		t.Fatalf("SDT-only discovery lost: %+v", r.Services)
	}
}

func TestEmptyCompositionIsJSONArray(t *testing.T) {
	pat, err := ts.BuildPATSection(1, 101, 0x1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	pmt := mmttest.Section(ts.TableIDPMT, 101, 0, 0, 0, []byte{0xe1, 0, 0xf0, 0})
	tsInput := append(tsSection(0, pat), tsSection(0x1000, pmt)...)
	pa := mmttest.PAMessage(0, mmttest.Table(mmt.TableIDMPT, 0, []byte{0, 2, 0, 101, 0, 0, 0}))
	tlvInput := mmttest.IPv6UDP([16]byte{0x20, 1}, [16]byte{0xff, 2}, 1000, 1000, mmttest.MMTP{PacketID: 0, PayloadType: 2, Payload: mmttest.SignalingPayload(0, 0, pa)}.Bytes())
	for _, input := range [][]byte{tsInput, tlvInput} {
		r := runFixture(t, input, Options{})
		if len(r.Services) != 1 || r.Services[0].Components == nil {
			t.Fatalf("nil components: %+v", r)
		}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte(`"components":[]`)) {
			t.Fatalf("JSON: %s", data)
		}
	}
}

func TestRelativeTSOnPlainTSHasSpecificWarning(t *testing.T) {
	r := runFixture(t, bytes.Join(tsFixture(t), nil), Options{RelativeTS: 2})
	if !strings.Contains(strings.Join(r.Warnings, "\n"), "TSMF header not observed; --relative-ts 2 cannot be applied") {
		t.Fatalf("warnings: %v", r.Warnings)
	}
}

func TestWarningsIdentifyRelativeTS(t *testing.T) {
	input := tsmfFixture(t)
	for off := 0; off+188 <= len(input); off += 188 {
		p := input[off : off+188]
		if ts.Packet(p).PID() == ts.PIDSDT {
			p[1] = 0x1f
			p[2] = 0xff
		}
	}
	r := runFixture(t, input, Options{})
	for _, id := range []int{1, 2} {
		want := fmt.Sprintf("service 101 (relative TS %d): service name unavailable", id)
		if !slices.Contains(r.Warnings, want) {
			t.Fatalf("missing %q in %v", want, r.Warnings)
		}
	}
}

func TestPATRemapInvalidatesOldComposition(t *testing.T) {
	input := bytes.Join(tsFixture(t), nil)
	pat, err := ts.BuildPATSection(1, 101, 0x1001, 1)
	if err != nil {
		t.Fatal(err)
	}
	packet := tsSection(0, pat)
	packet[3]++
	r := runFixture(t, append(input, packet...), Options{})
	s := r.Services[0]
	if s.MapReceived || s.MapComplete || len(s.Components) != 0 || s.MapID == nil || *s.MapID != 0x1001 {
		t.Fatalf("stale map: %+v", s)
	}
}
