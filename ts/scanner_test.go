package ts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestServiceScannerMatchesCompatibilityFixtures(t *testing.T) {
	cases := []struct {
		name              string
		inputPath         string
		compatibilityPath string
	}{
		{
			name:              "gr-27",
			inputPath:         "testdata/local/test-gr-27.ts",
			compatibilityPath: "testdata/local/mirakc-arib-scan-services-gr-27.json",
		},
		{
			name:              "bs-15",
			inputPath:         "testdata/local/test-bs-15.ts",
			compatibilityPath: "testdata/local/mirakc-arib-scan-services-bs-15.json",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !fileExists(tc.inputPath) || !fileExists(tc.compatibilityPath) {
				t.Skip("local TS fixture or compatibility output fixture not found")
			}
			input, err := os.Open(tc.inputPath)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()

			got, err := NewServiceScanner().ScanServices(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}

			wantBytes, err := os.ReadFile(tc.compatibilityPath)
			if err != nil {
				t.Fatal(err)
			}
			want := decodeServiceInfoJSON(t, wantBytes)
			if !containsServices(got, want) {
				t.Fatalf("ScanServices = %#v, want it to contain compatibility services %#v", got, want)
			}
		})
	}
}

func TestParseSDTParsesServiceDescriptors(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID: 100,
			descriptors: serviceDescriptor(1, nil, []byte{
				0x0e, 'N', 'H', 'K', 0x0f, 0x41, 0x6d,
			}),
		},
	})

	sdt, err := ParseSDT(section)
	if err != nil {
		t.Fatal(err)
	}
	if sdt.TransportStreamID != 0x1234 || sdt.OriginalNetworkID != 0x5678 {
		t.Fatalf("SDT ids = %#v/%#v, want 0x1234/0x5678", sdt.TransportStreamID, sdt.OriginalNetworkID)
	}
	if len(sdt.Services) != 1 || sdt.Services[0].ServiceID != 100 {
		t.Fatalf("SDT services = %#v", sdt.Services)
	}
	desc, err := ParseServiceDescriptor(sdt.Services[0].Descriptors[0])
	if err != nil {
		t.Fatal(err)
	}
	if desc.ServiceType != 1 || desc.ServiceName != "ＮＨＫ総" {
		t.Fatalf("service descriptor = %#v", desc)
	}
}

func TestParseSDTRejectsBrokenCRC(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, nil)
	section[len(section)-1] ^= 0xff
	if _, err := ParseSDT(section); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("ParseSDT error = %v, want ErrInvalidSection", err)
	}
}

func TestServiceScannerSkipsBrokenServiceDescriptor(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID:   100,
			descriptors: []byte{DescriptorTagService, 2, 1, 5},
		},
	})
	input := append(sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}), 0), sectionPackets(PIDSDT, section, 0)...)

	got, err := NewServiceScanner().ScanServices(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ScanServices returned %#v, want no services", got)
	}
}

func TestServiceScannerDoesNotFilterServiceTypes(t *testing.T) {
	var input []byte
	input = append(input, sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{
		100: 0x0100,
		101: 0x0101,
	}), 0)...)
	input = append(input, sectionPackets(PIDSDT, buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID:   100,
			descriptors: serviceDescriptor(0xAD, nil, []byte{0x0e, '4', 'K'}),
		},
		{
			serviceID:   101,
			descriptors: serviceDescriptor(0xC0, nil, []byte{0x0e, 'D', 'A', 'T', 'A'}),
		},
	}), 0)...)

	got, err := NewServiceScanner().ScanServices(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	want := []ServiceInfo{
		{Nid: 0x5678, Tsid: 0x1234, Sid: 100, Name: "４Ｋ", Type: 0xAD, LogoId: -1},
		{Nid: 0x5678, Tsid: 0x1234, Sid: 101, Name: "ＤＡＴＡ", Type: 0xC0, LogoId: -1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanServices returned %#v, want %#v", got, want)
	}
}

func TestServiceScannerHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewServiceScanner().ScanServices(ctx, bytes.NewReader(nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ScanServices error = %v, want context.Canceled", err)
	}
}

func decodeServiceInfoJSON(t *testing.T, data []byte) []ServiceInfo {
	t.Helper()
	var services []ServiceInfo
	if err := json.Unmarshal(data, &services); err != nil {
		t.Fatalf("decode service JSON %q: %v", string(data), err)
	}
	return services
}

func containsServices(got, want []ServiceInfo) bool {
	gotByID := make(map[uint16]ServiceInfo, len(got))
	for _, svc := range got {
		gotByID[svc.Sid] = svc
	}
	for _, svc := range want {
		gotSvc, ok := gotByID[svc.Sid]
		if ok {
			gotSvc.LogoVersion = svc.LogoVersion
			gotSvc.LogoDownloadDataId = svc.LogoDownloadDataId
		}
		if !ok || !reflect.DeepEqual(gotSvc, svc) {
			return false
		}
	}
	return true
}

type sdtServiceSpec struct {
	serviceID   uint16
	descriptors []byte
}

func buildSDT(t *testing.T, tsid, onid uint16, services []sdtServiceSpec) Section {
	t.Helper()
	serviceLoopLen := 0
	for _, svc := range services {
		serviceLoopLen += 5 + len(svc.descriptors)
	}
	sectionLength := 8 + serviceLoopLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDSDT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(tsid >> 8)
	s[4] = byte(tsid)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = byte(onid >> 8)
	s[9] = byte(onid)
	s[10] = 0xff
	off := 11
	for _, svc := range services {
		s[off] = byte(svc.serviceID >> 8)
		s[off+1] = byte(svc.serviceID)
		s[off+2] = 0xff
		s[off+3] = 0xf0 | byte(len(svc.descriptors)>>8)
		s[off+4] = byte(len(svc.descriptors))
		copy(s[off+5:], svc.descriptors)
		off += 5 + len(svc.descriptors)
	}
	writeCRC(s)
	return Section(s)
}

func serviceDescriptor(serviceType uint8, providerName, serviceName []byte) []byte {
	data := []byte{serviceType, byte(len(providerName))}
	data = append(data, providerName...)
	data = append(data, byte(len(serviceName)))
	data = append(data, serviceName...)
	return append([]byte{DescriptorTagService, byte(len(data))}, data...)
}
