package channel

import (
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
)

// Section builders for scan tests. They mirror the ts package's own test
// builders (which stay unexported there); the CRC below is test-only code,
// not a third production copy.

// testCRC32Table is the MPEG-2 CRC table used to seal test sections.
var testCRC32Table = func() [256]uint32 {
	var table [256]uint32
	for i := range table {
		crc := uint32(i) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

func testWriteCRC(s []byte) {
	var crc uint32 = 0xffffffff
	for _, b := range s[:len(s)-4] {
		crc = (crc << 8) ^ testCRC32Table[byte(crc>>24)^b]
	}
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func testBuildPAT(t *testing.T, programs map[uint16]uint16) ts.Section {
	t.Helper()
	sectionLength := 5 + len(programs)*4 + 4
	s := make([]byte, 3+sectionLength)
	s[0] = ts.TableIDPAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x12, 0x34
	s[5], s[6], s[7] = 0xc1, 0, 0
	off := 8
	for serviceID, pmtPID := range programs {
		s[off] = byte(serviceID >> 8)
		s[off+1] = byte(serviceID)
		s[off+2] = 0xe0 | byte(pmtPID>>8)
		s[off+3] = byte(pmtPID)
		off += 4
	}
	testWriteCRC(s)
	return ts.Section(s)
}

type testSDTService struct {
	serviceID     uint16
	runningStatus byte
	freeCA        bool
	descriptors   []byte
}

func testServiceDescriptor(serviceType uint8, providerName, serviceName []byte) []byte {
	data := []byte{serviceType, byte(len(providerName))}
	data = append(data, providerName...)
	data = append(data, byte(len(serviceName)))
	data = append(data, serviceName...)
	return append([]byte{ts.DescriptorTagService, byte(len(data))}, data...)
}

func testBuildSDT(t *testing.T, tsid, onid uint16, services []testSDTService) ts.Section {
	t.Helper()
	serviceLoopLen := 0
	for _, svc := range services {
		serviceLoopLen += 5 + len(svc.descriptors)
	}
	sectionLength := 8 + serviceLoopLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = ts.TableIDSDT0
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
		s[off+3] = svc.runningStatus<<5 | byte(len(svc.descriptors)>>8)
		if svc.freeCA {
			s[off+3] |= 0x10
		}
		s[off+4] = byte(len(svc.descriptors))
		copy(s[off+5:], svc.descriptors)
		off += 5 + len(svc.descriptors)
	}
	testWriteCRC(s)
	return ts.Section(s)
}

type testNITTransport struct {
	tsid        uint16
	onid        uint16
	descriptors []byte
}

func testBuildNIT(t *testing.T, transports []testNITTransport) ts.Section {
	t.Helper()
	tsLoopLen := 0
	for _, transport := range transports {
		tsLoopLen += 6 + len(transport.descriptors)
	}
	sectionLength := 13 + tsLoopLen
	s := make([]byte, 3+sectionLength)
	s[0] = ts.TableIDNIT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x56, 0x78
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8], s[9] = 0xf0, 0
	s[10], s[11] = 0xf0|byte(tsLoopLen>>8), byte(tsLoopLen)
	off := 12
	for _, transport := range transports {
		s[off] = byte(transport.tsid >> 8)
		s[off+1] = byte(transport.tsid)
		s[off+2] = byte(transport.onid >> 8)
		s[off+3] = byte(transport.onid)
		s[off+4] = 0xf0 | byte(len(transport.descriptors)>>8)
		s[off+5] = byte(len(transport.descriptors))
		copy(s[off+6:], transport.descriptors)
		off += 6 + len(transport.descriptors)
	}
	testWriteCRC(s)
	return ts.Section(s)
}

func testTSInformationDescriptor(remoteControlKeyID uint8, name []byte) []byte {
	data := []byte{remoteControlKeyID, byte(len(name) << 2)}
	data = append(data, name...)
	return append([]byte{ts.DescriptorTagTSInformation, byte(len(data))}, data...)
}

func testWithTableHeader(section ts.Section, tableID, version, number, last byte) ts.Section {
	section = append(ts.Section(nil), section...)
	section[0] = tableID
	section[5] = 0xc1 | ((version & 0x1f) << 1)
	section[6] = number
	section[7] = last
	testWriteCRC(section)
	return section
}

func TestScanSkipsBrokenServiceDescriptor(t *testing.T) {
	section := testBuildSDT(t, 0x1234, 0x5678, []testSDTService{
		{serviceID: 100, descriptors: []byte{ts.DescriptorTagService, 2, 1, 5}},
	})
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100}))
	scan.Observe(section)
	if got := scan.Services(); len(got) != 0 {
		t.Fatalf("Services returned %#v, want no services", got)
	}
}

func TestScanDoesNotFilterServiceTypes(t *testing.T) {
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100, 101: 0x0101}))
	scan.Observe(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{
		{serviceID: 100, runningStatus: 4, freeCA: true, descriptors: testServiceDescriptor(0xAD, []byte{0x0e, 'N', 'H', 'K'}, []byte{0x0e, '4', 'K'})},
		{serviceID: 101, descriptors: testServiceDescriptor(0xC0, nil, []byte{0x0e, 'D', 'A', 'T', 'A'})},
	}))
	got := scan.Services()
	want := []model.Service{
		{Key: model.ServiceKey{NetworkID: 0x5678, StreamID: 0x1234, ServiceID: 100}, Name: "４Ｋ", ProviderName: "ＮＨＫ", Type: 0xAD, RunningStatus: 4, FreeCA: true, EITSchedule: true, EITPresentFollow: true},
		{Key: model.ServiceKey{NetworkID: 0x5678, StreamID: 0x1234, ServiceID: 101}, Name: "ＤＡＴＡ", Type: 0xC0, EITSchedule: true, EITPresentFollow: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Services returned %#v, want %#v", got, want)
	}
}

func TestScanCompletesWithoutSDTForEveryPATService(t *testing.T) {
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100, 0xfff0: 0x0101}))
	scan.Observe(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID:   100,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'N', 'H', 'K'}),
	}}))
	scan.Observe(testBuildNIT(t, nil))

	if !scan.Complete() {
		t.Fatal("service scan did not complete after complete PAT, SDT, and NIT tables")
	}
	services := scan.Services()
	if len(services) != 1 || services[0].Key.ServiceID != 100 {
		t.Fatalf("service list = %#v, want only SID 100", services)
	}
}

func TestScanWaitsForEveryTableSection(t *testing.T) {
	scan := newServiceScan()
	pat0 := testWithTableHeader(testBuildPAT(t, map[uint16]uint16{100: 0x0100}), ts.TableIDPAT, 0, 0, 1)
	pat1 := testWithTableHeader(testBuildPAT(t, map[uint16]uint16{101: 0x0101}), ts.TableIDPAT, 0, 1, 1)
	sdt0 := testWithTableHeader(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID:   100,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'A'}),
	}}), ts.TableIDSDT0, 0, 0, 1)
	sdt1 := testWithTableHeader(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID:   101,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'B'}),
	}}), ts.TableIDSDT0, 0, 1, 1)
	nit0 := testWithTableHeader(testBuildNIT(t, nil), ts.TableIDNIT0, 0, 0, 1)
	nit1 := testWithTableHeader(testBuildNIT(t, nil), ts.TableIDNIT0, 0, 1, 1)

	for _, section := range []ts.Section{pat0, sdt0, nit0, pat1, sdt1} {
		scan.Observe(section)
		if scan.Complete() {
			t.Fatal("service scan completed before all NIT sections arrived")
		}
	}
	scan.Observe(nit1)
	if !scan.Complete() {
		t.Fatal("service scan did not complete after all table sections arrived")
	}
	services := scan.Services()
	if len(services) != 2 || services[0].Key.ServiceID != 100 || services[1].Key.ServiceID != 101 {
		t.Fatalf("service list = %#v, want SIDs 100 and 101", services)
	}
}

func TestScanIgnoresOtherTransportSDT(t *testing.T) {
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100}))
	scan.Observe(testBuildNIT(t, nil))
	other := testWithTableHeader(testBuildSDT(t, 0x9999, 0x5678, []testSDTService{{
		serviceID:   100,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'X'}),
	}}), ts.TableIDSDT1, 0, 0, 0)
	scan.Observe(other)
	if scan.Complete() || len(scan.Services()) != 0 {
		t.Fatalf("other-TS SDT changed scan state: complete=%v services=%#v", scan.Complete(), scan.Services())
	}

	scan.Observe(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID:   100,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'A'}),
	}}))
	if !scan.Complete() {
		t.Fatal("actual-TS SDT did not complete service scan")
	}
}

func TestScanAppliesRemoteControlKeysFromNIT(t *testing.T) {
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100}))
	scan.Observe(testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID:   100,
		descriptors: testServiceDescriptor(1, nil, []byte{0x0e, 'A'}),
	}}))
	scan.Observe(testBuildNIT(t, []testNITTransport{
		{tsid: 0x1234, onid: 0x5678, descriptors: testTSInformationDescriptor(4, []byte{0x0e, 'A'})},
	}))
	services := scan.Services()
	if len(services) != 1 || services[0].RemoteControlKey == nil || *services[0].RemoteControlKey != 4 {
		t.Fatalf("services = %#v, want remote key 4", services)
	}
}

func TestScanUsesLogoTransmissionDescriptor(t *testing.T) {
	section := testBuildSDT(t, 0x1234, 0x5678, []testSDTService{{
		serviceID: 100,
		descriptors: append(
			testServiceDescriptor(1, nil, []byte{0x0e, 'L', 'O', 'G', 'O'}),
			ts.DescriptorTagLogoTransmission, 7, 0x01, 0xff, 0x2a, 0xf0, 0x01, 0x12, 0x34,
		),
	}})
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100}))
	scan.Observe(section)
	got := scan.Services()
	if len(got) != 1 || got[0].Logo == nil || got[0].Logo.LogoID != 0x12a {
		t.Fatalf("services = %#v", got)
	}
	if got[0].Logo.Version == nil || *got[0].Logo.Version != 1 {
		t.Fatalf("logo version = %v, want 1", got[0].Logo.Version)
	}
	if got[0].Logo.DownloadDataID == nil || *got[0].Logo.DownloadDataID != 0x1234 {
		t.Fatalf("logo download data id = %v, want 0x1234", got[0].Logo.DownloadDataID)
	}
}

func TestScanResolvesIndirectLogoTransmissionDescriptor(t *testing.T) {
	section := testBuildSDT(t, 0x1234, 0x5678, []testSDTService{
		{
			serviceID: 100,
			descriptors: append(
				testServiceDescriptor(1, nil, []byte{0x0e, 'A'}),
				ts.DescriptorTagLogoTransmission, 7, 0x01, 0xff, 0x2a, 0xf0, 0x03, 0x12, 0x34,
			),
		},
		{
			serviceID: 101,
			descriptors: append(
				testServiceDescriptor(1, nil, []byte{0x0e, 'B'}),
				ts.DescriptorTagLogoTransmission, 3, 0x02, 0xff, 0x2a,
			),
		},
	})
	scan := newServiceScan()
	scan.Observe(testBuildPAT(t, map[uint16]uint16{100: 0x0100, 101: 0x0101}))
	scan.Observe(section)
	got := scan.Services()
	if len(got) != 2 {
		t.Fatalf("services = %#v", got)
	}
	logo := got[1].Logo
	if logo == nil || logo.LogoID != 0x12a || logo.Version == nil || *logo.Version != 3 ||
		logo.DownloadDataID == nil || *logo.DownloadDataID != 0x1234 {
		t.Fatalf("indirect logo service = %#v", got[1])
	}
}
