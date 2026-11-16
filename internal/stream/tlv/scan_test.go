package tlv

import (
	"testing"

	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestScanLogoReferences(t *testing.T) {
	for _, tc := range []struct {
		name    string
		replace bool
		logo    []byte
	}{
		{name: "indirect"},
		{name: "replaced without descriptor", replace: true},
		{name: "replaced with unknown type", replace: true, logo: []byte{0xff}},
		{name: "replaced with simple logo", replace: true, logo: []byte{mmt.LogoTransmissionTypeSimple, 'A'}},
	} {
		name, replace := tc.name, tc.replace
		t.Run(name, func(t *testing.T) {
			service := func(id byte, logo []byte) []byte {
				descriptors := mmttest.Descriptor(mmt.DescriptorTagMHService, []byte{1, 0, 0})
				if logo != nil {
					descriptors = append(descriptors, mmttest.Descriptor(mmt.DescriptorTagMHLogoTransmission, logo)...)
				}
				return append([]byte{0, id, 0xe3, 0x80 | byte(len(descriptors)>>8), byte(len(descriptors))}, descriptors...)
			}
			body := []byte{0, 0x0b, 0xff}
			body = append(body, service(1, []byte{mmt.LogoTransmissionTypeCDT1, 0, 0, 0, 2, 0, 9, mmt.LogoType2K, 0, 1})...)
			body = append(body, service(2, []byte{mmt.LogoTransmissionTypeCDT2, 0, 0})...)
			if replace {
				body = append(body, service(2, tc.logo)...)
			}
			scan := newServiceScan()
			scan.Observe(signal{Section: mmttest.Section(mmt.TableIDMHSDTActual, 0xB110, 0, 0, 0, body)})
			got := scan.Services()
			if len(got) != 2 {
				t.Fatalf("services = %#v", got)
			}
			logo := got[1].Logo
			if replace {
				if tc.logo == nil && logo != nil {
					t.Fatalf("replacement logo = %+v", logo)
				}
				if tc.logo != nil && (logo == nil || logo.Version != nil || logo.DownloadDataID != nil) {
					t.Fatalf("replacement metadata = %+v", logo)
				}
				return
			}
			if logo == nil || logo.LogoID != 0 || logo.Version == nil || *logo.Version != 2 || logo.DownloadDataID == nil || *logo.DownloadDataID != 9 {
				t.Fatalf("indirect logo = %+v", logo)
			}
		})
	}
}
