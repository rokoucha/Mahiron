package databroadcast

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/21S1298001/mahiron/ts"
)

func TestDecodeModuleResourcesMultipart(t *testing.T) {
	module := DataBroadcastModule{Data: []byte("Content-Type: multipart/mixed; boundary=part\r\n\r\n--part\r\nContent-Location: startup.bml\r\nContent-Type: text/bml; charset=utf-8\r\n\r\n<body/>\r\n--part\r\nContent-Location: image.png\r\nContent-Type: image/png\r\n\r\nPNG\r\n--part--\r\n")}
	resources, err := DecodeModuleResources(module)
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 2 || resources[0].ContentLocation == nil || *resources[0].ContentLocation != "startup.bml" || resources[0].ContentType != "text/bml" || string(resources[1].Data) != "PNG" {
		t.Fatalf("resources = %#v", resources)
	}
}

func TestDecodeModuleResourcesZlib(t *testing.T) {
	raw := []byte("Content-Type: text/bml\r\nContent-Location: startup.bml\r\n\r\n<body/>")
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	_, _ = w.Write(raw)
	_ = w.Close()
	info := []byte{ts.DSMCCModuleDescriptorCompressionType, 5, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(info[3:], uint32(len(raw)))
	resources, err := DecodeModuleResources(DataBroadcastModule{Info: info, Data: compressed.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 1 || string(resources[0].Data) != "<body/>" {
		t.Fatalf("resources = %#v", resources)
	}
}

func TestDecodeModuleResourcesDirectMapping(t *testing.T) {
	info := append([]byte{ts.DSMCCModuleDescriptorType, byte(len("text/bml"))}, "text/bml"...)
	info = append(info, ts.DSMCCModuleDescriptorName, byte(len("startup.bml")))
	info = append(info, "startup.bml"...)
	resources, err := DecodeModuleResources(DataBroadcastModule{Info: info, Data: []byte("<body/>")})
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 1 || resources[0].ContentLocation != nil || resources[0].ContentType != "text/bml" || string(resources[0].Data) != "<body/>" {
		t.Fatalf("resources = %#v", resources)
	}
}

func TestDecodeModuleResourcesSingleEntityIsModuleScoped(t *testing.T) {
	resources, err := DecodeModuleResources(DataBroadcastModule{Data: []byte("Content-Type: text/bml\r\nContent-Location: startup.bml\r\n\r\n<body/>")})
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 1 || resources[0].ContentLocation != nil || resources[0].ContentType != "text/bml" {
		t.Fatalf("resources = %#v", resources)
	}
}

func TestDecodeModuleResourcesSkipsIncompleteMultipartPart(t *testing.T) {
	data := []byte("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nignored\r\n--x\r\nContent-Location: startup.bml\r\nContent-Type: text/bml\r\n\r\n<body/>\r\n--x--\r\n")
	resources, err := DecodeModuleResources(DataBroadcastModule{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 1 || resources[0].ContentLocation == nil || *resources[0].ContentLocation != "startup.bml" {
		t.Fatalf("resources = %#v", resources)
	}
}

func TestDecodeModuleResourcesRejectsOversizedUncompressedEntity(t *testing.T) {
	data := append([]byte("Content-Type: text/bml\r\n\r\n"), bytes.Repeat([]byte{'x'}, maxDecodedModuleBytes+1)...)
	_, err := DecodeModuleResources(DataBroadcastModule{Data: data})
	if !errors.Is(err, ErrModuleResourceLimit) {
		t.Fatalf("error = %v, want resource limit", err)
	}
}

func TestDecodeModuleResourcesRejectsTooManyParts(t *testing.T) {
	var data bytes.Buffer
	data.WriteString("Content-Type: multipart/mixed; boundary=x\r\n\r\n")
	for range maxModuleResources + 1 {
		data.WriteString("--x\r\nContent-Location: item\r\nContent-Type: text/plain\r\n\r\n\r\n")
	}
	data.WriteString("--x--\r\n")
	_, err := DecodeModuleResources(DataBroadcastModule{Data: data.Bytes()})
	if !errors.Is(err, ErrModuleResourceLimit) {
		t.Fatalf("error = %v, want resource limit", err)
	}
}
