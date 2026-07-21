package databroadcast

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/21S1298001/mahiron/ts"
)

const (
	maxDecodedModuleBytes = 8 * 1024 * 1024
	maxModuleResources    = 256
)

var (
	ErrUnsupportedModuleCompression = errors.New("data broadcast: unsupported module compression")
	ErrMalformedModule              = errors.New("data broadcast: malformed module entity")
	ErrModuleResourceLimit          = errors.New("data broadcast: module resource limit exceeded")
)

// ModuleResource is one logical BML resource decoded from a completed DSM-CC
// module. ID is stable for the immutable module identity, not its name.
type ModuleResource struct {
	ID              string
	ContentLocation *string
	ContentType     string
	Data            []byte
}

// DecodeModuleResources expands the ARIB module entity. Compression type 0 is
// zlib as specified by ARIB TR-B14 and web-bml's CompressionType.Zlib. Other advertised formats are
// rejected rather than serving compressed bytes as BML content.
func DecodeModuleResources(module DataBroadcastModule) ([]ModuleResource, error) {
	data := module.Data
	metadata := module.Metadata
	if metadata == nil {
		if parsed, ok := (ts.DSMCCModuleInfo{Info: module.Info}).Metadata(); ok {
			metadata = &parsed
		}
	}
	if metadata != nil && metadata.CompressionType != nil {
		if *metadata.CompressionType != 0 {
			return nil, fmt.Errorf("%w: %d", ErrUnsupportedModuleCompression, *metadata.CompressionType)
		}
		var err error
		data, err = inflateModule(data, metadata.OriginalSize)
		if err != nil {
			return nil, err
		}
	}
	return parseModuleEntity(data, metadata)
}

func inflateModule(data []byte, originalSize *uint32) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid zlib stream", ErrMalformedModule)
	}
	defer r.Close()
	limit := int64(maxDecodedModuleBytes) + 1
	if originalSize != nil {
		if *originalSize > maxDecodedModuleBytes {
			return nil, fmt.Errorf("%w: decoded size exceeds limit", ErrModuleResourceLimit)
		}
		limit = int64(*originalSize) + 1
	}
	decoded, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid decoded size", ErrMalformedModule)
	}
	if len(decoded) == int(limit) {
		return nil, fmt.Errorf("%w: decoded size exceeds limit", ErrModuleResourceLimit)
	}
	if originalSize != nil && len(decoded) != int(*originalSize) {
		return nil, fmt.Errorf("%w: decoded size mismatch", ErrMalformedModule)
	}
	return decoded, nil
}

func parseModuleEntity(data []byte, metadata *ts.DSMCCModuleMetadata) ([]ModuleResource, error) {
	// A Type descriptor may map one resource directly to a module. Only
	// multipart modules use a MIME entity wrapper around individual resources.
	if metadata != nil && metadata.Type != "" {
		mediaType, _, err := mime.ParseMediaType(metadata.Type)
		if err != nil || mediaType == "" {
			return nil, fmt.Errorf("%w: invalid module Type descriptor", ErrMalformedModule)
		}
		if !strings.EqualFold(mediaType, "multipart/mixed") {
			if len(data) > maxDecodedModuleBytes {
				return nil, fmt.Errorf("%w: decoded size exceeds limit", ErrModuleResourceLimit)
			}
			return []ModuleResource{{ID: "0", ContentType: mediaType, Data: append([]byte(nil), data...)}}, nil
		}
	}
	headEnd := bytes.Index(data, []byte("\r\n\r\n"))
	if headEnd < 0 {
		return nil, ErrMalformedModule
	}
	headers, err := textproto.NewReader(bufioReader(data[:headEnd+4])).ReadMIMEHeader()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedModule, err)
	}
	body := data[headEnd+4:]
	contentType := headers.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Content-Type", ErrMalformedModule)
	}
	if !strings.EqualFold(mediaType, "multipart/mixed") {
		if len(body) > maxDecodedModuleBytes {
			return nil, fmt.Errorf("%w: decoded size exceeds limit", ErrModuleResourceLimit)
		}
		return []ModuleResource{{ID: "0", ContentType: mediaType, Data: append([]byte(nil), body...)}}, nil
	}
	boundary := params["boundary"]
	if boundary == "" {
		return nil, fmt.Errorf("%w: missing multipart boundary", ErrMalformedModule)
	}
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	resources := []ModuleResource{}
	totalBytes := 0
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformedModule, err)
		}
		if len(resources) == maxModuleResources {
			return nil, fmt.Errorf("%w: too many multipart parts", ErrModuleResourceLimit)
		}
		partType, _, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
		if err != nil || partType == "" || part.Header.Get("Content-Location") == "" {
			// web-bml skips malformed individual parts; retain the usable
			// resources instead of rejecting the whole broadcast module.
			continue
		}
		partData, err := io.ReadAll(io.LimitReader(part, maxDecodedModuleBytes+1))
		if err != nil {
			return nil, fmt.Errorf("%w: incomplete multipart part", ErrMalformedModule)
		}
		if len(partData) > maxDecodedModuleBytes || totalBytes > maxDecodedModuleBytes-len(partData) {
			return nil, fmt.Errorf("%w: decoded size exceeds limit", ErrModuleResourceLimit)
		}
		totalBytes += len(partData)
		contentLocation := part.Header.Get("Content-Location")
		resources = append(resources, ModuleResource{ID: strconv.Itoa(len(resources)), ContentLocation: &contentLocation, ContentType: partType, Data: partData})
	}
	if len(resources) == 0 {
		return nil, fmt.Errorf("%w: empty multipart module", ErrMalformedModule)
	}
	return resources, nil
}

func bufioReader(data []byte) *bufio.Reader { return bufio.NewReader(bytes.NewReader(data)) }
