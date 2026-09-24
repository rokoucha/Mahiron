package isdb

// This file holds the ARIB STD-B10 component tables that ISDB-T/S EIT
// carries and that the Mirakurun-compatible API exposes as raw values
// (streamContent, componentType). Meanings are plain strings so that the
// table stays free of the internal model.

// ComponentVideo describes a TS video component_descriptor's component_type
// as meaning values: resolution and aspect ratio. The high nibble selects
// the resolution family and the low nibble (1-4) selects the aspect variant
// (4:3, 16:9 with pan-vector, 16:9 without pan-vector, 16:9 over-scan).
// Values outside the defined table report ok=false and are carried as
// unknown, never coerced.
type ComponentVideo struct {
	Resolution string
	Aspect     string
}

const (
	VideoAspect43           = "4:3"
	VideoAspect169PanVector = "16:9-pan-vector"
	VideoAspect169NoPanVec  = "16:9-no-pan-vector"
	VideoAspect169Over      = "16:9-over"
)

// ParseVideoComponentType splits a video component_type into resolution and
// aspect meanings.
func ParseVideoComponentType(componentType byte) (ComponentVideo, bool) {
	var resolution string
	switch {
	case componentType >= 0x01 && componentType <= 0x04:
		resolution = "480i"
	case componentType == 0x83:
		resolution = "4320p"
	case componentType >= 0x91 && componentType <= 0x94:
		resolution = "2160p"
	case componentType >= 0xA1 && componentType <= 0xA4:
		resolution = "480p"
	case componentType >= 0xB1 && componentType <= 0xB4:
		resolution = "1080i"
	case componentType >= 0xC1 && componentType <= 0xC4:
		resolution = "720p"
	case componentType >= 0xD1 && componentType <= 0xD4:
		resolution = "240p"
	case componentType >= 0xE1 && componentType <= 0xE4:
		resolution = "1080p"
	default:
		return ComponentVideo{}, false
	}
	var aspect string
	switch componentType & 0x0F {
	case 0x01:
		aspect = VideoAspect43
	case 0x02:
		aspect = VideoAspect169PanVector
	case 0x03:
		aspect = VideoAspect169NoPanVec
	case 0x04:
		aspect = VideoAspect169Over
	default:
		return ComponentVideo{}, false
	}
	return ComponentVideo{Resolution: resolution, Aspect: aspect}, true
}

// VideoComponentTypeToRaw recomposes a component_type from meaning values.
// It is the inverse of ParseVideoComponentType and returns ok=false for
// combinations the table does not define.
func VideoComponentTypeToRaw(resolution, aspect string) (byte, bool) {
	var high byte
	switch resolution {
	case "480i":
		high = 0x00
	case "4320p":
		if aspect == VideoAspect169NoPanVec {
			return 0x83, true
		}
		return 0, false
	case "2160p":
		high = 0x90
	case "480p":
		high = 0xA0
	case "1080i":
		high = 0xB0
	case "720p":
		high = 0xC0
	case "240p":
		high = 0xD0
	case "1080p":
		high = 0xE0
	default:
		return 0, false
	}
	var low byte
	switch aspect {
	case VideoAspect43:
		low = 0x01
	case VideoAspect169PanVector:
		low = 0x02
	case VideoAspect169NoPanVec:
		low = 0x03
	case VideoAspect169Over:
		low = 0x04
	default:
		return 0, false
	}
	return high | low, true
}

// AudioCodecForTSStreamContent maps a TS audio stream_content to its
// meaning-level codec. MMT uses different values (AAC is 0x3, MPEG-4 ALS is
// 0x4), so the systems never share the raw value.
func AudioCodecForTSStreamContent(streamContent byte) (string, bool) {
	switch streamContent {
	case 0x2:
		return "aac", true
	default:
		return "", false
	}
}

// VideoCodecForTSStreamContent maps a TS video stream_content to its codec:
// "mpeg2", "h264" or "h265".
func VideoCodecForTSStreamContent(streamContent byte) (string, bool) {
	switch streamContent {
	case 0x01:
		return "mpeg2", true
	case 0x05:
		return "h264", true
	case 0x09:
		return "h265", true
	default:
		return "", false
	}
}

// TSStreamContentForVideoCodec is the inverse of
// VideoCodecForTSStreamContent.
func TSStreamContentForVideoCodec(codec string) (byte, bool) {
	switch codec {
	case "mpeg2":
		return 0x01, true
	case "h264":
		return 0x05, true
	case "h265":
		return 0x09, true
	default:
		return 0, false
	}
}
