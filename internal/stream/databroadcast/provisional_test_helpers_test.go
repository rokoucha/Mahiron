package databroadcast

import "github.com/21S1298001/mahiron/ts"

// provisionalTestBuildPMT and provisionalTestBuildDII build minimal, valid
// section bytes for RestoreSnapshot tests. They intentionally duplicate the
// small builders in internal/stream/channel's tests rather than sharing them:
// each is a self-contained fixture tightly coupled to its own package's
// scenarios.

func provisionalTestBuildPMT(serviceID, carouselPID uint16, componentTag byte) []byte {
	esInfo := []byte{0x52, 0x01, componentTag}
	length := 9 + 5 + len(esInfo) + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDPMT
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5] = 0xc1
	s[8] = 0x1f
	s[9] = 0xff
	off := 12
	s[off] = ts.StreamTypeDSMCCDataCarousel
	s[off+1] = 0xe0 | byte(carouselPID>>8)
	s[off+2] = byte(carouselPID)
	s[off+3] = 0xf0 | byte(len(esInfo)>>8)
	s[off+4] = byte(len(esInfo))
	copy(s[off+5:], esInfo)
	provisionalTestWriteCRC(s)
	return s
}

func provisionalTestBuildDII(downloadID uint32, blockSize, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte) []byte {
	body := []byte{
		byte(downloadID >> 24), byte(downloadID >> 16), byte(downloadID >> 8), byte(downloadID),
		byte(blockSize >> 8), byte(blockSize),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
		0, 1,
		byte(moduleID >> 8), byte(moduleID),
		byte(moduleSize >> 24), byte(moduleSize >> 16), byte(moduleSize >> 8), byte(moduleSize),
		moduleVersion,
		byte(len(moduleInfo)),
	}
	body = append(body, moduleInfo...)
	return provisionalTestBuildDSMCCSection(ts.TableIDDSMCCDII, 0x1002, 1, body)
}

func provisionalTestBuildDSMCCSection(tableID byte, messageID uint16, headerID uint32, body []byte) []byte {
	message := []byte{0x11, 0x03, byte(messageID >> 8), byte(messageID), byte(headerID >> 24), byte(headerID >> 16), byte(headerID >> 8), byte(headerID), 0xff, 0}
	message = append(message, byte(len(body)>>8), byte(len(body)))
	message = append(message, body...)
	length := 5 + len(message) + 4
	s := make([]byte, 3+length)
	s[0] = tableID
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = 0
	s[4] = 1
	s[5] = 0xc1
	copy(s[8:], message)
	provisionalTestWriteCRC(s)
	return s
}

func provisionalTestWriteCRC(s []byte) {
	crc := provisionalTestCRC32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func provisionalTestCRC32MPEG2(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
