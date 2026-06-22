package aribstr

// aribRowCellKey packs transmitted row/cell bytes, i.e. row/cell + 0x20.
func aribRowCellKey(first, second byte) uint16 {
	return uint16(first)<<8 | uint16(second)
}

// aribPlaneRowCellKey packs a JIS X 0213 plane with transmitted row/cell bytes.
func aribPlaneRowCellKey(plane, first, second byte) uint32 {
	return uint32(plane)<<16 | uint32(first)<<8 | uint32(second)
}

func aribValidRowCell(first, second byte) bool {
	return first >= 0x21 && first <= 0x7e && second >= 0x21 && second <= 0x7e
}
