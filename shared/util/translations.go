package util

import "fmt"

const (
	rangePrefix = "block"
)

// RangeName generates a well-formed name for a blocks directory, given a height and a range
func RangeName(height uint64, directoryRange uint64) string {
	rangeSize := directoryRange
	bottom := (height / rangeSize) * rangeSize
	top := bottom + rangeSize - 1
	return fmt.Sprintf("%s%d-%d", rangePrefix, bottom, top)
}

func BlockNumberToHex(blockNumber uint64) string {
	return "0x" + fmt.Sprintf("%x", blockNumber)
}
