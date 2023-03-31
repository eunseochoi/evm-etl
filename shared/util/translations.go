package util

import "fmt"

const (
	rangePrefix = "block"
)

// RangeName generates a well-formed name for a blocks directory, given a height and a range
func RangeName(height int64, directoryRange int) string {
	rangeSize := int64(directoryRange)
	bottom := (height / rangeSize) * rangeSize
	top := bottom + rangeSize - 1
	return fmt.Sprintf("%s%d-%d", rangePrefix, bottom, top)
}
