package main

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Uint16Slice []uint16

func (p Uint16Slice) Len() int           { return len(p) }
func (p Uint16Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint16Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Sort(slice []uint16) {
}

func TestParseRange(t *testing.T) {
	assert := assert.New(t)

	partitions := parseRange("1,2, 3,6-12, -1,17-2, 5 - 11")
	expected := []uint16{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12}
	sort.Sort(Uint16Slice(partitions))
	assert.EqualValues(expected, partitions)

	partitions = parseRange("5 - 11")
	expected = []uint16{5, 6, 7, 8, 9, 10, 11}
	sort.Sort(Uint16Slice(partitions))
	assert.EqualValues(expected, partitions)

	partitions = parseRange("11 - 5")
	expected = make([]uint16, 1024)
	for i := 0; i < 1024; i++ {
		expected[i] = uint16(i)
	}
	assert.EqualValues(expected, partitions)

}
