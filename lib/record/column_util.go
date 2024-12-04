// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

import (
	"unsafe"
)

var (
	BitMask        = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
	FlippedBitMask = [8]byte{254, 253, 251, 247, 239, 223, 191, 127}
)

type ColVal struct {
	Val          []byte
	Offset       []uint32
	Bitmap       []byte
	BitMapOffset int
	Len          int
	NilCount     int
}

func (cv *ColVal) Init() {
	cv.Val = cv.Val[:0]
	cv.Offset = cv.Offset[:0]
	cv.Bitmap = cv.Bitmap[:0]
	cv.Len = 0
	cv.NilCount = 0
	cv.BitMapOffset = 0
}

func (cv *ColVal) reserveOffset(size int) {
	offsetCap := cap(cv.Offset)
	offsetLen := len(cv.Offset)
	remain := offsetCap - offsetLen
	if delta := size - remain; delta > 0 {
		cv.Offset = append(cv.Offset[:offsetCap], make([]uint32, delta)...)
	}
	cv.Offset = cv.Offset[:offsetLen+size]
}

func (cv *ColVal) resetBitMap(index int) {
	if (cv.Len+cv.BitMapOffset)>>3 >= len(cv.Bitmap) {
		cv.Bitmap = append(cv.Bitmap, 0)
		return
	}

	index += cv.BitMapOffset
	cv.Bitmap[index>>3] &= FlippedBitMask[index&0x07]
}

func appendNulls(cv *ColVal, count int) {
	for i := 0; i < count; i++ {
		appendNull(cv)
	}
}

func appendNull(cv *ColVal) {
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) reserveVal(size int) {
	cv.Val = reserveBytes(cv.Val, size)
}

func reserveBytes(b []byte, size int) []byte {
	valCap := cap(b)
	if valCap == 0 {
		return make([]byte, size)
	}

	valLen := len(b)
	remain := valCap - valLen
	if delta := size - remain; delta > 0 {
		if delta <= len(zeroBuf) {
			b = append(b[:valCap], zeroBuf[:delta]...)
		} else {
			b = append(b[:valCap], make([]byte, delta)...)
		}
	}
	return b[:valLen+size]
}

func (cv *ColVal) setBitMap(index int) {
	if (cv.Len+cv.BitMapOffset)>>3 >= len(cv.Bitmap) {
		cv.Bitmap = append(cv.Bitmap, 1)
		return
	}

	index += cv.BitMapOffset
	cv.Bitmap[index>>3] |= BitMask[index&0x07]
}

func appendValues[T ExceptString](cv *ColVal, values ...T) {
	for _, v := range values {
		appendValue(cv, v)
	}
}

func appendValue[T ExceptString](cv *ColVal, v T) {
	index := len(cv.Val)
	cv.reserveVal(int(unsafe.Sizeof(v)))
	*(*T)(unsafe.Pointer(&cv.Val[index])) = v
	cv.setBitMap(cv.Len)
	cv.Len++
}

func values[T ExceptString](cv *ColVal) []T {
	valueLen := int(unsafe.Sizeof(*new(T)))
	if cv.Val == nil {
		return nil
	}
	data := unsafe.Slice((*T)(unsafe.Pointer(&cv.Val[0])), len(cv.Val)/valueLen)
	return data
}

func (cv *ColVal) FloatValues() []float64 {
	return values[float64](cv)
}

func (cv *ColVal) IsNil(i int) bool {
	if i >= cv.Len || len(cv.Bitmap) == 0 {
		return true
	}
	if cv.NilCount == 0 {
		return false
	}
	idx := cv.BitMapOffset + i
	return !((cv.Bitmap[idx>>3] & BitMask[idx&0x07]) != 0)
}

func (cv *ColVal) StringValues(dst []string) []string {
	if len(cv.Offset) == 0 {
		return dst
	}

	offs := cv.Offset
	for i := 0; i < len(offs); i++ {
		if cv.IsNil(i) {
			continue
		}
		off := offs[i]
		if i == len(offs)-1 {
			dst = append(dst, Bytes2str(cv.Val[off:]))
		} else {
			dst = append(dst, Bytes2str(cv.Val[off:offs[i+1]]))
		}
	}

	return dst
}

func (cv *ColVal) BooleanValues() []bool {
	return values[bool](cv)
}

func (cv *ColVal) IntegerValues() []int64 {
	return values[int64](cv)
}

func (cv *ColVal) appendAll(src *ColVal) {
	cv.Val = append(cv.Val, src.Val...)
	cv.Offset = append(cv.Offset, src.Offset...)
	bitmap, bitMapOffset := subBitmapBytes(src.Bitmap, src.BitMapOffset, src.Len)
	cv.Bitmap = append(cv.Bitmap, bitmap...)
	cv.BitMapOffset = bitMapOffset
	cv.Len = src.Len
	cv.NilCount = src.NilCount
}

func subBitmapBytes(bitmap []byte, bitMapOffset int, length int) ([]byte, int) {
	if ((bitMapOffset + length) & 0x7) != 0 {
		return bitmap[bitMapOffset>>3 : ((bitMapOffset+length)>>3 + 1)], bitMapOffset & 0x7
	}

	return bitmap[bitMapOffset>>3 : (bitMapOffset+length)>>3], bitMapOffset & 0x7
}

func (cv *ColVal) appendString(src *ColVal, start, end int) {
	offset := uint32(len(cv.Val))
	for i := start; i < end; i++ {
		if i != start {
			offset += src.Offset[i] - src.Offset[i-1]
		}
		cv.Offset = append(cv.Offset, offset)
	}

	if end == src.Len {
		cv.Val = append(cv.Val, src.Val[src.Offset[start]:]...)
	} else {
		cv.Val = append(cv.Val, src.Val[src.Offset[start]:src.Offset[end]]...)
	}
}

func (cv *ColVal) Size() int {
	size := 0
	size += SizeOfInt()                  // Len
	size += SizeOfInt()                  // NilCount
	size += SizeOfInt()                  // BitMapOffset
	size += SizeOfByteSlice(cv.Val)      // Val
	size += SizeOfByteSlice(cv.Bitmap)   // Bitmap
	size += SizeOfUint32Slice(cv.Offset) // Offset
	return size
}

func (cv *ColVal) Marshal(buf []byte) ([]byte, error) {
	buf = AppendInt(buf, cv.Len)
	buf = AppendInt(buf, cv.NilCount)
	buf = AppendInt(buf, cv.BitMapOffset)
	buf = AppendBytes(buf, cv.Val)
	buf = AppendBytes(buf, cv.Bitmap)
	buf = AppendUint32Slice(buf, cv.Offset)
	return buf, nil
}
