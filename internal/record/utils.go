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

const (
	BooleanSizeBytes = int(unsafe.Sizeof(false))
	Uint32SizeBytes  = int(unsafe.Sizeof(uint32(0)))
	Int64SizeBytes   = int(unsafe.Sizeof(int64(0)))
	Float64SizeBytes = int(unsafe.Sizeof(float64(0)))

	sizeOfInt    = int(unsafe.Sizeof(int(0)))
	sizeOfUint16 = 2
	sizeOfUint32 = 4
	MaxSliceSize = sizeOfUint32
)

var (
	typeSize = make([]int, FieldTypeLast)
	zeroBuf  = make([]byte, 1024)
)

func init() {
	typeSize[FieldTypeInt] = Int64SizeBytes
	typeSize[FieldTypeFloat] = Float64SizeBytes
	typeSize[FieldTypeBoolean] = BooleanSizeBytes
}

type ExceptString interface {
	int64 | float64 | bool
}

func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func AppendString(b []byte, s string) []byte {
	b = AppendUint16(b, uint16(len(s)))
	b = append(b, s...)
	return b
}

// AppendUint16 appends marshaled v to dst and returns the result.
func AppendUint16(dst []byte, u uint16) []byte {
	return append(dst, byte(u>>8), byte(u))
}

// AppendUint32 appends marshaled v to dst and returns the result.
func AppendUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// AppendInt64 appends marshaled v to dst and returns the result.
func AppendInt64(dst []byte, v int64) []byte {
	// Such encoding for negative v must improve compression.
	v = (v << 1) ^ (v >> 63) // zig-zag encoding without branching.
	u := uint64(v)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func AppendInt(b []byte, i int) []byte {
	return AppendInt64(b, int64(i))
}

func AppendBytes(b []byte, buf []byte) []byte {
	b = AppendUint32(b, uint32(len(buf)))
	b = append(b, buf...)
	return b
}

func AppendUint32Slice(b []byte, a []uint32) []byte {
	b = AppendUint32(b, uint32(len(a)))
	if len(a) == 0 {
		return b
	}

	b = append(b, Uint32Slice2byte(a)...)
	return b
}

func SizeOfString(s string) int {
	return len(s) + sizeOfUint16
}

func SizeOfUint32() int {
	return sizeOfUint32
}

func SizeOfInt() int {
	return sizeOfInt
}

func SizeOfUint32Slice(s []uint32) int {
	return len(s)*SizeOfUint32() + MaxSliceSize
}

func SizeOfByteSlice(s []byte) int {
	return len(s) + SizeOfUint32()
}

func Uint32Slice2byte(u []uint32) []byte {
	if len(u) == 0 {
		return nil
	}
	// Get pointer to the first element of uint32 slice
	ptr := unsafe.Pointer(unsafe.SliceData(u))
	// Create a new byte slice from the pointer
	return unsafe.Slice((*byte)(ptr), len(u)*Uint32SizeBytes)
}
