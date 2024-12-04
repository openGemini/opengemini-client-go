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

func (cv *ColVal) AppendStringNulls(count int) {
	for i := 0; i < count; i++ {
		cv.AppendStringNull()
	}
}

func (cv *ColVal) AppendStringNull() {
	cv.reserveOffset(1)
	cv.Offset[cv.Len] = uint32(len(cv.Val))
	cv.resetBitMap(cv.Len)
	cv.Len++
	cv.NilCount++
}

func (cv *ColVal) AppendStrings(values ...string) {
	for _, v := range values {
		cv.AppendString(v)
	}
}

func (cv *ColVal) AppendString(v string) {
	index := len(cv.Val)
	cv.reserveVal(len(v))
	copy(cv.Val[index:], v)
	cv.reserveOffset(1)
	cv.Offset[cv.Len] = uint32(index)
	cv.setBitMap(cv.Len)
	cv.Len++
}
