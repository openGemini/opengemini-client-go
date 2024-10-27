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

package opengemini

import (
	"crypto/rand"
	"math/big"

	"github.com/libgox/unicodex/letter"
)

func RandBytes(n int64) []byte {
	if n <= 0 {
		return []byte{}
	}
	b := make([]byte, n)
	for i := range b {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(letter.EnglishCount)))
		if err != nil {
			panic(err)
		}
		b[i] = letter.EnglishLetters[index.Int64()]
	}
	return b
}

func RandStr(n int64) string {
	if n <= 0 {
		return ""
	}
	return string(RandBytes(n))
}
