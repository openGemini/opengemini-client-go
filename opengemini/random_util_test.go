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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandBytes(t *testing.T) {
	assert.Equal(t, 0, len(RandStr(-1)))
	assert.Equal(t, 0, len(RandStr(0)))
	assert.Equal(t, 1, len(RandStr(1)))
	assert.Equal(t, 8, len(RandStr(8)))
	assert.Equal(t, 32, len(RandStr(32)))
	assert.Equal(t, 32, len(RandBytes(32)))
	assert.Equal(t, 8, len(RandBytes(8)))
	assert.Equal(t, 0, len(RandBytes(0)))
	assert.Equal(t, 0, len(RandBytes(-1)))
}
