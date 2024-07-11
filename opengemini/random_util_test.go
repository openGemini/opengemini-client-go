package opengemini

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
