package opengemini

import (
	"crypto/rand"
	"math/big"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandBytes(n int64) []byte {
	if n <= 0 {
		return []byte{}
	}
	b := make([]byte, n)
	for i := range b {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			panic(err)
		}
		b[i] = letters[index.Int64()]
	}
	return b
}

func RandStr(n int64) string {
	if n <= 0 {
		return ""
	}
	return string(RandBytes(n))
}
