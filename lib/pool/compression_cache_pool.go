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

package pool

import (
	"bytes"
	"compress/gzip"
	"errors"
	"runtime"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var (
	gzipReaderPool = NewCachePool[*gzip.Reader](func() *gzip.Reader {
		return new(gzip.Reader)
	}, 2*runtime.NumCPU())

	snappyReaderPool = NewCachePool[*snappy.Reader](func() *snappy.Reader {
		return snappy.NewReader(nil)
	}, 2*runtime.NumCPU())

	zstdDecoderPool = NewCachePool[*zstd.Decoder](func() *zstd.Decoder {
		decoder, error := zstd.NewReader(nil)
		if error != nil {
			return nil
		}
		return decoder
	}, 2*runtime.NumCPU())
)

func GetGzipReader(body []byte) (*gzip.Reader, error) {
	gzipReader := gzipReaderPool.Get()
	if gzipReader == nil {
		return nil, errors.New("failed to get gzip reader")
	}
	err := gzipReader.Reset(bytes.NewReader(body))
	if err != nil {
		gzipReaderPool.Put(gzipReader) // Return the reader to the pool if reset fails
		return nil, err
	}
	return gzipReader, nil
}

func PutGzipReader(reader *gzip.Reader) {
	gzipReaderPool.Put(reader)
}

func GetSnappyReader(body []byte) (*snappy.Reader, error) {
	snappyReader := snappyReaderPool.Get()
	if snappyReader == nil {
		return nil, errors.New("failed to get snappy reader")
	}

	snappyReader.Reset(bytes.NewReader(body))
	return snappyReader, nil
}

func PutSnappyReader(reader *snappy.Reader) {
	reader.Reset(nil)
	snappyReaderPool.Put(reader)
}

func GetZstdDecoder(body []byte) (*zstd.Decoder, error) {
	decoder := zstdDecoderPool.Get()
	if decoder == nil {
		return nil, errors.New("failed to get zstd decoder")
	}

	err := decoder.Reset(bytes.NewReader(body))
	if err != nil {
		zstdDecoderPool.Put(decoder) // Return the decoder to the pool if reset fails
		return nil, err
	}
	return decoder, nil
}

func PutZstdDecoder(decoder *zstd.Decoder) {
	err := decoder.Reset(nil)
	if err != nil {
		return
	}
	zstdDecoderPool.Put(decoder)
}
