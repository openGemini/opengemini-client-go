// Copyright 2024 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

var (
	gzipReaderPool = NewCachePool(nil, 2*runtime.NumCPU())

	snappyReaderPool = NewCachePool(func() interface{} {
		return snappy.NewReader(bytes.NewReader(nil))
	}, 2*runtime.NumCPU())

	zstdDecoderPool = NewCachePool(func() interface{} {
		decoder, _ := zstd.NewReader(nil)
		return decoder
	}, 2*runtime.NumCPU())
)

func GetGzipReader(body []byte) (*gzip.Reader, error) {
	if gzipReaderPool.CurrentLength() == 0 {
		return gzip.NewReader(bytes.NewReader(body))
	}
	reader := gzipReaderPool.Get().(*gzip.Reader)
	if reader == nil {
		return nil, errors.New("failed to get gzip reader")
	}
	reader.Reset(bytes.NewReader(body))
	return reader, nil
}

func PutGzipReader(reader *gzip.Reader) {
	reader.Close()
	gzipReaderPool.Put(reader)
}

func GetSnappyReader(body []byte) (*snappy.Reader, error) {
	reader := snappyReaderPool.Get().(*snappy.Reader)
	if reader == nil {
		return nil, errors.New("failed to get snappy reader")
	}
	reader.Reset(bytes.NewReader(body))
	return reader, nil
}

func PutSnappyReader(reader *snappy.Reader) {
	snappyReaderPool.Put(reader)
}

func GetZstdDecoder(body []byte) (*zstd.Decoder, error) {
	decoder := zstdDecoderPool.Get().(*zstd.Decoder)
	if decoder == nil {
		return nil, errors.New("failed to get zstd decoder")
	}
	decoder.Reset(bytes.NewReader(body))
	return decoder, nil
}

func PutZstdDecoder(decoder *zstd.Decoder) {
	decoder.Reset(nil)
	zstdDecoderPool.Put(decoder)
}
