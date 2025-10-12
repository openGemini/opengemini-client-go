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
	"io"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

func TestGzipReaderPool(t *testing.T) {
	data := []byte("test data")
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	if err != nil {
		t.Fatalf("failed to write gzip data: %v", err)
	}
	writer.Close()

	compressedData := buf.Bytes()

	reader, err := GetGzipReader(compressedData)
	if err != nil {
		t.Fatalf("failed to get gzip reader: %v", err)
	}

	decompressedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read gzip data: %v", err)
	}

	if !bytes.Equal(decompressedData, data) {
		t.Errorf("expected %v, got %v", data, decompressedData)
	}

	PutGzipReader(reader)
}

func TestSnappyReaderPool(t *testing.T) {
	data := []byte("test data")
	var buf bytes.Buffer

	// Write data to buffer
	writer := snappy.NewBufferedWriter(&buf)
	_, err := writer.Write(data)
	if err != nil {
		t.Fatalf("failed to write snappy data: %v", err)
	}
	writer.Close()

	compressedData := buf.Bytes()

	reader, err := GetSnappyReader(compressedData)
	if err != nil {
		t.Fatalf("failed to get snappy reader: %v", err)
	}

	decompressedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read snappy data: %v", err)
	}

	if !bytes.Equal(decompressedData, data) {
		t.Errorf("expected %v, got %v", data, decompressedData)
	}

	PutSnappyReader(reader)

}

func TestZstdDecoderPool(t *testing.T) {
	data := []byte("test data")
	encoder, _ := zstd.NewWriter(nil)
	compressedData := encoder.EncodeAll(data, nil)
	encoder.Close()

	decoder, err := GetZstdDecoder(compressedData)
	if err != nil {
		t.Fatalf("failed to get zstd decoder: %v", err)
	}

	decompressedData, err := decoder.DecodeAll(compressedData, nil)
	if err != nil {
		t.Fatalf("failed to read zstd data: %v", err)
	}

	if !bytes.Equal(decompressedData, data) {
		t.Errorf("expected %v, got %v", data, decompressedData)
	}

	PutZstdDecoder(decoder)
}
