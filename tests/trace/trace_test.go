// Copyright 2025 openGemini Authors
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

package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/libgox/unicodex/letter"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTracerProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider() (*sdktrace.TracerProvider, error) {
	var ctx = context.Background()
	// export to ts-trace
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("127.0.0.1:18086"),
		otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes("", semconv.ServiceName("opengemini-client-go"))),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(time.Second)),
	)
	return tracerProvider, nil
}

func finish(t *testing.T) {
	watcher := testDefaultClient(t)
	err := watcher.DropMeasurement("jaeger_storage", "trace", "opengemini-client-go")
	assert.NoError(t, err)
}

func testDefaultClient(t *testing.T) opengemini.Client {
	// create an openGemini client
	config := &opengemini.Config{
		Addresses: []opengemini.Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
	}
	client, err := opengemini.NewClient(config)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return client
}

func TestOtelClient_WriteInterceptors(t *testing.T) {
	c := testDefaultClient(t)

	//Register the OtelCClient interceptor
	c.Interceptors(opengemini.NewOtelInterceptor())

	defer finish(t)

	databaseName := letter.RandEnglish(8)
	err := c.CreateDatabase(databaseName)
	assert.NoError(t, err)
	time.Sleep(time.Second * 3)
	point := &opengemini.Point{
		Measurement: "test_write",
		Precision:   opengemini.PrecisionNanosecond,
		Timestamp:   time.Now().UnixNano(),
		Tags: map[string]string{
			"foo": "bar",
		},
		Fields: map[string]interface{}{
			"v1": 1,
		},
	}
	err = c.WritePoint(databaseName, point, opengemini.CallbackDummy)
	require.Nil(t, err)

	watcher := testDefaultClient(t)
	time.Sleep(time.Second * 3)
	queryResult, err := watcher.Query(opengemini.Query{
		Database:        "jaeger_storage",
		Command:         "select * from \"opengemini-client-go\"",
		RetentionPolicy: "trace",
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if len(queryResult.Results) == 0 {
		return
	}
	result := queryResult.Results[0]
	if len(result.Series) == 0 {
		return
	}
	series := result.Series[0]
	// must be 2, create database and write point
	if len(series.Values) != 2 {
		panic("expected 2 values")
	}
}

func TestOtelClient_ShowTagKeys(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(opengemini.NewOtelInterceptor())
	defer finish(t)

	databaseName := letter.RandEnglish(8)
	err := c.CreateDatabase(databaseName)
	assert.NoError(t, err)
	point := &opengemini.Point{
		Measurement: "test_write",
		Precision:   opengemini.PrecisionNanosecond,
		Timestamp:   time.Now().UnixNano(),
		Tags: map[string]string{
			"foo": "bar",
		},
		Fields: map[string]interface{}{
			"v1": 1,
		},
	}
	err = c.WritePoint(databaseName, point, opengemini.CallbackDummy)
	require.Nil(t, err)
	measurement := letter.RandEnglish(8)
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(opengemini.Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	// SHOW TAG KEYS FROM measurement limit 3 OFFSET 0
	tagKeyResult, err := c.ShowTagKeys(opengemini.NewShowTagKeysBuilder().Database(databaseName).Measurement(measurement).Limit(3).Offset(0))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	assert.Equal(t, 3, len(tagKeyResult[measurement]))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)

	watcher := testDefaultClient(t)
	time.Sleep(time.Second * 3)
	queryResult, err := watcher.Query(opengemini.Query{
		Database:        "jaeger_storage",
		Command:         "select * from \"opengemini-client-go\"",
		RetentionPolicy: "trace",
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if len(queryResult.Results) == 0 {
		return
	}
	result := queryResult.Results[0]
	if len(result.Series) == 0 {
		return
	}
	series := result.Series[0]
	// must be 5, create database,write point,create measurement,show tag keys,drop database
	if len(series.Values) != 5 {
		panic("expected 5 values")
	}
}

func TestOtelCreateAndQueryMeasurement(t *testing.T) {
	c := testDefaultClient(t)
	//Register the OtelCClient interceptor
	c.Interceptors(opengemini.NewOtelInterceptor())
	defer finish(t)

	databaseName := letter.RandEnglish(8)
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)

	measurement := letter.RandEnglish(8)

	createCmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG, tag2 TAG, field1 INT64 FIELD)", measurement)
	createQuery := opengemini.Query{Command: createCmd, Database: databaseName}
	_, err = c.Query(createQuery)
	require.Nil(t, err)

	queryCmd := fmt.Sprintf("SELECT * FROM %s", measurement)
	queryQuery := opengemini.Query{Command: queryCmd, Database: databaseName}
	result, err := c.Query(queryQuery)
	require.Nil(t, err)
	assert.NotEmpty(t, result)

	watcher := testDefaultClient(t)
	time.Sleep(time.Second * 3)
	queryResult, err := watcher.Query(opengemini.Query{
		Database:        "jaeger_storage",
		Command:         "select * from \"opengemini-client-go\"",
		RetentionPolicy: "trace",
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	if len(queryResult.Results) == 0 {
		return
	}
	results := queryResult.Results[0]
	if len(results.Series) == 0 {
		return
	}
	series := results.Series[0]
	// must be 3, create database,create measurement,query measurement
	if len(series.Values) != 3 {
		panic("expected 3 values")
	}
}
