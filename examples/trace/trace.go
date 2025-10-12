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
	"time"

	"github.com/libgox/unicodex/letter"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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

	// for example: export to jaeger
	jaegerExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint("127.0.0.1:4318"),
		otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	// for example: export to ts-trace
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("127.0.0.1:18086"),
		otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes("", semconv.ServiceName("opengemini-client-go"))),
		sdktrace.WithBatcher(jaegerExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(time.Second)),
	)
	return tracerProvider, nil
}

func main() {
	var ctx = context.Background()
	shutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	//Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, shutdown(ctx))
	}()

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
		return
	}

	// set otel interceptor
	client.Interceptors(opengemini.NewOtelInterceptor())

	databaseName := letter.RandEnglish(8)
	err = client.CreateDatabase(databaseName)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
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
	err = client.WritePoint(databaseName, point, opengemini.CallbackDummy)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Println("write point success")
}
