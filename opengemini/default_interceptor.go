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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	TraceName                   = "opengemini-client-go"
	SpanNameQuery               = "query"
	SpanNameWrite               = "write"
	AttributeDatabase           = "db"
	AttributeRetentionPolicy    = "rp"
	AttributeMeasurement        = "mst"
	AttributePrecision          = "precision"
	AttributeCommand            = "cmd"
	AttributeResponseStatusCode = "status-code"
	AttributeResponseBody       = "response-body"
	AttributeWriteLineProtocol  = "lp"
)

var (
	tracer = otel.Tracer(TraceName)
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
	//traceExporter, err := stdouttrace.New(
	//	stdouttrace.WithPrettyPrint())
	//if err != nil {
	//	return nil, err
	//}

	traceExporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("127.0.0.1:4318"),
		otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes("", semconv.ServiceName("opengemini-client-go"))),
		sdktrace.WithBatcher(traceExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			sdktrace.WithBatchTimeout(time.Second)),
	)
	return tracerProvider, nil
}

type OtelQuery struct {
	*Query
	Ctx     context.Context
	Span    trace.Span
	Carrier propagation.TextMapCarrier
}

type OtelWrite struct {
	Database        string
	RetentionPolicy string
	Measurement     string
	LineProtocol    string
	Precision       string
	Ctx             context.Context
	Span            trace.Span
	Carrier         propagation.TextMapCarrier
}

type OtelClient struct {
}

func NewOtelInterceptor() Interceptor {
	return &OtelClient{}
}

func getSpanFromCarrier(ctx context.Context, carrier propagation.TextMapCarrier) (context.Context, trace.Span) {
	propagator := otel.GetTextMapPropagator()
	ctx = propagator.Extract(ctx, carrier)
	span := trace.SpanFromContext(ctx)
	return ctx, span
}

func (o *OtelClient) QueryBefore(ctx context.Context, query *OtelQuery) {
	var span trace.Span
	if query.Carrier != nil {
		ctx, span = getSpanFromCarrier(ctx, query.Carrier)
	} else {
		ctx, span = tracer.Start(ctx, SpanNameQuery)
	}

	span.SetAttributes(attribute.String(AttributeDatabase, query.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, query.RetentionPolicy))
	span.SetAttributes(attribute.String(AttributePrecision, query.Precision.Epoch()))
	span.SetAttributes(attribute.String(AttributeCommand, query.Command))

	query.Ctx = ctx
	query.Span = span
}

func (o *OtelClient) QueryAfter(ctx context.Context, query *OtelQuery, response *http.Response) {
	if response == nil {
		if query.Span != nil {
			query.Span.End()
		}
		return
	}

	var span trace.Span
	if query.Span != nil {
		span = query.Span
	} else if query.Carrier != nil {
		ctx, span = getSpanFromCarrier(ctx, query.Carrier)
	}

	defer span.End()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("read query body failed:", err)
		return
	}

	response.Body = io.NopCloser(bytes.NewBuffer(body))

	span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
	span.SetAttributes(attribute.String(AttributeResponseBody, string(body)))
}

func (o *OtelClient) WriteBefore(ctx context.Context, write *OtelWrite) {
	var span trace.Span
	if write.Carrier != nil {
		ctx, span = getSpanFromCarrier(ctx, write.Carrier)
	} else {
		ctx, span = tracer.Start(ctx, SpanNameWrite)
	}

	span.SetAttributes(attribute.String(AttributeDatabase, write.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, write.RetentionPolicy))
	//span.SetAttributes(attribute.String(AttributeMeasurement, write.Measurement))
	span.SetAttributes(attribute.String(AttributePrecision, write.Precision))
	span.SetAttributes(attribute.String(AttributeWriteLineProtocol, write.LineProtocol))
	//span.SetAttributes(attribute.String("write.type", "batch"))
	write.Ctx = ctx
	write.Span = span
}

func (o *OtelClient) WriteAfter(ctx context.Context, write *OtelWrite, response *http.Response) {
	var span trace.Span
	if write.Span != nil {
		span = write.Span
	} else if write.Carrier != nil {
		ctx, span = getSpanFromCarrier(ctx, write.Carrier)
	}

	defer span.End()

	span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
}
