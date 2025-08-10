package opengemini

import (
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
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

const (
	TraceName                   = "opengemini-client-go"
	SpanNameQueryBefore         = "query-before"
	SpanNameQueryAfter          = "query-after"
	SpanNameWriteBefore         = "write-before"
	SpanNameWriteAfter          = "write-after"
	AttributeDatabase           = "db"
	AttributeRetentionPolicy    = "rp"
	AttributeMeasurement        = "mst"
	AttributePrecision          = "precision"
	AttributeCommand            = "cmd"
	AttributeResponseStatusCode = "status-code"
	AttributeResponseBody       = "response-body"
	AttributeWriteLineProtocol  = "lp"
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

func newTracerProvider() (*trace.TracerProvider, error) {
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

	tracerProvider := trace.NewTracerProvider(
		trace.WithResource(resource.NewWithAttributes("", semconv.ServiceName("opengemini-client-go"))),
		trace.WithBatcher(traceExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			trace.WithBatchTimeout(time.Second)),
	)
	return tracerProvider, nil
}

type OtelClient struct {
}

func NewOtelInterceptor() Interceptor {
	return &OtelClient{}
}

func (o *OtelClient) QueryBefore(ctx context.Context, query *Query) context.Context {
	tracer := otel.Tracer(TraceName)
	ctx, span := tracer.Start(ctx, SpanNameQueryBefore)
	defer span.End()

	span.SetAttributes(attribute.String(AttributeDatabase, query.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, query.RetentionPolicy))
	span.SetAttributes(attribute.String(AttributePrecision, query.Precision.Epoch()))
	span.SetAttributes(attribute.String(AttributeCommand, query.Command))

	return ctx
}

func (o *OtelClient) QueryAfter(ctx context.Context, response *http.Response) {
	if response == nil {
		return
	}

	tracer := otel.Tracer(TraceName)
	ctx, span := tracer.Start(ctx, SpanNameQueryAfter)
	defer span.End()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("read query body failed:", err)
		return
	}

	span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
	span.SetAttributes(attribute.String(AttributeResponseBody, string(body)))
}

func (o *OtelClient) WriteBefore(ctx context.Context, point *OtelPoint) context.Context {
	tracer := otel.Tracer(TraceName)
	ctx, span := tracer.Start(ctx, SpanNameWriteBefore)
	defer span.End()

	span.SetAttributes(attribute.String(AttributeDatabase, point.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, point.RetentionPolicy))

	if point.Measurement == "batch_write" {
		span.SetAttributes(attribute.String("write.type", "batch"))
	} else {
		span.SetAttributes(attribute.String(AttributeMeasurement, point.Measurement))
		span.SetAttributes(attribute.String(AttributePrecision, point.Precision))

		if point.Point != nil {
			span.SetAttributes(attribute.String(AttributeWriteLineProtocol, point.toLineProtocol()))
		}
	}
	return ctx
}

func (o *OtelClient) WriteAfter(ctx context.Context, response *http.Response) {
	tracer := otel.Tracer(TraceName)
	ctx, span := tracer.Start(ctx, SpanNameWriteAfter)
	defer span.End()

	span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
}
