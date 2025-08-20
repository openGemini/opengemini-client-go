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
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
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

type InterceptorQuery struct {
	*Query
	Ctx     context.Context
	Span    trace.Span
	Carrier propagation.TextMapCarrier
}

type InterceptorWrite struct {
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

func (o *OtelClient) Query(ctx context.Context, query *InterceptorQuery) InterceptorClosure {
	ctx = otel.GetTextMapPropagator().Extract(ctx, query.Carrier)
	var span trace.Span

	if query.Span != nil {
		span = query.Span
	} else {
		ctx, span = tracer.Start(ctx, SpanNameQuery)
	}

	otel.GetTextMapPropagator().Inject(ctx, query.Carrier)

	span.SetAttributes(attribute.String(AttributeDatabase, query.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, query.RetentionPolicy))
	span.SetAttributes(attribute.String(AttributePrecision, query.Precision.Epoch()))
	span.SetAttributes(attribute.String(AttributeCommand, query.Command))

	return func(ctx context.Context, response *http.Response) error {
		defer span.End()
		if response == nil {
			fmt.Println("otel interceptor query response body is nil")
			return nil // when otel makes an error, it does not affect the main business process
		}

		var buf bytes.Buffer
		tee := io.TeeReader(response.Body, &buf)
		data, err := io.ReadAll(tee)
		if err != nil {
			fmt.Println("otel interceptor read query response body failed: " + err.Error())
			return nil
		}
		response.Body = io.NopCloser(&buf)

		span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
		span.SetAttributes(attribute.String(AttributeResponseBody, string(data)))
		return nil
	}
}

func (o *OtelClient) Write(ctx context.Context, write *InterceptorWrite) InterceptorClosure {
	ctx = otel.GetTextMapPropagator().Extract(ctx, write.Carrier)
	var span trace.Span

	if write.Span != nil {
		span = write.Span
	} else {
		ctx, span = tracer.Start(ctx, SpanNameWrite)
	}

	otel.GetTextMapPropagator().Inject(ctx, write.Carrier)

	span.SetAttributes(attribute.String(AttributeDatabase, write.Database))
	span.SetAttributes(attribute.String(AttributeRetentionPolicy, write.RetentionPolicy))
	span.SetAttributes(attribute.String(AttributePrecision, write.Precision))
	span.SetAttributes(attribute.String(AttributeWriteLineProtocol, write.LineProtocol))

	return func(ctx context.Context, response *http.Response) error {
		defer span.End()
		if response == nil {
			fmt.Println("otel interceptor write response body is nil")
			return nil
		}
		span.SetAttributes(attribute.Int(AttributeResponseStatusCode, response.StatusCode))
		return nil
	}
}
