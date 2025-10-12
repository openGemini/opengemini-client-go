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
	"context"

	"go.opentelemetry.io/otel"
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
