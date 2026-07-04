// Package obs wires OpenTelemetry tracing and carries trace context across NATS,
// so one order shows up as a single connected trace spanning every service.
package obs

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init sets up the tracer provider. Endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT.
// Export is best-effort: if the collector is down, spans are dropped, not fatal.
func Init(ctx context.Context, service string) (func(context.Context) error, error) {
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

// Publish sends a JetStream message with the current trace context injected into
// its headers, so the consumer can continue the same trace.
func Publish(ctx context.Context, js jetstream.JetStream, subject string, data []byte) error {
	h := nats.Header{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
	_, err := js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data, Header: h})
	return err
}

// Extract pulls trace context out of a consumed message's headers.
func Extract(ctx context.Context, msg jetstream.Msg) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(msg.Headers()))
}
