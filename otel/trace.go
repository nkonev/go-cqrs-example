package otel

import (
	"context"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	jaegerPropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
)

func ConfigureTracePropagator() propagation.TextMapPropagator {
	return jaegerPropagator.Jaeger{}
}

func ConfigureTraceProvider(
	slogLogger *slog.Logger,
	propagator propagation.TextMapPropagator,
	exporter *otlptrace.Exporter,
	lc fx.Lifecycle,
) *sdktrace.TracerProvider {
	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(app.TRACE_RESOURCE),
	)
	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(batchSpanProcessor),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)

	// register jaeger propagator
	otel.SetTextMapPropagator(propagator)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping trace provider")
			if err := tp.Shutdown(context.Background()); err != nil {
				slogLogger.Error("Error shutting trace provider", "err", err)
			}
			return nil
		},
	})
	return tp
}

func ConfigureTraceExporter(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (*otlptrace.Exporter, error) {
	traceExporterConn, err := grpc.DialContext(context.Background(), cfg.OtlpConfig.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithGRPCConn(traceExporterConn))
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping trace exporter")

			if err := exporter.Shutdown(ctx); err != nil {
				slogLogger.Error("Error shutting down trace exporter", "err", err)
			}

			if err := traceExporterConn.Close(); err != nil {
				slogLogger.Error("Error shutting down trace exporter connection", "err", err)
			}
			return nil
		},
	})

	return exporter, err
}
