package logger

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

const LogFieldTraceId = "trace_id"

func GetTraceId(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	return sc.TraceID().String()
}

func LogWithTrace(ctx context.Context, slogLogger *slog.Logger) *slog.Logger {
	return slogLogger.With(LogFieldTraceId, GetTraceId(ctx))
}
