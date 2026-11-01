package sqliteha

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const telemetryInstrumentationName = "github.com/litesql/go-sqlite-ha"

type sqliteTelemetry struct {
	tracer            trace.Tracer
	operationCount    metric.Int64Counter
	operationDuration metric.Float64Histogram
	backupSize        metric.Int64Histogram
}

type operationState struct {
	span  trace.Span
	route string
}

type operationContextKey struct{}

var (
	sqliteTelemetryOnce sync.Once
	sqliteTelemetryInst *sqliteTelemetry
	sqliteTelemetryErr  error
)

func newSQLiteTelemetry() (*sqliteTelemetry, error) {
	return newSQLiteTelemetryWithProviders(otel.GetMeterProvider(), otel.GetTracerProvider())
}

func defaultSQLiteTelemetry() (*sqliteTelemetry, error) {
	sqliteTelemetryOnce.Do(func() {
		sqliteTelemetryInst, sqliteTelemetryErr = newSQLiteTelemetry()
	})
	return sqliteTelemetryInst, sqliteTelemetryErr
}

func newSQLiteTelemetryWithProviders(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*sqliteTelemetry, error) {
	meter := meterProvider.Meter(telemetryInstrumentationName)
	t := &sqliteTelemetry{
		tracer: tracerProvider.Tracer(telemetryInstrumentationName),
	}
	var err error
	if t.operationCount, err = meter.Int64Counter(
		"go_sqlite_ha.db.operations",
		metric.WithDescription("Number of SQLite driver operations."),
	); err != nil {
		return nil, err
	}
	if t.operationDuration, err = meter.Float64Histogram(
		"go_sqlite_ha.db.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of SQLite driver operations."),
	); err != nil {
		return nil, err
	}
	if t.backupSize, err = meter.Int64Histogram(
		"go_sqlite_ha.backup.size",
		metric.WithUnit("By"),
		metric.WithDescription("Size of SQLite backups written by the driver."),
	); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *sqliteTelemetry) startOperation(ctx context.Context, operation string) (context.Context, func(error)) {
	if t == nil {
		return ctx, func(error) {}
	}
	ctx, span := t.tracer.Start(ctx, "go_sqlite_ha.db."+operation)
	state := &operationState{span: span, route: "local"}
	ctx = context.WithValue(ctx, operationContextKey{}, state)
	span.SetAttributes(
		attribute.String("db.system.name", "sqlite"),
		attribute.String("db.operation.name", operation),
		attribute.String("go_sqlite_ha.route", state.route),
	)
	started := time.Now()
	return ctx, func(err error) {
		result := operationResult(err)
		span.SetAttributes(attribute.String("db.operation.result", result))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		attrs := metric.WithAttributes(
			attribute.String("db.system.name", "sqlite"),
			attribute.String("db.operation.name", operation),
			attribute.String("db.operation.result", result),
			attribute.String("go_sqlite_ha.route", state.route),
		)
		t.operationCount.Add(ctx, 1, attrs)
		t.operationDuration.Record(ctx, time.Since(started).Seconds(), attrs)
		span.End()
	}
}

func (t *sqliteTelemetry) startBackup(ctx context.Context) (context.Context, func(error, int64)) {
	if t == nil {
		return ctx, func(error, int64) {}
	}
	ctx, span := t.tracer.Start(ctx, "go_sqlite_ha.backup")
	started := time.Now()
	span.SetAttributes(
		attribute.String("db.system.name", "sqlite"),
		attribute.String("db.operation.name", "backup"),
		attribute.String("go_sqlite_ha.route", "local"),
	)
	return ctx, func(err error, bytes int64) {
		span.SetAttributes(
			attribute.Int64("backup.size.bytes", bytes),
			attribute.String("db.operation.result", operationResult(err)),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		attrs := metric.WithAttributes(
			attribute.String("db.system.name", "sqlite"),
			attribute.String("db.operation.name", "backup"),
			attribute.String("db.operation.result", operationResult(err)),
			attribute.String("go_sqlite_ha.route", "local"),
		)
		t.operationCount.Add(ctx, 1, attrs)
		t.operationDuration.Record(ctx, time.Since(started).Seconds(), attrs)
		t.backupSize.Record(ctx, bytes, attrs)
		span.End()
	}
}

func setOperationRoute(ctx context.Context, route string) {
	if state, ok := ctx.Value(operationContextKey{}).(*operationState); ok {
		state.route = route
		state.span.SetAttributes(attribute.String("go_sqlite_ha.route", route))
	}
}

func operationResult(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "cancelled"
	}
	if err != nil {
		return "error"
	}
	return "success"
}
