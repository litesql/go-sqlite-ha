package sqliteha

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSQLiteTelemetryRecordsOperationsAndBackups(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	spanExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(spanExporter)),
	)
	defer meterProvider.Shutdown(context.Background())
	defer tracerProvider.Shutdown(context.Background())

	telemetry, err := newSQLiteTelemetryWithProviders(meterProvider, tracerProvider)
	if err != nil {
		t.Fatal(err)
	}

	ctx, finishOperation := telemetry.startOperation(context.Background(), "query")
	finishOperation(errors.New("query failed"))
	_, finishBackup := telemetry.startBackup(ctx)
	finishBackup(nil, 128)

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	foundCount := false
	foundDuration := false
	foundBackupSize := false
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			switch item.Name {
			case "go_sqlite_ha.db.operations":
				sum, ok := item.Data.(metricdata.Sum[int64])
				foundCount = ok && len(sum.DataPoints) == 2
			case "go_sqlite_ha.db.operation.duration":
				histogram, ok := item.Data.(metricdata.Histogram[float64])
				foundDuration = ok && len(histogram.DataPoints) == 2
			case "go_sqlite_ha.backup.size":
				histogram, ok := item.Data.(metricdata.Histogram[int64])
				foundBackupSize = ok && len(histogram.DataPoints) == 1 &&
					histogram.DataPoints[0].Attributes.HasValue(attribute.Key("go_sqlite_ha.route"))
			}
		}
	}
	if !foundCount || !foundDuration || !foundBackupSize {
		t.Fatalf("missing metrics: operations=%v duration=%v backup_size=%v", foundCount, foundDuration, foundBackupSize)
	}

	spans := spanExporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want query and backup spans", len(spans))
	}
	if spans[0].Name != "go_sqlite_ha.db.query" || spans[1].Name != "go_sqlite_ha.backup" {
		t.Fatalf("unexpected span names: %q, %q", spans[0].Name, spans[1].Name)
	}
	if spans[0].Status.Code != codes.Error {
		t.Fatalf("query span status = %v, want error", spans[0].Status.Code)
	}
	routeFound := false
	for _, attr := range spans[0].Attributes {
		if attr.Key == attribute.Key("go_sqlite_ha.route") && attr.Value.AsString() == "local" {
			routeFound = true
			break
		}
	}
	if !routeFound {
		t.Fatal("query span is missing the local route attribute")
	}
}
