package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// withSpanRecorder installerar en tracetest-recorder på paketets tracer och
// returnerar restore-funktion. Den globala providern rörs inte: redan skapade
// tracers byts endast ut vid första SetTracerProvider, så global save/restore
// per test ger ordningsberoende tomma recorders. Direktbyte är deterministiskt.
func withSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	prev := runtimeTracer
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	runtimeTracer = tp.Tracer("test")
	return rec, func() {
		_ = tp.Shutdown(context.Background())
		runtimeTracer = prev
	}
}

func spanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) (any, bool) {
	for _, a := range span.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsInterface(), true
		}
	}
	return nil, false
}

// Kedjan: handle-batch + evaluate-thing länkas som förälder/barn med
// tenant-, sak- och message-attribut.
func TestTracingBatchChain(t *testing.T) {
	rec, restore := withSpanRecorder(t)
	defer restore()

	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m-trace", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}

	batch := spanByName(rec.Ended(), "handle-batch")
	if batch == nil {
		t.Fatalf("no handle-batch span, got %v", spanNames(rec.Ended()))
	}
	if v, ok := spanAttr(batch, "tenant"); !ok || v != "t" {
		t.Fatalf("handle-batch tenant=%v, want t", v)
	}
	if v, ok := spanAttr(batch, "messaging.message_id"); !ok || v != "m-trace" {
		t.Fatalf("handle-batch message_id=%v, want m-trace", v)
	}

	child := spanByName(rec.Ended(), "evaluate-thing")
	if child == nil {
		t.Fatal("no evaluate-thing span")
	}
	if child.Parent().SpanID() != batch.SpanContext().SpanID() {
		t.Fatal("evaluate-thing must be a child of handle-batch")
	}
	if v, ok := spanAttr(child, "thing.id"); !ok || v != "bin-42" {
		t.Fatalf("evaluate-thing thing.id=%v, want bin-42", v)
	}
	if v, ok := spanAttr(child, "events"); !ok || v != int64(3) {
		t.Fatalf("evaluate-thing events=%v, want 3", v)
	}
	if child.Status().Code != codes.Unset {
		t.Fatalf("success span status=%v, want Unset", child.Status().Code)
	}
}

type failPublisher struct{}

func (failPublisher) PublishThingValues(context.Context, pkgtypes.ThingValuesEventV1) error {
	return errors.New("broker down")
}

func (failPublisher) PublishMessage(context.Context, messaging.TopicMessage) error {
	return errors.New("broker down")
}

// Fel i kedjan (här: misslyckad publicering) ska markera span som Error.
func TestTracingDrainError(t *testing.T) {
	rec, restore := withSpanRecorder(t)
	defer restore()

	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := rt.DrainAndPublish(context.Background(), failPublisher{}, "t", "bin-42", base); err == nil {
		t.Fatal("publish must fail")
	}

	drain := spanByName(rec.Ended(), "drain-publish")
	if drain == nil {
		t.Fatalf("no drain-publish span, got %v", spanNames(rec.Ended()))
	}
	if drain.Status().Code != codes.Error {
		t.Fatalf("failed drain span status=%v, want Error", drain.Status().Code)
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}
