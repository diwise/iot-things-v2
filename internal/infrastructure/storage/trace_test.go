package storage

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// TV-07: trace-context överlever outbox (minne + postgres) separat från payload.
func TestOutboxTraceRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	tc := map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}

	// Minne via adapter.
	s, err := New(t.Context(), NewConfig("", "", "", "5432", "diwise", "disable"))
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	a := NewRuntimeAdapter(s)
	ev := application.FlowEvent{PropertyID: "p", Value: 1, ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1, TraceContext: tc}
	a.StorePending(context.Background(), "t", "th", []application.FlowEvent{ev})
	got := a.PendingEvents(context.Background(), "t", "th")
	if len(got) != 1 || got[0].TraceContext["traceparent"] != tc["traceparent"] {
		t.Fatalf("memory values trace=%+v, want %+v", got, tc)
	}
	raw := application.RawOutboxEvent{ID: "v1/t/th/lifecycle/created/1", Topic: pkgtypes.TopicLifecycleChanges, Payload: []byte(`{}`), ObservedAt: at, Revision: 1, TraceContext: tc}
	a.StoreRawEvents(context.Background(), "t", "th", []application.RawOutboxEvent{raw})
	rgot := a.PendingRawEvents(context.Background(), "t", "th")
	if len(rgot) != 1 || rgot[0].TraceContext["traceparent"] != tc["traceparent"] {
		t.Fatalf("memory raw trace=%+v, want %+v", rgot, tc)
	}

	// Postgres (skippas utan DB): PgStore implementerar RuntimeStore direkt.
	var ps application.RuntimeStore = pgTestStore(t)
	pev := application.FlowEvent{PropertyID: "p", Value: 1, ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1, TraceContext: tc}
	ps.StorePending(context.Background(), "t", "th-pg", []application.FlowEvent{pev})
	pgot := ps.PendingEvents(context.Background(), "t", "th-pg")
	if len(pgot) != 1 || pgot[0].TraceContext["traceparent"] != tc["traceparent"] {
		t.Fatalf("pg values trace=%+v, want %+v", pgot, tc)
	}
	praw := application.RawOutboxEvent{ID: "v1/t/th-pg/lifecycle/created/1", Topic: pkgtypes.TopicLifecycleChanges, Payload: []byte(`{}`), ObservedAt: at, Revision: 1, TraceContext: tc}
	ps.StoreRawEvents(context.Background(), "t", "th-pg", []application.RawOutboxEvent{praw})
	prgot := ps.PendingRawEvents(context.Background(), "t", "th-pg")
	if len(prgot) != 1 || prgot[0].TraceContext["traceparent"] != tc["traceparent"] {
		t.Fatalf("pg raw trace=%+v, want %+v", prgot, tc)
	}
}
