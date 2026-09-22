package storage

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// K6: GDPR-radering i minnet tar historikrader + sedda nycklar för saken,
// lämnar andra saker orörda; ominflöde av samma innehåll lagras igen.
func TestMemHistoryPurgeThing(t *testing.T) {
	s := NewMemHistoryStore()
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 1.0
	mk := func(thing, prop string, at time.Time) HistoryRecord {
		return HistoryRecord{Tenant: "t", ThingID: thing, PropertyID: prop,
			ObservedAt: at, ReceivedAt: at, Value: &v, Quality: "ok",
			Source: "s", MessageID: "m"}
	}
	s.AppendIdempotent(mk("bin-1", "a", at))
	s.AppendIdempotent(mk("bin-1", "b", at.Add(time.Minute)))
	s.AppendIdempotent(mk("bin-2", "a", at))

	if n := s.PurgeThing("t", "bin-1"); n != 2 {
		t.Fatalf("purged=%d, want 2", n)
	}
	if got := s.List("t", "bin-1", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-1 history must be gone: %+v", got)
	}
	if got := s.List("t", "bin-2", "", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("bin-2 history must survive: %+v", got)
	}
	if !s.AppendIdempotent(mk("bin-1", "a", at)) {
		t.Fatal("purged content must be ingestible again (seen dropped)")
	}
	if n := s.PurgeThing("t", "unknown"); n != 0 {
		t.Fatalf("unknown thing must purge 0, got %d", n)
	}
}

// K6: GDPR-radering i PostgreSQL tar historikrader för saken och lämnar
// andra saker orörda. Kräver testdatabas (se pgTestStore).
func TestPostgresPurgeThingHistory(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 1.0
	mk := func(prop string, at time.Time) application.HistoryWrite {
		return application.HistoryWrite{PropertyID: prop, Value: &v, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m-" + prop}
	}
	a.AppendHistory(context.Background(), "t", "bin-42", mk("a", at))
	a.AppendHistory(context.Background(), "t", "bin-42", mk("b", at.Add(time.Minute)))
	a.AppendHistory(context.Background(), "t", "bin-43", mk("a", at))

	if n, err := s.PurgeThingHistory(context.Background(), "t", "bin-42"); err != nil || n != 2 {
		t.Fatalf("purged=%d err=%v, want 2", n, err)
	}
	if got := a.History(context.Background(), "t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-42 history must be gone: %+v", got)
	}
	if got := a.History(context.Background(), "t", "bin-43", "", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("bin-43 history must survive: %+v", got)
	}
	if n, err := s.PurgeThingHistory(context.Background(), "t", "unknown"); err != nil || n != 0 {
		t.Fatalf("unknown thing must purge 0, got %d err=%v", n, err)
	}
}

// TV-02: purge tar historik + väntande värdesevent men bevarar
// livscykel-/relationsrader, dedup och sedda ID:n: återleverans efter
// purge omberäknas inte. Kräver testdatabas (se pgTestStore).
func TestPostgresPurgeKeepsDeliveryProtection(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 1.0
	a.AppendHistory(context.Background(), "t", "bin-42", application.HistoryWrite{
		PropertyID: "distance", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m1",
	})
	ev := application.FlowEvent{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending(context.Background(), "t", "bin-42", []application.FlowEvent{ev})
	raw := application.RawOutboxEvent{
		ID: "v1/t/bin-42/lifecycle/created/1", Topic: pkgtypes.TopicLifecycleChanges,
		Payload: []byte(`{"kind":"created"}`), ObservedAt: at, Revision: 1,
	}
	a.StoreRawEvents(context.Background(), "t", "bin-42", []application.RawOutboxEvent{raw})
	a.DedupMark(context.Background(), "t", "m1", "bin-42")

	if n, err := s.PurgeThingHistory(context.Background(), "t", "bin-42"); err != nil || n != 1 {
		t.Fatalf("purged=%d err=%v, want 1", n, err)
	}
	if got := a.History(context.Background(), "t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("history must be gone: %+v", got)
	}
	if got := a.PendingEvents(context.Background(), "t", "bin-42"); len(got) != 0 {
		t.Fatalf("pending values must be gone: %+v", got)
	}
	if got := a.PendingRawEvents(context.Background(), "t", "bin-42"); len(got) != 1 {
		t.Fatalf("lifecycle rows must survive purge: %+v", got)
	}
	if !a.DedupCheck(context.Background(), "t", "m1", "bin-42") {
		t.Fatal("dedup mark must survive purge")
	}
	// Återleverans av samma meddelande dedupliceras fortfarande.
	a.StorePending(context.Background(), "t", "bin-42", []application.FlowEvent{ev})
	if got := a.PendingEvents(context.Background(), "t", "bin-42"); len(got) != 0 {
		t.Fatalf("redelivery must stay deduped, pending=%+v", got)
	}
}

// TV-02 i minnet: samma skydd som pg — värderader bort, livscykel,
// dedup och sedda ID:n kvar, återleverans deduplicerad.
func TestAdapterPurgeKeepsDeliveryProtection(t *testing.T) {
	a := newAdapter(t)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 1.0
	a.AppendHistory(context.Background(), "t", "bin-42", application.HistoryWrite{
		PropertyID: "distance", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m1",
	})
	ev := application.FlowEvent{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending(context.Background(), "t", "bin-42", []application.FlowEvent{ev})
	raw := application.RawOutboxEvent{
		ID: "v1/t/bin-42/lifecycle/created/1", Topic: pkgtypes.TopicLifecycleChanges,
		Payload: []byte(`{"kind":"created"}`), ObservedAt: at, Revision: 1,
	}
	a.StoreRawEvents(context.Background(), "t", "bin-42", []application.RawOutboxEvent{raw})
	a.DedupMark(context.Background(), "t", "m1", "bin-42")

	if n, err := a.PurgeThingHistory(context.Background(), "t", "bin-42"); err != nil || n != 1 {
		t.Fatalf("purged=%d err=%v, want 1", n, err)
	}
	if got := a.History(context.Background(), "t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("history must be gone: %+v", got)
	}
	if got := a.PendingEvents(context.Background(), "t", "bin-42"); len(got) != 0 {
		t.Fatalf("pending values must be gone: %+v", got)
	}
	if got := a.PendingRawEvents(context.Background(), "t", "bin-42"); len(got) != 1 {
		t.Fatalf("lifecycle rows must survive purge: %+v", got)
	}
	if !a.DedupCheck(context.Background(), "t", "m1", "bin-42") {
		t.Fatal("dedup mark must survive purge")
	}
	a.StorePending(context.Background(), "t", "bin-42", []application.FlowEvent{ev})
	if got := a.PendingEvents(context.Background(), "t", "bin-42"); len(got) != 0 {
		t.Fatalf("redelivery must stay deduped, pending=%+v", got)
	}
}
