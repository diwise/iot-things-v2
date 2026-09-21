package storage

import (
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
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
	a.AppendHistory("t", "bin-42", mk("a", at))
	a.AppendHistory("t", "bin-42", mk("b", at.Add(time.Minute)))
	a.AppendHistory("t", "bin-43", mk("a", at))

	if n := s.PurgeThingHistory("t", "bin-42"); n != 2 {
		t.Fatalf("purged=%d, want 2", n)
	}
	if got := a.History("t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-42 history must be gone: %+v", got)
	}
	if got := a.History("t", "bin-43", "", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("bin-43 history must survive: %+v", got)
	}
	if n := s.PurgeThingHistory("t", "unknown"); n != 0 {
		t.Fatalf("unknown thing must purge 0, got %d", n)
	}
}
