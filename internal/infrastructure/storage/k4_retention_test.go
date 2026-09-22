package storage

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
)

// K4: retention i minnet tar rader äldre än cutoff, behåller gränsen och
// bygger om sedda nycklar så att rensat innehåll kan strömma in igen medan
// överlevande dubbletter fortsatt dedupliceras.
func TestMemHistoryPurgeOlderThan(t *testing.T) {
	s := NewMemHistoryStore()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	cutoff := now.AddDate(0, 0, -30)
	v := 1.0
	mk := func(prop string, at time.Time) HistoryRecord {
		return HistoryRecord{Tenant: "t", ThingID: "bin-1", PropertyID: prop,
			ObservedAt: at, ReceivedAt: at, Value: &v, Quality: "ok",
			Source: "s", MessageID: "m-" + prop}
	}
	s.AppendIdempotent(mk("old", cutoff.Add(-time.Hour)))
	s.AppendIdempotent(mk("edge", cutoff))
	s.AppendIdempotent(mk("new", cutoff.Add(time.Hour)))

	if n := s.PurgeOlderThan(cutoff); n != 1 {
		t.Fatalf("purged=%d, want 1", n)
	}
	got := s.List("t", "bin-1", "", time.Time{}, time.Time{}, 0, 0)
	if len(got) != 2 {
		t.Fatalf("survivors=%d, want 2 (edge + new)", len(got))
	}
	if !s.AppendIdempotent(mk("old", cutoff.Add(-time.Hour))) {
		t.Fatal("purged content must be ingestible again (seen rebuilt)")
	}
	if s.AppendIdempotent(mk("new", cutoff.Add(time.Hour))) {
		t.Fatal("surviving content must still deduplicate")
	}
}

// K4: retention i PostgreSQL tar rader äldre än cutoff. Kräver testdatabas.
func TestPostgresPurgeHistoryOlderThan(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	cutoff := now.AddDate(0, 0, -30)
	v := 1.0
	mk := func(prop string, at time.Time) application.HistoryWrite {
		return application.HistoryWrite{PropertyID: prop, Value: &v, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m-" + prop}
	}
	a.AppendHistory(context.Background(), "t", "bin-42", mk("old", cutoff.Add(-time.Hour)))
	a.AppendHistory(context.Background(), "t", "bin-42", mk("edge", cutoff))
	a.AppendHistory(context.Background(), "t", "bin-42", mk("new", cutoff.Add(time.Hour)))
	a.AppendHistory(context.Background(), "t", "bin-43", mk("old", cutoff.Add(-time.Hour)))

	if n, err := s.PurgeHistoryOlderThan(context.Background(), cutoff); err != nil || n != 2 {
		t.Fatalf("purged=%d err=%v, want 2 (both tenants)", n, err)
	}
	if got := a.History(context.Background(), "t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 2 {
		t.Fatalf("bin-42 survivors=%d, want 2", len(got))
	}
	if got := a.History(context.Background(), "t", "bin-43", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-43 must be fully purged: %+v", got)
	}
}
