package application

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
)

// K4: retention via Manager raderar äldre historik i alla tenants och
// rapporterar antal; gräns och nytt innehåll påverkas inte.
func TestManagePurgeHistory(t *testing.T) {
	fs := newFakeStore()
	mgr := NewManager(catalog.New(), NewRuntime(fs), fs)
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	cutoff := now.AddDate(0, 0, -30)
	mkHist := func(prop string, at time.Time) HistoryWrite {
		v := 1.0
		return HistoryWrite{PropertyID: prop, Value: &v, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m-" + prop}
	}
	fs.AppendHistory("t", "bin-1", mkHist("old", cutoff.Add(-time.Hour)))
	fs.AppendHistory("t", "bin-1", mkHist("edge", cutoff))
	fs.AppendHistory("t", "bin-1", mkHist("new", cutoff.Add(time.Hour)))
	fs.AppendHistory("o", "bin-9", mkHist("old", cutoff.Add(-time.Hour)))

	if n := mgr.PurgeHistory(context.Background(), cutoff); n != 2 {
		t.Fatalf("purged=%d, want 2", n)
	}
	if got := fs.History("t", "bin-1", "", time.Time{}, time.Time{}, 0, 0); len(got) != 2 {
		t.Fatalf("bin-1 survivors=%d, want 2", len(got))
	}
	if got := fs.History("o", "bin-9", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-9 must be fully purged: %+v", got)
	}
}
