package application

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// K6: borttagna mätpunkter lämnar inga spöken i blobben.
func TestPruneRemovedState(t *testing.T) {
	cfg := &catalog.EffectiveConfig{
		Properties: map[string]things.Property{"a": {}},
		Recipes:    []catalog.Recipe{{Name: "r", Operator: "passthrough", Version: "v1", Inputs: []string{"a"}, Outputs: []string{"b"}}},
	}
	v := 1.0
	st := StoredThingState{
		Current: map[string]CurrentValue{
			"a":    {Value: &v, Quality: "ok", ObservedAt: time.Now().UTC()},
			"gone": {Value: &v, Quality: "ok", ObservedAt: time.Now().UTC()},
		},
		ProcessorStates: map[string]StoredProcessor{
			"b":     {Operator: "passthrough", Version: "v1"},
			"stale": {Operator: "counter", Version: "v1"},
		},
	}
	pruneRemovedState(cfg, &st)
	if _, ok := st.Current["gone"]; ok {
		t.Fatalf("current for removed property must be pruned: %+v", st.Current)
	}
	if _, ok := st.Current["a"]; !ok {
		t.Fatal("current for kept property must survive")
	}
	if _, ok := st.ProcessorStates["stale"]; ok {
		t.Fatalf("state for removed output must be pruned: %+v", st.ProcessorStates)
	}
	if _, ok := st.ProcessorStates["b"]; !ok {
		t.Fatal("state for kept output must survive")
	}
}

// K6 integration: uppdatering som tar bort en valfri mätpunkt rensar dess
// aktuella värde ur blobben; historik påverkas inte.
func TestRegisterPrunesRemovedPropertyCurrent(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "prune", Version: "v1",
			Required: []string{"distance", "level"},
			Optional: []string{"extra"},
			Category: "wastebin",
		},
		ParamDefaults: map[string]float64{"sensorToBottom": 1.5},
		Recipes: []catalog.Recipe{
			{Name: "lvl", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
		},
	}
	if err := cat.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "prune-1", Tenant: "t", Name: "Prune",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "prune", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}, "level": {}, "extra": {}},
		Bindings: []things.Binding{
			{DeviceID: "dev1", Object: rtDistURN, Resource: "5700", Input: "distance"},
			{DeviceID: "dev2", Object: rtDistURN, Resource: "5700", Input: "extra"},
		},
	}
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{
		rtDistObs("dev1", 0.30, base), rtDistObs("dev2", 5.0, base),
	}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	st, ok := fs.LoadThing("t", "prune-1")
	if !ok {
		t.Fatal("thing missing")
	}
	if _, ok := st.Current["extra"]; !ok {
		t.Fatalf("precondition: extra must be current before removal: %+v", st.Current)
	}

	obj.Properties = map[string]things.Property{"distance": {}, "level": {}}
	obj.Bindings = obj.Bindings[:1]
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("update: %v", err)
	}
	st, _ = fs.LoadThing("t", "prune-1")
	if _, ok := st.Current["extra"]; ok {
		t.Fatalf("current for removed property must be pruned: %+v", st.Current)
	}
	if _, ok := st.Current["distance"]; !ok {
		t.Fatal("current for kept property must survive")
	}
	if n := fs.historyCount("t", "prune-1"); n == 0 {
		t.Fatal("history must be preserved when properties are removed")
	}
}

// K6: GDPR-radering via Manager tar historik + transient för saken,
// lämnar andra saker och blobben orörda, okänd sak ger 0 utan fel.
func TestManagePurgeThingHistory(t *testing.T) {
	fs := newFakeStore()
	mgr := NewManager(catalog.New(), NewRuntime(fs), fs)
	ctx := context.Background()
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkHist := func(prop string, at time.Time) HistoryWrite {
		v := 1.0
		return HistoryWrite{PropertyID: prop, Value: &v, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Source: "s", MessageID: "m-" + prop}
	}
	fs.AppendHistory("t", "bin-1", mkHist("a", at))
	fs.AppendHistory("t", "bin-1", mkHist("b", at.Add(time.Minute)))
	fs.AppendHistory("t", "bin-2", mkHist("a", at))

	if n := mgr.PurgeThingHistory(ctx, "t", "bin-1"); n != 2 {
		t.Fatalf("purged=%d, want 2", n)
	}
	if got := fs.History("t", "bin-1", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("bin-1 history must be gone: %+v", got)
	}
	if got := fs.History("t", "bin-2", "", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("bin-2 history must survive: %+v", got)
	}
	if n := mgr.PurgeThingHistory(ctx, "t", "unknown"); n != 0 {
		t.Fatalf("unknown thing must purge 0, got %d", n)
	}
}
