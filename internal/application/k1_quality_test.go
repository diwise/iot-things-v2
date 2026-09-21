package application

import (
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// K1.5 code review: min/max hade endast enhetstester, ingen flödesbevis.
// Lås att de aggregerar över bundna signaler med samma kvalitetsregler
// som mean (alla ok/uncertain, uncertain smittar, invalid -> skip).
func minMaxCfg(op string) catalog.EffectiveConfig {
	return catalog.EffectiveConfig{
		Properties: map[string]things.Property{
			"temperature.window": {}, "temperature.center": {},
			"temperature." + op: {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNtemp, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s2", Object: flowURNtemp, Resource: "5700", Input: "temperature.center"},
		},
		Recipes: []catalog.Recipe{
			{Name: op + "-temp", Operator: op, Version: "v1",
				Inputs:  []string{"temperature.window", "temperature.center"},
				Outputs: []string{"temperature." + op}},
		},
	}
}

func TestK1MinFlowAggregates(t *testing.T) {
	cfg := minMaxCfg("min")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 24, base.Add(time.Minute)),
	}, base.Add(2*time.Minute), "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	got, ok := out.Updated["temperature.min"]
	if !ok || got.Value == nil || *got.Value != 18 {
		t.Fatalf("min=%+v, want 18", got)
	}
	if got.Quality != "ok" {
		t.Fatalf("min quality=%q, want ok", got.Quality)
	}
}

func TestK1MaxFlowAggregates(t *testing.T) {
	cfg := minMaxCfg("max")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 24, base.Add(time.Minute)),
	}, base.Add(2*time.Minute), "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	got, ok := out.Updated["temperature.max"]
	if !ok || got.Value == nil || *got.Value != 24 {
		t.Fatalf("max=%+v, want 24", got)
	}
}

func TestK1MinMaxUncertainPropagatesAndInvalidSkips(t *testing.T) {
	for _, op := range []string{"min", "max"} {
		cfg := minMaxCfg(op)
		base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
		uncertain := tempObs("s2", 24, base.Add(time.Minute))
		uncertain.Quality = things.QualityUncertain
		out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
			tempObs("s1", 18, base), uncertain,
		}, base.Add(2*time.Minute), "m1", 1)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if q := out.Updated["temperature."+op].Quality; q != "uncertain" {
			t.Fatalf("%s uncertain quality=%q, want uncertain", op, q)
		}
		invalid := tempObs("s2", 24, base.Add(time.Minute))
		invalid.Quality = things.QualityInvalid
		out, err = EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
			tempObs("s1", 18, base), invalid,
		}, base.Add(2*time.Minute), "m2", 1)
		if err != nil {
			t.Fatalf("%s invalid: %v", op, err)
		}
		if _, ok := out.Updated["temperature."+op]; ok {
			t.Fatalf("%s invalid input must skip, got %+v", op, out.Updated)
		}
	}
}

// K1.5 code review: counterFalling/counterBoth saknade flödestest.
// Lås flankbeteendet genom flödet (inte bara CountEdge-enhetstest).
func counterEdgeCfg(edge string) catalog.EffectiveConfig {
	op := "counter"
	if edge != "" {
		op = edge
	}
	return catalog.EffectiveConfig{
		Properties: map[string]things.Property{"door": {}, "passages": {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: flowURNpresence, Resource: "5500", Input: "door"},
		},
		Recipes: []catalog.Recipe{
			{Name: "count-passages", Operator: op, Version: "v1",
				Inputs: []string{"door"}, Outputs: []string{"passages"}},
		},
	}
}

func TestK1CounterFallingCountsOnlyTrueToFalse(t *testing.T) {
	cfg := counterEdgeCfg("counterFalling")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	round := func(v bool, at time.Time, id string) float64 {
		t.Helper()
		out, err := EvaluateBatchFlow(&cfg, current, proc,
			[]things.Observation{boolObs("s", flowURNpresence, "5500", v, at)}, at, id, 1)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		current, proc = mergeCurrent(current, out), mergeProc(proc, out)
		return *out.Updated["passages"].Value
	}
	if got := round(true, base, "m0"); got != 0 {
		t.Fatalf("initial active must not count falling, got %v", got)
	}
	if got := round(false, base.Add(time.Minute), "m1"); got != 1 {
		t.Fatalf("true->false must count, got %v", got)
	}
	if got := round(true, base.Add(2*time.Minute), "m2"); got != 1 {
		t.Fatalf("false->true must not count falling, got %v", got)
	}
}

func TestK1CounterBothCountsEveryTransition(t *testing.T) {
	cfg := counterEdgeCfg("counterBoth")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	round := func(v bool, at time.Time, id string) float64 {
		t.Helper()
		out, err := EvaluateBatchFlow(&cfg, current, proc,
			[]things.Observation{boolObs("s", flowURNpresence, "5500", v, at)}, at, id, 1)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		current, proc = mergeCurrent(current, out), mergeProc(proc, out)
		return *out.Updated["passages"].Value
	}
	if got := round(true, base, "m0"); got != 1 {
		t.Fatalf("first active counts (both), got %v", got)
	}
	if got := round(false, base.Add(time.Minute), "m1"); got != 2 {
		t.Fatalf("true->false counts (both), got %v", got)
	}
	if got := round(true, base.Add(2*time.Minute), "m2"); got != 3 {
		t.Fatalf("false->true counts (both), got %v", got)
	}
}
