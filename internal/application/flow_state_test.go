package application

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/functions"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

const flowStateDistURN = "urn:oma:lwm2m:ext:3330"

func activeTimeCfg() catalog.EffectiveConfig {
	return catalog.EffectiveConfig{
		Properties: map[string]things.Property{"sig": {}, "run": {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: flowURNtemp, Resource: "5700", Input: "sig"},
		},
		Recipes: []catalog.Recipe{
			{Name: "run-time", Operator: "activeTime", Version: "v1",
				Inputs: []string{"sig"}, Outputs: []string{"run"}},
		},
	}
}

func sigObs(device string, v float64, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID(device, "", flowURNtemp, "5700"),
		Value:      new(v),
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

func mergeProc(base map[string]StoredProcessor, out *FlowOutcome) map[string]StoredProcessor {
	next := make(map[string]StoredProcessor, len(base)+len(out.ProcStates))
	for k, v := range base {
		v.StateJSON = append([]byte(nil), v.StateJSON...)
		next[k] = v
	}
	for k, v := range out.ProcStates {
		v.StateJSON = append([]byte(nil), v.StateJSON...)
		next[k] = v
	}
	return next
}

func decodeActiveTime(t *testing.T, sp StoredProcessor) functions.ActiveTimeState {
	t.Helper()
	var st functions.ActiveTimeState
	if err := json.Unmarshal(sp.StateJSON, &st); err != nil {
		t.Fatalf("decode activeTime state: %v", err)
	}
	return st
}

// Livscykel: start -> pågående vy -> avslut med ackumulerat, tillstånd
// versionerat och persisterbart via ProcStates.
func TestActiveTimeLifecycle(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	t2 := base.Add(10 * time.Minute)
	t3 := base.Add(20 * time.Minute)

	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}

	// t0: inaktiv signal -> tid 0, tillstånd inaktivt lagrat.
	out, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 0, base)}, base, "m0", 1)
	if err != nil {
		t.Fatalf("t0: %v", err)
	}
	if got := *out.Updated["run"].Value; got != 0 {
		t.Fatalf("t0 run=%v, want 0", got)
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)

	// t1: aktivering -> start lagras, pågående vy 0.
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, t1)}, t1, "m1", 1)
	if err != nil {
		t.Fatalf("t1: %v", err)
	}
	if got := *out.Updated["run"].Value; got != 0 {
		t.Fatalf("t1 run=%v, want 0 (just started)", got)
	}
	st := decodeActiveTime(t, out.ProcStates["run"])
	if !st.Active || st.StartUnix != t1.Unix() {
		t.Fatalf("t1 state=%+v, want active since t1", st)
	}
	if out.ProcStates["run"].Operator != "activeTime" || out.ProcStates["run"].Version != "v1" {
		t.Fatalf("state must be version-stamped: %+v", out.ProcStates["run"])
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)

	// t2: fortsatt aktiv -> pågående vy 300 s, ingen ackumulering än.
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, t2)}, t2, "m2", 1)
	if err != nil {
		t.Fatalf("t2: %v", err)
	}
	if got := *out.Updated["run"].Value; got != 300 {
		t.Fatalf("t2 run=%v, want 300 (ongoing view)", got)
	}
	if acc := decodeActiveTime(t, out.ProcStates["run"]).AccumulatedSec; acc != 0 {
		t.Fatalf("t2 accumulated=%d, want 0 (finalizes on end)", acc)
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)

	// t3: avslut -> 900 s fullbordade.
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 0, t3)}, t3, "m3", 1)
	if err != nil {
		t.Fatalf("t3: %v", err)
	}
	if got := *out.Updated["run"].Value; got != 900 {
		t.Fatalf("t3 run=%v, want 900", got)
	}
	st = decodeActiveTime(t, out.ProcStates["run"])
	if st.Active || st.AccumulatedSec != 900 {
		t.Fatalf("t3 state=%+v, want inactive with 900", st)
	}
}

// Tyst runda: utan berörda ingångar körs inga recept (L5) — inga
// härledda historikrader eller event.
func TestQuietBatchSkipsDerived(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	out, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, base)}, base, "m1", 1)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)
	nHist := len(out.History)

	late := sigObs("s", 1, base.Add(-time.Hour))
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{late}, base.Add(time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("late: %v", err)
	}
	if len(out.Updated) != 0 {
		t.Fatalf("quiet batch must not update current: %+v", out.Updated)
	}
	if len(out.Events) != 0 {
		t.Fatalf("quiet batch must not emit events: %+v", out.Events)
	}
	if len(out.History) != 1 || !out.History[0].Late {
		t.Fatalf("quiet batch must only record late history: %+v", out.History)
	}
	_ = nHist
}

// Versionsbyte nollställer tillståndet och flaggar uncertain i stället för
// att tyst återanvända ett främmande tillstånd.
func TestActiveTimeVersionReset(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	old, _ := json.Marshal(functions.ActiveTimeState{Active: true, StartUnix: base.Unix(), AccumulatedSec: 100})
	proc := map[string]StoredProcessor{
		"run": {Operator: "activeTime", Version: "v0", StateJSON: old},
	}
	current := map[string]CurrentValue{
		"sig": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
	}
	out, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, base.Add(time.Minute))}, base.Add(time.Minute), "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if out.Updated["run"].Quality != "uncertain" {
		t.Fatalf("quality=%q after version reset, want uncertain", out.Updated["run"].Quality)
	}
	if got := *out.Updated["run"].Value; got != 0 {
		t.Fatalf("run=%v after reset, want fresh 0", got)
	}
	st := decodeActiveTime(t, out.ProcStates["run"])
	if st.AccumulatedSec != 0 {
		t.Fatalf("accumulated=%d after reset, want 0", st.AccumulatedSec)
	}
}

// Sen observation som inte blir aktuell påverkar aldrig ackumulerad tid.
func TestActiveTimeLateDoesNotCorrupt(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	out, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, t1)}, t1, "m1", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)

	late := sigObs("s", 0, base) // Äldre än aktuellt: inaktivt värde från förr.
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{late}, t1.Add(time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("late: %v", err)
	}
	if len(out.ProcStates) != 0 {
		t.Fatalf("late must not touch processor state: %+v", out.ProcStates)
	}
	st := decodeActiveTime(t, proc["run"])
	if !st.Active || st.StartUnix != t1.Unix() || st.AccumulatedSec != 0 {
		t.Fatalf("state corrupted by late observation: %+v", st)
	}
}

// Avlopp end-to-end (A7) med generiska operatorer: distans -> nivå ->
// bräddning (tröskel) -> bräddningstid (aktiv tid). Inga avloppsklasser.
func sewerCatalog(t *testing.T) (*catalog.Catalog, catalog.ObjectSpec) {
	t.Helper()
	c := catalog.New()
	err := c.PutTemplate(catalog.TemplateSpec{
		Template: things.Template{
			ID: "sewer", Version: "v1",
			Category: "sewer",
			Required: []string{"distance", "level", "overflow", "overflowTime"},
		},
		ParamDefaults: map[string]float64{"sensorToBottom": 2.0, "high": 1.5, "low": 1.4},
		Overridable:   []string{"sensorToBottom", "high", "low"},
		Recipes: []catalog.Recipe{
			{Name: "sewer-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
			{Name: "overflow-state", Operator: "threshold", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"overflow"},
				Params: []string{"high", "low"}},
			{Name: "overflow-time", Operator: "activeTime", Version: "v1",
				Inputs: []string{"overflow"}, Outputs: []string{"overflowTime"}},
		},
	})
	if err != nil {
		t.Fatalf("put sewer: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "sewer-1", Tenant: "t", Name: "Avlopp 1",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "sewer", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "overflow": {}, "overflowTime": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "sewer-sensor", Object: flowStateDistURN, Resource: "5700", Input: "distance"},
		},
	}
	return c, obj
}

func sewerObs(v float64, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID("sewer-sensor", "", flowStateDistURN, "5700"),
		Value:      new(v),
		Unit:       "m",
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

func TestSewerEndToEndWithRestart(t *testing.T) {
	cat, obj := sewerCatalog(t)
	fs := newFakeStore()
	rt := NewRuntime(fs)
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	t2 := base.Add(10 * time.Minute)
	t3 := base.Add(20 * time.Minute)

	// t0: torrt (nivå 1.0 under tröskel 1.5).
	out, err := rt.HandleBatch(context.Background(), "t", "s0", []things.Observation{sewerObs(1.0, base)}, base)
	if err != nil {
		t.Fatalf("t0: %v", err)
	}
	got := out[0].Outcome.Updated
	if *got["level"].Value != 1.0 || *got["overflow"].Value != 0 || *got["overflowTime"].Value != 0 {
		t.Fatalf("t0 dry state wrong: %+v", got)
	}

	// t1: bräddning startar (nivå 1.7).
	out, err = rt.HandleBatch(context.Background(), "t", "s1", []things.Observation{sewerObs(0.3, t1)}, t1)
	if err != nil {
		t.Fatalf("t1: %v", err)
	}
	got = out[0].Outcome.Updated
	if *got["overflow"].Value != 1 || *got["overflowTime"].Value != 0 {
		t.Fatalf("t1 overflow must start at 0: %+v", got)
	}

	// t2: fortsatt bräddning, pågående vy 300 s.
	out, err = rt.HandleBatch(context.Background(), "t", "s2", []things.Observation{sewerObs(0.35, t2)}, t2)
	if err != nil {
		t.Fatalf("t2: %v", err)
	}
	if got := *out[0].Outcome.Updated["overflowTime"].Value; got != 300 {
		t.Fatalf("t2 overflowTime=%v, want 300", got)
	}

	// Omstart mitt i bräddning: nytt runtime över samma store.
	rt2 := NewRuntime(fs)
	out, err = rt2.HandleBatch(context.Background(), "t", "s3", []things.Observation{sewerObs(1.2, t3)}, t3)
	if err != nil {
		t.Fatalf("t3 after restart: %v", err)
	}
	got = out[0].Outcome.Updated
	if *got["level"].Value < 0.79 || *got["level"].Value > 0.81 {
		t.Fatalf("t3 level=%v, want 0.8", *got["level"].Value)
	}
	if *got["overflow"].Value != 0 {
		t.Fatalf("t3 overflow must end: %+v", got)
	}
	if *got["overflowTime"].Value != 900 {
		t.Fatalf("t3 overflowTime=%v, want 900 (survived restart)", *got["overflowTime"].Value)
	}

	pub := &fakePublisher{}
	n, err := rt2.DrainAll(context.Background(), pub, t3)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n == 0 || pub.count() == 0 {
		t.Fatal("expected published batches")
	}
}

// Paramändring mitt i period: ackumulerat bevaras, nya trösklar styr
// fortsättningen.
func TestSewerThresholdChangeKeepsAccumulated(t *testing.T) {
	cat, obj := sewerCatalog(t)
	fs := newFakeStore()
	rt := NewRuntime(fs)
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	if _, err := rt.HandleBatch(context.Background(), "t", "s1", []things.Observation{sewerObs(0.3, t1)}, t1); err != nil {
		t.Fatalf("overflow start: %v", err)
	}
	// Skärp tröskeln under pågående bräddning (rev 1 -> 2).
	obj.ParamOverrides = map[string]float64{"high": 1.8, "low": 1.65}
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	// Nivå 1.7 ligger nu under nya high men över nya low: hysteresen
	// behåller aktivt tillstånd från tidigare runda.
	t2 := base.Add(10 * time.Minute)
	out, err := rt.HandleBatch(context.Background(), "t", "s2", []things.Observation{sewerObs(0.3, t2)}, t2)
	if err != nil {
		t.Fatalf("t2: %v", err)
	}
	if got := *out[0].Outcome.Updated["overflow"].Value; got != 1 {
		t.Fatalf("overflow=%v under nya trösklar, want 1 (hysteresis keeps)", got)
	}
	// Nivå 1.65 <= nya low 1.65: bräddningen avslutas, hela perioden
	// (även före paramändringen) räknas.
	t3 := base.Add(15 * time.Minute)
	out, err = rt.HandleBatch(context.Background(), "t", "s3", []things.Observation{sewerObs(0.35, t3)}, t3)
	if err != nil {
		t.Fatalf("t3: %v", err)
	}
	got := out[0].Outcome.Updated
	if *got["overflow"].Value != 0 {
		t.Fatalf("t3 overflow=%v, want 0 (new low 1.75)", *got["overflow"].Value)
	}
	if *got["overflowTime"].Value != 600 {
		t.Fatalf("t3 overflowTime=%v, want 600 (accumulated across param change)", *got["overflowTime"].Value)
	}
}

// Regression C9/L10: retry med samma messageID får inte dubbelackumulera
// aktiv tid. Samma batch via Runtime ger Duplicate utan omberäkning; slut
// efter dubblett ger fortfarande 900 s, inte 1800.
func TestActiveTimeDuplicateNoDoubleCount(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{ID: "t", Version: "v1", Required: []string{"sig", "run"}, Category: "t"},
		Recipes: []catalog.Recipe{{Name: "run-time", Operator: "activeTime", Version: "v1",
			Inputs: []string{"sig"}, Outputs: []string{"run"}}},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "th", Tenant: "tn", Name: "Test",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "t", TemplateVersion: "v1",
		Properties: map[string]things.Property{"sig": {}, "run": {}},
		Bindings:   []things.Binding{{DeviceID: "s", Object: flowURNtemp, Resource: "5700", Input: "sig"}},
	}
	fs := newFakeStore()
	rt := NewRuntime(fs)
	if _, err := rt.RegisterThing("tn", c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	t3 := base.Add(20 * time.Minute)
	if _, err := rt.HandleBatch(context.Background(), "tn", "m1", []things.Observation{sigObs("s", 1, t1)}, t1); err != nil {
		t.Fatalf("start: %v", err)
	}
	dup, err := rt.HandleBatch(context.Background(), "tn", "m1", []things.Observation{sigObs("s", 1, t1)}, t1)
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if len(dup) != 1 || !dup[0].Duplicate {
		t.Fatalf("duplicate must be flagged: %+v", dup)
	}
	out, err := rt.HandleBatch(context.Background(), "tn", "m2", []things.Observation{sigObs("s", 0, t3)}, t3)
	if err != nil {
		t.Fatalf("end: %v", err)
	}
	if got := *out[0].Outcome.Updated["run"].Value; got != 900 {
		t.Fatalf("run=%v, want 900 (duplicate must not double)", got)
	}
	st, _ := fs.LoadThing("tn", "th")
	if acc := decodeActiveTime(t, st.ProcessorStates["run"]).AccumulatedSec; acc != 900 {
		t.Fatalf("accumulated=%d, want 900", acc)
	}
}

// Regression: samma instant (samma ObservedAt, nytt messageID) får inte
// dubbelackumulera. Direktflöde med samma mättid ger ingen ny current
// (tie-break) och därmed ingen ny tillståndsövergång.
func TestActiveTimeSameInstantNoDouble(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	t3 := base.Add(20 * time.Minute)
	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	out, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, t1)}, t1, "m1", 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	current, proc = mergeCurrent(current, out), mergeProc(proc, out)
	retry, err := EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 1, t1)}, t1, "m1-retry", 1)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(retry.Updated) != 0 || len(retry.ProcStates) != 0 {
		t.Fatalf("same-instant retry must be quiet (no current/state): updated=%v states=%v", retry.Updated, retry.ProcStates)
	}
	out, err = EvaluateBatchFlow(&cfg, current, proc, []things.Observation{sigObs("s", 0, t3)}, t3, "m3", 1)
	if err != nil {
		t.Fatalf("end: %v", err)
	}
	if got := *out.Updated["run"].Value; got != 900 {
		t.Fatalf("run=%v, want 900", got)
	}
}

// Regression: tröskel-skip (saknad/ogiltig ingång) bevarar gammal output
// tyst (B4/B12): inget resultat, inget event, ingen historik för output,
// aktuell står kvar.
func TestThresholdSkipPreservesOutput(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "bench", Version: "v1",
		Category: "bench",
		Required: []string{"soilMoisture", "soilMoistureLow"},
	},
		ParamDefaults: map[string]float64{"high": 30, "low": 20},
		Recipes: []catalog.Recipe{
			{Name: "dry", Operator: "threshold", Version: "v1",
				Inputs: []string{"soilMoisture"}, Outputs: []string{"soilMoistureLow"},
				Params: []string{"high", "low"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg := mustCompileBench(t, c)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	out, err := EvaluateBatchFlow(cfg, current, nil, []things.Observation{{
		Signal: things.ParseSignalID("s", "", flowURNtemp, "5700"), Value: new(float64(35)),
		Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base,
	}}, base, "m1", 1)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	current = mergeCurrent(current, out)
	if got := *current["soilMoistureLow"].Value; got != 1 {
		t.Fatalf("init low=%v, want 1", got)
	}
	bad := things.Observation{
		Signal: things.ParseSignalID("s", "", flowURNtemp, "5700"), Value: new(float64(0)),
		Quality: things.QualityInvalid, ObservedAt: base.Add(time.Minute), ReceivedAt: base.Add(time.Minute),
	}
	skipped, err := EvaluateBatchFlow(cfg, current, nil, []things.Observation{bad}, base.Add(time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("skip: %v", err)
	}
	if len(skipped.Updated) != 0 {
		t.Fatalf("skip must not update current: %+v", skipped.Updated)
	}
	for _, e := range skipped.Events {
		if e.PropertyID == "soilMoistureLow" {
			t.Fatalf("skip must not emit event for output: %+v", e)
		}
	}
	for _, h := range skipped.History {
		if h.PropertyID == "soilMoistureLow" {
			t.Fatalf("skip must not write history for output: %+v", h)
		}
	}
	if got := *current["soilMoistureLow"].Value; got != 1 {
		t.Fatalf("retained low=%v, want 1 (old output sticks)", got)
	}
}

// Regression C/L: outcome/store/anropare får ej dela StateJSON-array.
// Muterad outcome-blob får inte påverka lagrat tillstånd och vice versa.
func TestProcStateNoAliasing(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	out, err := EvaluateBatchFlow(&cfg, map[string]CurrentValue{}, map[string]StoredProcessor{}, []things.Observation{sigObs("s", 1, t1)}, t1, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	merged := mergeProc(map[string]StoredProcessor{}, out)
	if len(out.ProcStates["run"].StateJSON) == 0 || len(merged["run"].StateJSON) == 0 {
		t.Fatal("missing state JSON")
	}
	saved := out.ProcStates["run"].StateJSON[0]
	merged["run"].StateJSON[0] ^= 0xFF
	if out.ProcStates["run"].StateJSON[0] != saved {
		t.Fatal("mergeProc shares StateJSON array with outcome (aliasing)")
	}
	// Runtime-lagring: muterad LoadThing-kopia får inte påverka storet.
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{ID: "t", Version: "v1", Required: []string{"sig", "run"}, Category: "t"},
		Recipes: []catalog.Recipe{{Name: "run-time", Operator: "activeTime", Version: "v1",
			Inputs: []string{"sig"}, Outputs: []string{"run"}}},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "th", Tenant: "tn", Name: "Test",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "t", TemplateVersion: "v1",
		Properties: map[string]things.Property{"sig": {}, "run": {}},
		Bindings:   []things.Binding{{DeviceID: "s", Object: flowURNtemp, Resource: "5700", Input: "sig"}},
	}
	fs := newFakeStore()
	rt := NewRuntime(fs)
	if _, err := rt.RegisterThing("tn", c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.HandleBatch(context.Background(), "tn", "m1", []things.Observation{sigObs("s", 1, t1)}, t1); err != nil {
		t.Fatalf("batch: %v", err)
	}
	st1, _ := fs.LoadThing("tn", "th")
	origJSON := append([]byte(nil), st1.ProcessorStates["run"].StateJSON...)
	st1.ProcessorStates["run"].StateJSON[0] ^= 0xFF
	st1.ProcessorStates["evil"] = StoredProcessor{Operator: "x"}
	st2, _ := fs.LoadThing("tn", "th")
	if _, ok := st2.ProcessorStates["evil"]; ok {
		t.Fatal("LoadThing shares processor map with store (aliasing)")
	}
	if string(st2.ProcessorStates["run"].StateJSON) != string(origJSON) {
		t.Fatal("LoadThing shares StateJSON array with store (aliasing): store corrupted via loaded copy")
	}
	st3, _ := fs.LoadThing("tn", "th")
	orig := decodeActiveTime(t, st3.ProcessorStates["run"])
	if !orig.Active || orig.StartUnix != t1.Unix() {
		t.Fatalf("store corrupted via loaded copy: %+v", orig)
	}
}

// Regression C4/S3: icke-positiv höjd/avstånd nekas statiskt varje runda,
// även oberörd (ingen tyst acceptans som först fallerar vid beröring).
func TestStaticRejectsNonPositiveParams(t *testing.T) {
	mk := func(op, param string, v float64) catalog.EffectiveConfig {
		return catalog.EffectiveConfig{
			Properties:  map[string]things.Property{"p": {}, "q": {}},
			ParamValues: map[string]float64{param: v},
			Recipes:     []catalog.Recipe{{Name: "r", Operator: op, Version: "v1", Inputs: []string{"p"}, Outputs: []string{"q"}}},
		}
	}
	for _, tc := range []struct {
		op, param string
		v         float64
	}{
		{"distanceToLevel", "sensorToBottom", 0},
		{"distanceToLevel", "sensorToBottom", -1},
		{"level", "sensorToBottom", 0},
		{"fillRate", "usableHeight", 0},
		{"fillRate", "usableHeight", -0.5},
		{"percent", "usableHeight", 0},
	} {
		cfg := mk(tc.op, tc.param, tc.v)
		if err := validateRecipeStatic(&cfg, cfg.Recipes[0]); err == nil {
			t.Fatalf("%s %s=%v must fail static validation", tc.op, tc.param, tc.v)
		}
		current := map[string]CurrentValue{"p": {Value: new(float64(1)), Quality: "ok", ObservedAt: time.Now().UTC()}}
		if _, err := EvaluateBatchFlow(&cfg, current, nil, nil, time.Now().UTC(), "m", 1); err == nil {
			t.Fatalf("%s %s=%v must fail even when untouched", tc.op, tc.param, tc.v)
		}
	}
}

// Regression K2: nivårecept med fler än ett bottenavstånd nekas statiskt
// (extra parametrar signalerar geometri som inte hedras). Vinkelparametern
// angle nekas vid publicering/kompilering och når aldrig flödet.
func TestStaticRejectsLevelExtraParams(t *testing.T) {
	cfg := catalog.EffectiveConfig{
		Properties:  map[string]things.Property{"p": {}, "q": {}},
		ParamValues: map[string]float64{"sensorToBottom": 1.5, "usableHeight": 1.0},
		Recipes: []catalog.Recipe{{Name: "r", Operator: "level", Version: "v1",
			Inputs: []string{"p"}, Outputs: []string{"q"},
			Params: []string{"sensorToBottom", "usableHeight"}}},
	}
	err := validateRecipeStatic(&cfg, cfg.Recipes[0])
	if err == nil {
		t.Fatal("level recipe with 2 params must fail static validation")
	}
	if !strings.Contains(err.Error(), "vertical-down") {
		t.Fatalf("error should name the supported geometry, got %q", err)
	}
}

// Regression C2: självloop (recept som läser sin egen utgång) nekas som cykel.
func TestSelfLoopRejected(t *testing.T) {
	recs := []catalog.Recipe{
		{Name: "self", Operator: "passthrough", Version: "v1", Inputs: []string{"a"}, Outputs: []string{"a"}},
	}
	if _, err := orderRecipes(recs); err == nil {
		t.Fatal("self-loop must be rejected as cycle")
	}
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "loop", Version: "v1", Required: []string{"a"}, Category: "loop",
	},
		Recipes: recs,
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "loop-1", Tenant: "t", Name: "Loop",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "loop", TemplateVersion: "v1",
		Properties: map[string]things.Property{"a": {}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := EvaluateBatchFlow(cfg, map[string]CurrentValue{
		"a": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
	}, nil, nil, base, "m", 1); err == nil {
		t.Fatal("self-loop flow must fail")
	}
}

// Regression: aliasoperatorer (activeTime/active_time, threshold/hysteresis)
// delar tillstånd utan spurious reset; nil proc-karta läses säkert första rundan.
func TestOperatorAliasNoSpuriousReset(t *testing.T) {
	cfg := activeTimeCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	t1 := base.Add(5 * time.Minute)
	// Första rundan med nil proc-karta (ej tom karta) ska fungera.
	out, err := EvaluateBatchFlow(&cfg, map[string]CurrentValue{}, nil, []things.Observation{sigObs("s", 1, t1)}, t1, "m1", 1)
	if err != nil {
		t.Fatalf("nil proc first round: %v", err)
	}
	proc := mergeProc(nil, out)
	current := mergeCurrent(nil, out)
	cfg2 := cfg
	cfg2.Recipes[0].Operator = "active_time"
	out2, err := EvaluateBatchFlow(&cfg2, current, proc, []things.Observation{sigObs("s", 1, t1.Add(time.Minute))}, t1.Add(time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("alias round: %v", err)
	}
	if q := out2.Updated["run"].Quality; q == "uncertain" {
		t.Fatalf("alias activeTime->active_time caused spurious versionReset (quality uncertain)")
	}
	if got := *out2.Updated["run"].Value; got != 60 {
		t.Fatalf("run=%v, want 60 ongoing (no reset)", got)
	}
	// threshold/hysteresis-alias ger samma resultat.
	mk := func(op string) catalog.EffectiveConfig {
		return catalog.EffectiveConfig{
			Properties:  map[string]things.Property{"p": {}, "q": {}},
			ParamValues: map[string]float64{"high": 30, "low": 20},
			Recipes:     []catalog.Recipe{{Name: "r", Operator: op, Version: "v1", Inputs: []string{"p"}, Outputs: []string{"q"}}},
		}
	}
	for _, op := range []string{"threshold", "hysteresis"} {
		cc := mk(op)
		if err := validateRecipeStatic(&cc, cc.Recipes[0]); err != nil {
			t.Fatalf("%s static: %v", op, err)
		}
	}
}

func counterCfg() catalog.EffectiveConfig {
	return catalog.EffectiveConfig{
		Properties: map[string]things.Property{"door": {}, "passages": {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: flowURNpresence, Resource: "5500", Input: "door"},
		},
		Recipes: []catalog.Recipe{
			{Name: "count-passages", Operator: "counter", Version: "v1",
				Inputs: []string{"door"}, Outputs: []string{"passages"}},
		},
	}
}

// Livscykel: första aktiva räknas, upprepad aktiv räknas inte, ny flank
// efter inaktiv räknas igen. Tillståndet är versionerat och överlever
// omstart via ProcStates utan dubbelräkning.
func TestCounterLifecycle(t *testing.T) {
	cfg := counterCfg()
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	round := func(v bool, at time.Time, id string) *FlowOutcome {
		t.Helper()
		out, err := EvaluateBatchFlow(&cfg, current, proc,
			[]things.Observation{boolObs("s", flowURNpresence, "5500", v, at)}, at, id, 1)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		current, proc = mergeCurrent(current, out), mergeProc(proc, out)
		return out
	}

	if out := round(false, base, "m0"); *out.Updated["passages"].Value != 0 {
		t.Fatalf("t0 passages=%v, want 0", *out.Updated["passages"].Value)
	}
	if out := round(true, base.Add(time.Minute), "m1"); *out.Updated["passages"].Value != 1 {
		t.Fatalf("t1 passages=%v, want 1", *out.Updated["passages"].Value)
	}
	if out := round(true, base.Add(2*time.Minute), "m2"); *out.Updated["passages"].Value != 1 {
		t.Fatalf("t2 passages=%v, want still 1", *out.Updated["passages"].Value)
	}
	if out := round(false, base.Add(3*time.Minute), "m3"); *out.Updated["passages"].Value != 1 {
		t.Fatalf("t3 passages=%v, want still 1", *out.Updated["passages"].Value)
	}
	if out := round(true, base.Add(4*time.Minute), "m4"); *out.Updated["passages"].Value != 2 {
		t.Fatalf("t4 passages=%v, want 2", *out.Updated["passages"].Value)
	}

	var st functions.CounterState
	if err := json.Unmarshal(proc["passages"].StateJSON, &st); err != nil {
		t.Fatalf("decode counter state: %v", err)
	}
	if st.Count != 2 || !st.LastActive {
		t.Fatalf("persisted state=%+v, want {2 true}", st)
	}

	// Versionsbyte nollställer och flaggar uncertain i stället för att
	// tyst återanvända främmande tillstånd.
	cfg2 := counterCfg()
	cfg2.Recipes[0].Version = "v2"
	out, err := EvaluateBatchFlow(&cfg2, current, proc,
		[]things.Observation{boolObs("s", flowURNpresence, "5500", true, base.Add(5*time.Minute))},
		base.Add(5*time.Minute), "m5", 1)
	if err != nil {
		t.Fatalf("v2 round: %v", err)
	}
	if q := out.Updated["passages"].Quality; q != "uncertain" {
		t.Fatalf("version reset quality=%q, want uncertain", q)
	}
}

func meterCfg(op string) catalog.EffectiveConfig {
	out := "consumption"
	if op == "sum" {
		out = "total"
	}
	return catalog.EffectiveConfig{
		Properties: map[string]things.Property{"volume": {}, out: {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: flowURNtemp, Resource: "5700", Input: "volume"},
		},
		Recipes: []catalog.Recipe{
			{Name: "meter-" + op, Operator: op, Version: "v1",
				Inputs: []string{"volume"}, Outputs: []string{out}},
		},
	}
}

// Delta: första rundan seedar utan resultat, sedan differenser
// (även negativa). Tillståndet överlever omstart via ProcStates.
func TestDeltaLifecycle(t *testing.T) {
	cfg := meterCfg("delta")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	round := func(v float64, at time.Time, id string) *FlowOutcome {
		t.Helper()
		out, err := EvaluateBatchFlow(&cfg, current, proc,
			[]things.Observation{sigObs("s", v, at)}, at, id, 1)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		current, proc = mergeCurrent(current, out), mergeProc(proc, out)
		return out
	}

	if out := round(12.5, base, "m0"); len(out.Updated) != 1 {
		t.Fatalf("seed updated=%+v, want only direct volume", out.Updated)
	} else if _, ok := out.Updated["consumption"]; ok {
		t.Fatal("seed must not produce consumption")
	}
	if out := round(13.0, base.Add(time.Minute), "m1"); *out.Updated["consumption"].Value != 0.5 {
		t.Fatalf("consumption=%v, want 0.5", *out.Updated["consumption"].Value)
	}
	if out := round(12.0, base.Add(2*time.Minute), "m2"); *out.Updated["consumption"].Value != -1.0 {
		t.Fatalf("consumption=%v, want -1.0", *out.Updated["consumption"].Value)
	}

	var st functions.DeltaState
	if err := json.Unmarshal(proc["consumption"].StateJSON, &st); err != nil {
		t.Fatalf("decode delta state: %v", err)
	}
	if !st.HasLast || st.LastValue != 12.0 {
		t.Fatalf("persisted state=%+v, want {true 12.0}", st)
	}
}

// Summa: löpande total över rapporter, bevarad över omstart.
func TestSumLifecycle(t *testing.T) {
	cfg := meterCfg("sum")
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	current := map[string]CurrentValue{}
	proc := map[string]StoredProcessor{}
	total := 0.0
	for i, v := range []float64{0.2, 0.2, 0.5} {
		at := base.Add(time.Duration(i) * time.Minute)
		out, err := EvaluateBatchFlow(&cfg, current, proc,
			[]things.Observation{sigObs("s", v, at)}, at, "m", 1)
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		total += v
		current, proc = mergeCurrent(current, out), mergeProc(proc, out)
		if got := *out.Updated["total"].Value; got < total-1e-9 || got > total+1e-9 {
			t.Fatalf("round %d total=%v, want %v", i, got, total)
		}
	}

	var st functions.SumState
	if err := json.Unmarshal(proc["total"].StateJSON, &st); err != nil {
		t.Fatalf("decode sum state: %v", err)
	}
	if st.Total < 0.899 || st.Total > 0.901 {
		t.Fatalf("persisted total=%v, want ~0.9", st.Total)
	}
}
