package application

import (
	"os"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

const (
	flowURNdistance = "urn:oma:lwm2m:ext:3330"
	flowURNtemp     = "urn:oma:lwm2m:ext:3303"
	flowURNpresence = "urn:oma:lwm2m:ext:3200"
)

func flowBins(t *testing.T) (*catalog.Catalog, catalog.EffectiveConfig) {
	t.Helper()
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required: []string{"distance", "level", "fillRate"},
			Category: "wastebin",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
		Recipes: []catalog.Recipe{
			// Avsiktligt omvänd ordning: flödet ska ändå köra nivå före grad.
			{Name: "fill-rate", Operator: "fillRate", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"fillRate"},
				Params: []string{"usableHeight"}},
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put wastebin: %v", err)
	}
	if err := c.PutVariant(catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 1.2},
	}}); err != nil {
		t.Fatalf("put variant: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "milesight:214", Object: flowURNdistance, Resource: "5700", Input: "distance"},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	})
	if err != nil {
		t.Fatalf("compile bin-42: %v", err)
	}
	return c, *cfg
}

func flowRooms(t *testing.T) (*catalog.Catalog, catalog.EffectiveConfig) {
	t.Helper()
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "room", Version: "v1",
			RepeatGroups: []string{"temperature."},
			Optional:     []string{"temperature.average"},
			Category:     "room",
		},
		Recipes: []catalog.Recipe{
			{Name: "mean-temp", Operator: "mean", Version: "v1",
				Inputs:  []string{"temperature.window", "temperature.center", "temperature.door"},
				Outputs: []string{"temperature.average"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put room: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Rum A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {}, "temperature.center": {},
			"temperature.door": {}, "temperature.average": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNtemp, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s2", Object: flowURNtemp, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "s3", Object: flowURNtemp, Resource: "5700", Input: "temperature.door"},
		},
	})
	if err != nil {
		t.Fatalf("compile room-a: %v", err)
	}
	return c, *cfg
}

func tempObs(device string, v float64, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID(device, "", flowURNtemp, "5700"),
		Value:      new(v),
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

func boolObs(device, object, resource string, v bool, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID(device, "", object, resource),
		BoolValue:  &v,
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

func mergeCurrent(base map[string]CurrentValue, out *FlowOutcome) map[string]CurrentValue {
	next := make(map[string]CurrentValue, len(base)+len(out.Updated))
	for k, v := range base {
		if v.Value != nil {
			c := *v.Value
			v.Value = &c
		}
		next[k] = v
	}
	for k, v := range out.Updated {
		if v.Value != nil {
			c := *v.Value
			v.Value = &c
		}
		next[k] = v
	}
	return next
}

func TestBinChainLevelAndFillRate(t *testing.T) {
	_, cfg := flowBins(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{{
		Signal:     things.ParseSignalID("milesight:214", "", flowURNdistance, "5700"),
		Value:      new(0.30),
		Quality:    things.QualityOK,
		ObservedAt: base,
		ReceivedAt: base,
	}}, base, "m1", 7)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	level := out.Updated["level"]
	fill := out.Updated["fillRate"]
	if level.Value == nil || *level.Value < 1.119 || *level.Value > 1.121 {
		t.Fatalf("level=%v, want 1.12", level.Value)
	}
	want := 100 * 1.12 / 1.2
	if fill.Value == nil || *fill.Value < want-0.01 || *fill.Value > want+0.01 {
		t.Fatalf("fillRate=%v, want %v", fill.Value, want)
	}
	if len(out.Events) != 3 {
		t.Fatalf("events=%d, want 3 (distance+level+fillRate)", len(out.Events))
	}
	for _, e := range out.Events {
		if e.ConfigRevision != 7 || e.MessageID != "m1" {
			t.Fatalf("event missing provenance: %+v", e)
		}
	}
}

func TestRoomMeanAcrossBatches(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	steps := []struct {
		device string
		value  float64
		minute int
	}{
		{"s1", 18, 0}, {"s2", 21, 1}, {"s3", 24, 2},
	}
	var out *FlowOutcome
	for i, s := range steps {
		at := base.Add(time.Duration(s.minute) * time.Minute)
		var err error
		out, err = EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs(s.device, s.value, at)}, at, "m", 1)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		current = mergeCurrent(current, out)
	}
	avg, ok := current["temperature.average"]
	if !ok || avg.Value == nil || *avg.Value != 21 {
		t.Fatalf("average=%+v, want 21", avg)
	}
	at := base.Add(3 * time.Minute)
	out, err := EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s3", 27, at)}, at, "m", 1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if out.Updated["temperature.average"].Value == nil || *out.Updated["temperature.average"].Value != 22 {
		t.Fatalf("average=%+v, want 22", out.Updated["temperature.average"])
	}
}

func TestBatchSingleMeanEvent(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 21, base.Add(time.Minute)),
		tempObs("s3", 24, base.Add(2*time.Minute)),
	}, base.Add(3*time.Minute), "m-batch", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	n := 0
	for _, e := range out.Events {
		if e.PropertyID == "temperature.average" {
			n++
			if e.Available != 3 || e.Expected != 3 {
				t.Fatalf("mean provenance=%+v, want 3/3", e)
			}
		}
	}
	if n != 1 {
		t.Fatalf("mean events=%d, want exactly 1 per batch", n)
	}
}

func TestLateObservationSavedNotCurrent(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	out, err := EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s1", 30, base.Add(10*time.Minute))}, base.Add(10*time.Minute), "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	current = mergeCurrent(current, out)
	out, err = EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s1", 10, base)}, base.Add(11*time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("late flow: %v", err)
	}
	if got := *current["temperature.window"].Value; got != 30 {
		t.Fatalf("current=%v, want 30 (late must not win)", got)
	}
	if len(out.Updated) != 0 {
		t.Fatalf("late must not update current: %+v", out.Updated)
	}
	late := 0
	for _, h := range out.History {
		if h.Late {
			late++
		}
	}
	if late != 1 {
		t.Fatalf("late history writes=%d, want 1", late)
	}
}

func TestInvalidNeverReplaces(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	out, err := EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s1", 30, base)}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	current = mergeCurrent(current, out)
	bad := tempObs("s1", 0, base.Add(time.Minute))
	bad.Quality = things.QualityInvalid
	out, err = EvaluateBatchFlow(&cfg, current, nil, []things.Observation{bad}, base.Add(time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("invalid flow: %v", err)
	}
	if got := *current["temperature.window"].Value; got != 30 {
		t.Fatalf("current=%v, want 30", got)
	}
	if len(out.History) != 1 || out.History[0].Quality != "invalid" {
		t.Fatalf("invalid must still be recorded in history: %+v", out.History)
	}
}

func TestBoolIngestDirect(t *testing.T) {
	cfg := catalog.EffectiveConfig{
		Properties: map[string]things.Property{"presence": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNpresence, Resource: "5500", Input: "presence"},
		},
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

	out, err := EvaluateBatchFlow(&cfg, nil, nil,
		[]things.Observation{boolObs("s1", flowURNpresence, "5500", true, base)}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	cur, ok := out.Updated["presence"]
	if !ok || cur.Value == nil || *cur.Value != 1 {
		t.Fatalf("presence=%+v, want current 1", out.Updated)
	}
	if len(out.History) != 1 || out.History[0].Value == nil || *out.History[0].Value != 1 ||
		out.History[0].Quality != "ok" {
		t.Fatalf("history=%+v, want one ok row with 1", out.History)
	}
	if len(out.Events) != 1 || out.Events[0].Value != 1 {
		t.Fatalf("events=%+v, want one event with 1", out.Events)
	}

	current := mergeCurrent(nil, out)
	later := base.Add(time.Minute)
	out, err = EvaluateBatchFlow(&cfg, current, nil,
		[]things.Observation{boolObs("s1", flowURNpresence, "5500", false, later)}, later, "m2", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if got := *out.Updated["presence"].Value; got != 0 {
		t.Fatalf("presence=%v, want current 0", got)
	}
}

func TestMissingInputSkipsMean(t *testing.T) {

	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base), tempObs("s2", 21, base),
	}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if _, ok := out.Updated["temperature.average"]; ok {
		t.Fatal("mean without all inputs must not produce a result")
	}
	for _, e := range out.Events {
		if e.PropertyID == "temperature.average" {
			t.Fatal("mean without all inputs must not emit an event")
		}
	}
	if len(out.Updated) != 2 {
		t.Fatalf("direct values must still be recorded: %+v", out.Updated)
	}
}

func TestUnboundSignalIgnored(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	obs := tempObs("unknown-sensor", 99, base)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{obs}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if len(out.Updated) != 0 || len(out.History) != 0 || len(out.Events) != 0 {
		t.Fatalf("unbound signal must be ignored: %+v", out)
	}
}

func TestCycleDetected(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "loop", Version: "v1",
			Required: []string{"a", "b"},
			Category: "loop",
		},
		Recipes: []catalog.Recipe{
			{Name: "r1", Operator: "passthrough", Version: "v1", Inputs: []string{"b"}, Outputs: []string{"a"}},
			{Name: "r2", Operator: "passthrough", Version: "v1", Inputs: []string{"a"}, Outputs: []string{"b"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "loop-1", Tenant: "t", Name: "Loop",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "loop", TemplateVersion: "v1",
		Properties: map[string]things.Property{"a": {}, "b": {}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := EvaluateBatchFlow(cfg, map[string]CurrentValue{
		"a": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
	}, nil, nil, base, "m", 1); err == nil {
		t.Fatal("cyclic graph must be rejected")
	}
}

func TestUnknownOperatorRejected(t *testing.T) {
	mk := func(op string) catalog.EffectiveConfig {
		return catalog.EffectiveConfig{
			Properties: map[string]things.Property{"p": {}, "q": {}},
			Recipes: []catalog.Recipe{
				{Name: "r", Operator: op, Version: "v1", Inputs: []string{"p"}, Outputs: []string{"q"}},
			},
		}
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{
		"p": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
	}
	for _, op := range []string{"noSuchOperator", "active_time_typo"} {
		cfg := mk(op)
		if _, err := EvaluateBatchFlow(&cfg, current, nil, nil, base, "m", 1); err == nil {
			t.Fatalf("operator %q must fail", op)
		}
	}
	// activeTime körs sedan Etapp 5 (se TestActiveTimeLifecycle).
}

func TestUnitMismatchRecordedInvalid(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1",
		Category:     "room",
		RepeatGroups: []string{"temperature."},
	}}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "room-u", Tenant: "t", Name: "Rum U",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {ID: "temperature.window", Unit: "°C"},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNtemp, Resource: "5700", Input: "temperature.window"},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	obs := tempObs("s1", 70, base)
	obs.Unit = "°F"
	out, err := EvaluateBatchFlow(cfg, nil, nil, []things.Observation{obs}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if len(out.Updated) != 0 {
		t.Fatalf("unit mismatch must not become current: %+v", out.Updated)
	}
	if len(out.History) != 1 || out.History[0].Quality != "invalid" {
		t.Fatalf("unit mismatch must be recorded invalid: %+v", out.History)
	}
}

func TestThresholdHysteresisStable(t *testing.T) {
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
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	for i, tc := range []struct {
		value float64
		want  float64
	}{
		{35, 1}, {25, 1}, {15, 0}, {25, 0}, {35, 1},
	} {
		at := base.Add(time.Duration(i) * time.Minute)
		out, err := EvaluateBatchFlow(mustCompileBench(t, c), current, nil, []things.Observation{{
			Signal:     things.ParseSignalID("s", "", flowURNtemp, "5700"),
			Value:      new(tc.value),
			Quality:    things.QualityOK,
			ObservedAt: at,
			ReceivedAt: at,
		}}, at, "m", 1)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		current = mergeCurrent(current, out)
		got := current["soilMoistureLow"]
		if got.Value == nil || *got.Value != tc.want {
			t.Fatalf("step %d value=%v: state=%+v, want %v", i, tc.value, got, tc.want)
		}
	}
}

func mustCompileBench(t *testing.T, c *catalog.Catalog) *catalog.EffectiveConfig {
	t.Helper()
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "bench-1", Tenant: "t", Name: "Bänk",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "bench", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"soilMoisture": {}, "soilMoistureLow": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: flowURNtemp, Resource: "5700", Input: "soilMoisture"},
		},
	})
	if err != nil {
		t.Fatalf("compile bench: %v", err)
	}
	return cfg
}

func TestFillRateOutOfRangeKeptUncertain(t *testing.T) {
	_, cfg := flowBins(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{{
		Signal:     things.ParseSignalID("milesight:214", "", flowURNdistance, "5700"),
		Value:      new(2.0),
		Quality:    things.QualityOK,
		ObservedAt: base,
		ReceivedAt: base,
	}}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	fill, ok := out.Updated["fillRate"]
	if !ok || fill.Value == nil {
		t.Fatalf("out-of-range fillRate must be kept: %+v", out.Updated)
	}
	if *fill.Value >= 0 {
		t.Fatalf("fillRate=%v, want negative (kept, not clipped)", *fill.Value)
	}
	if fill.Quality != "uncertain" {
		t.Fatalf("fillRate quality=%q, want uncertain", fill.Quality)
	}
}

func flowSingleTempTwoSources(t *testing.T) catalog.EffectiveConfig {
	t.Helper()
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "single", Version: "v1",
		Required: []string{"temp"},
		Category: "single",
	}}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "single-1", Tenant: "t", Name: "Singel",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "single", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temp": {}},
		Bindings: []things.Binding{
			{DeviceID: "sA", Object: flowURNtemp, Resource: "5700", Input: "temp"},
			{DeviceID: "sB", Object: flowURNtemp, Resource: "5700", Input: "temp"},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return *cfg
}

func countEvents(out *FlowOutcome, prop string) int {
	n := 0
	for _, e := range out.Events {
		if e.PropertyID == prop {
			n++
		}
	}
	return n
}

// B13 tie-break: lika ObservedAt, senare ReceivedAt vinner. Samma batch i
// båda ordningarna ska ge samma vinnare med exakt ett event (B14).
func TestTieBreakReceivedAtWins(t *testing.T) {
	cfg := flowSingleTempTwoSources(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkBatch := func() []things.Observation {
		return []things.Observation{
			{
				Signal:     things.ParseSignalID("sA", "", flowURNtemp, "5700"),
				Value:      new(float64(10)),
				Quality:    things.QualityOK,
				ObservedAt: base,
				ReceivedAt: base,
			},
			{
				Signal:     things.ParseSignalID("sB", "", flowURNtemp, "5700"),
				Value:      new(float64(20)),
				Quality:    things.QualityOK,
				ObservedAt: base,
				ReceivedAt: base.Add(time.Minute),
			},
		}
	}
	orders := [][]things.Observation{
		mkBatch(),
		{mkBatch()[1], mkBatch()[0]},
	}
	for i, batch := range orders {
		out, err := EvaluateBatchFlow(&cfg, nil, nil, batch, base.Add(2*time.Minute), "m", 1)
		if err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
		got, ok := out.Updated["temp"]
		if !ok || got.Value == nil || *got.Value != 20 {
			t.Fatalf("order %d: current=%+v, want 20 (later ReceivedAt wins)", i, got)
		}
		if n := countEvents(out, "temp"); n != 1 {
			t.Fatalf("order %d: temp events=%d, want exactly 1", i, n)
		}
		if len(out.History) != 2 {
			t.Fatalf("order %d: history=%d, want 2", i, len(out.History))
		}
		late := 0
		for _, h := range out.History {
			if h.Late {
				late++
			}
		}
		if late != 1 {
			t.Fatalf("order %d: late history=%d, want 1", i, late)
		}
	}
}

// B13 tie-break: lika ObservedAt+ReceivedAt, större käll-ID vinner.
func TestTieBreakSourceIDWins(t *testing.T) {
	cfg := flowSingleTempTwoSources(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkBatch := func() []things.Observation {
		return []things.Observation{
			{
				Signal:     things.ParseSignalID("sA", "", flowURNtemp, "5700"),
				Value:      new(float64(10)),
				Quality:    things.QualityOK,
				ObservedAt: base,
				ReceivedAt: base,
			},
			{
				Signal:     things.ParseSignalID("sB", "", flowURNtemp, "5700"),
				Value:      new(float64(20)),
				Quality:    things.QualityOK,
				ObservedAt: base,
				ReceivedAt: base,
			},
		}
	}
	// sB/3303/5700 > sA/3303/5700 i strängordning, alltså ska sB vinna.
	for i, batch := range [][]things.Observation{mkBatch(), {mkBatch()[1], mkBatch()[0]}} {
		out, err := EvaluateBatchFlow(&cfg, nil, nil, batch, base, "m", 1)
		if err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
		got := out.Updated["temp"]
		if got.Value == nil || *got.Value != 20 {
			t.Fatalf("order %d: current=%+v, want 20 (larger source wins)", i, got)
		}
		if n := countEvents(out, "temp"); n != 1 {
			t.Fatalf("order %d: temp events=%d, want 1", i, n)
		}
	}
}

// B14: godtycklig ordning inom rapporten ger samma resultat. Samma batch
// fram- och baklänges ska ge identiska Updated/Events och Late-flaggor.
func TestBatchOrderDeterministic(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	fwd := []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 21, base.Add(time.Minute)),
		tempObs("s3", 24, base.Add(2*time.Minute)),
	}
	rev := []things.Observation{fwd[2], fwd[1], fwd[0]}
	outF, err := EvaluateBatchFlow(&cfg, nil, nil, fwd, base.Add(3*time.Minute), "m", 1)
	if err != nil {
		t.Fatalf("fwd: %v", err)
	}
	outR, err := EvaluateBatchFlow(&cfg, nil, nil, rev, base.Add(3*time.Minute), "m", 1)
	if err != nil {
		t.Fatalf("rev: %v", err)
	}
	for k, vf := range outF.Updated {
		vr, ok := outR.Updated[k]
		if !ok || vr.Value == nil || vf.Value == nil || *vr.Value != *vf.Value {
			t.Fatalf("updated %q differs: fwd=%+v rev=%+v", k, vf, vr)
		}
		if vf.Quality != vr.Quality || !vf.ObservedAt.Equal(vr.ObservedAt) {
			t.Fatalf("updated %q metadata differs: fwd=%+v rev=%+v", k, vf, vr)
		}
	}
	if len(outF.Updated) != len(outR.Updated) {
		t.Fatalf("updated size differs: %d vs %d", len(outF.Updated), len(outR.Updated))
	}
	if len(outF.Events) != len(outR.Events) {
		t.Fatalf("events size differs: %d vs %d", len(outF.Events), len(outR.Events))
	}
	for i := range outF.Events {
		a, b := outF.Events[i], outR.Events[i]
		if a.PropertyID != b.PropertyID || a.Value != b.Value || !a.ObservedAt.Equal(b.ObservedAt) {
			t.Fatalf("event %d differs: %+v vs %+v", i, a, b)
		}
	}
	lateF, lateR := 0, 0
	for _, h := range outF.History {
		if h.Late {
			lateF++
		}
	}
	for _, h := range outR.History {
		if h.Late {
			lateR++
		}
	}
	if lateF != lateR {
		t.Fatalf("late count differs: %d vs %d", lateF, lateR)
	}
}

// B14: flera observationer för samma egenskap i en batch ger ett event
// (vinnaren) och historik för samtliga, oavsett inflödesordning.
func TestBatchSamePropertySingleEvent(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkBatch := func() []things.Observation {
		return []things.Observation{
			tempObs("s1", 10, base),
			tempObs("s1", 30, base.Add(10*time.Minute)),
		}
	}
	for i, batch := range [][]things.Observation{mkBatch(), {mkBatch()[1], mkBatch()[0]}} {
		out, err := EvaluateBatchFlow(&cfg, nil, nil, batch, base.Add(11*time.Minute), "m", 1)
		if err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
		got := out.Updated["temperature.window"]
		if got.Value == nil || *got.Value != 30 {
			t.Fatalf("order %d: current=%+v, want 30", i, got)
		}
		if n := countEvents(out, "temperature.window"); n != 1 {
			t.Fatalf("order %d: window events=%d, want 1", i, n)
		}
		if len(out.History) != 2 {
			t.Fatalf("order %d: history=%d, want 2", i, len(out.History))
		}
		late := 0
		for _, h := range out.History {
			if h.Late {
				late++
			}
		}
		if late != 1 {
			t.Fatalf("order %d: late=%d, want 1", i, late)
		}
	}
}

// L3/B13: sen observation ger historik utan event och utan current.
func TestLateGivesHistoryWithoutEvent(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{}
	out, err := EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s1", 30, base.Add(10*time.Minute))}, base.Add(10*time.Minute), "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	current = mergeCurrent(current, out)
	out, err = EvaluateBatchFlow(&cfg, current, nil, []things.Observation{tempObs("s1", 10, base)}, base.Add(11*time.Minute), "m2", 1)
	if err != nil {
		t.Fatalf("late: %v", err)
	}
	if len(out.Updated) != 0 {
		t.Fatalf("late must not update current: %+v", out.Updated)
	}
	if len(out.Events) != 0 {
		t.Fatalf("late must not emit events: %+v", out.Events)
	}
	if len(out.History) != 1 || !out.History[0].Late {
		t.Fatalf("late must give exactly one Late history write: %+v", out.History)
	}
}

// B4/B12: ogiltig ingång ger inget medelresultat och inget event.
func TestInvalidInputSkipsMean(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	bad := tempObs("s1", 0, base)
	bad.Quality = things.QualityInvalid
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		bad,
		tempObs("s2", 21, base),
		tempObs("s3", 24, base),
	}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if _, ok := out.Updated["temperature.average"]; ok {
		t.Fatalf("mean with invalid input must not produce a result: %+v", out.Updated)
	}
	for _, e := range out.Events {
		if e.PropertyID == "temperature.average" {
			t.Fatalf("mean with invalid input must not emit event: %+v", e)
		}
	}
	for _, h := range out.History {
		if h.PropertyID == "temperature.average" {
			t.Fatalf("mean with invalid input must not write history: %+v", h)
		}
	}
}

// C7: uncertain på någon ingång ger uncertain på medelresultatet.
func TestUncertainPropagatesThroughMean(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	u := tempObs("s1", 18, base)
	u.Quality = things.QualityUncertain
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		u,
		tempObs("s2", 21, base.Add(time.Minute)),
		tempObs("s3", 24, base.Add(2*time.Minute)),
	}, base.Add(3*time.Minute), "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	avg, ok := out.Updated["temperature.average"]
	if !ok || avg.Value == nil || *avg.Value != 21 {
		t.Fatalf("average=%+v, want 21", avg)
	}
	if avg.Quality != "uncertain" {
		t.Fatalf("average quality=%q, want uncertain", avg.Quality)
	}
	for _, e := range out.Events {
		if e.PropertyID == "temperature.average" && e.Quality != "uncertain" {
			t.Fatalf("mean event quality=%q, want uncertain: %+v", e.Quality, e)
		}
	}
}

// C8: härlett värde får senaste bidragande mättid.
func TestDerivedObservedAtIsLatest(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	latest := base.Add(2 * time.Minute)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 21, base.Add(time.Minute)),
		tempObs("s3", 24, latest),
	}, base.Add(3*time.Minute), "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	avg := out.Updated["temperature.average"]
	if !avg.ObservedAt.Equal(latest) {
		t.Fatalf("average ObservedAt=%v, want latest %v", avg.ObservedAt, latest)
	}
}

// C7/F7: härledda källor ska vara underliggande käll-ID:n, inte
// egenskapsnamn, med antal tillgängliga/förväntade samt revision.
func TestDerivedSourcesAreSignalIDs(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 18, base),
		tempObs("s2", 21, base.Add(time.Minute)),
		tempObs("s3", 24, base.Add(2*time.Minute)),
	}, base.Add(3*time.Minute), "m-batch", 3)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	var found *FlowEvent
	for i, e := range out.Events {
		if e.PropertyID == "temperature.average" {
			found = &out.Events[i]
		}
	}
	if found == nil {
		t.Fatalf("no average event: %+v", out.Events)
	}
	want := map[string]bool{
		"s1/3303/5700": false, "s2/3303/5700": false, "s3/3303/5700": false,
	}
	if len(found.Sources) != 3 {
		t.Fatalf("sources=%v, want 3 signal IDs", found.Sources)
	}
	for _, s := range found.Sources {
		if _, ok := want[s]; !ok {
			t.Fatalf("source %q is not a signal ID, want s1/s2/s3 stream IDs: %v", s, found.Sources)
		}
		want[s] = true
	}
	if found.Available != 3 || found.Expected != 3 {
		t.Fatalf("available/expected=%d/%d, want 3/3", found.Available, found.Expected)
	}
	if found.ConfigRevision != 3 || found.MessageID != "m-batch" {
		t.Fatalf("provenance missing: %+v", found)
	}
	if found.Operator != "mean" || found.OperatorVersion != "v1" {
		t.Fatalf("operator provenance missing: %+v", found)
	}
}

// C2: dubblett-output från två recept nekas.
func TestDuplicateOutputRejected(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "dup", Version: "v1",
		Required: []string{"a", "b", "c"},
		Category: "dup",
	},
		Recipes: []catalog.Recipe{
			{Name: "r1", Operator: "passthrough", Version: "v1", Inputs: []string{"a"}, Outputs: []string{"c"}},
			{Name: "r2", Operator: "passthrough", Version: "v1", Inputs: []string{"b"}, Outputs: []string{"c"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "dup-1", Tenant: "t", Name: "Dupp",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "dup", TemplateVersion: "v1",
		Properties: map[string]things.Property{"a": {}, "b": {}, "c": {}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{
		"a": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
		"b": {Value: new(float64(2)), Quality: "ok", ObservedAt: base},
	}
	if _, err := EvaluateBatchFlow(cfg, current, nil, nil, base, "m", 1); err == nil {
		t.Fatal("duplicate output must be rejected")
	}
}

// C4: saknade operatorparametrar nekas, inte tysta default.
func TestMissingOperatorParamsRejected(t *testing.T) {
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	current := map[string]CurrentValue{
		"p": {Value: new(float64(1)), Quality: "ok", ObservedAt: base},
	}
	for _, tc := range []struct {
		name string
		cfg  catalog.EffectiveConfig
	}{
		{"threshold-no-params", catalog.EffectiveConfig{
			Properties: map[string]things.Property{"p": {}, "q": {}},
			Recipes: []catalog.Recipe{
				{Name: "r", Operator: "threshold", Version: "v1", Inputs: []string{"p"}, Outputs: []string{"q"}},
			},
		}},
		{"level-no-param", catalog.EffectiveConfig{
			Properties: map[string]things.Property{"p": {}, "q": {}},
			Recipes: []catalog.Recipe{
				{Name: "r", Operator: "distanceToLevel", Version: "v1", Inputs: []string{"p"}, Outputs: []string{"q"}},
			},
		}},
	} {
		if _, err := EvaluateBatchFlow(&tc.cfg, current, nil, nil, base, "m", 1); err == nil {
			t.Fatalf("%s: missing param must fail", tc.name)
		}
	}
}

// C6: saknat värde får aldrig bli numeriskt noll.
func TestNilValueNeverBecomesCurrent(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	obs := tempObs("s1", 0, base)
	obs.Value = nil
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{obs}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if _, ok := out.Updated["temperature.window"]; ok {
		t.Fatalf("nil value must not become current: %+v", out.Updated)
	}
	for _, e := range out.Events {
		if e.PropertyID == "temperature.window" {
			t.Fatalf("nil value must not emit event: %+v", e)
		}
	}
	if len(out.History) != 1 || out.History[0].Quality != "invalid" {
		t.Fatalf("nil value must be recorded invalid: %+v", out.History)
	}
}

// B12/B13: ogiltig nekas av kvalitet och markeras inte som Late.
func TestInvalidHistoryNotMarkedLate(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	bad := tempObs("s1", 0, base.Add(time.Minute))
	bad.Quality = things.QualityInvalid
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{bad}, base.Add(time.Minute), "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if len(out.History) != 1 {
		t.Fatalf("history=%+v, want 1", out.History)
	}
	if out.History[0].Late {
		t.Fatalf("invalid must not be marked Late: %+v", out.History[0])
	}
	if len(out.Updated) != 0 || len(out.Events) != 0 {
		t.Fatalf("invalid must give no current/event: %+v %+v", out.Updated, out.Events)
	}
}

// L/C: utfall får inte dela pekare med indata eller mellan Updated/History.
func TestOutcomeDoesNotAliasInput(t *testing.T) {
	_, cfg := flowRooms(t)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 18.0
	obs := tempObs("s1", v, base)
	obs.Value = &v
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{obs}, base, "m", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	v = 999
	got := out.Updated["temperature.window"]
	if got.Value == nil || *got.Value != 18 {
		t.Fatalf("mutating input affected outcome: %+v", got)
	}
	if len(out.History) != 1 || out.History[0].Value == nil || *out.History[0].Value != 18 {
		t.Fatalf("history aliased input: %+v", out.History)
	}
	*got.Value = 777
	if *out.History[0].Value != 18 {
		t.Fatalf("Updated and History share pointer: history=%v", *out.History[0].Value)
	}
}

// Differens mellan två mätpunkter (t.ex. temperatur vid två ingångar):
// första minus andra, med tecken. Båda krävs; uncertain propagerar.
func TestSubtractInputs(t *testing.T) {
	cfg := catalog.EffectiveConfig{
		Properties: map[string]things.Property{
			"temperature.a": {}, "temperature.b": {}, "temperature.diff": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNtemp, Resource: "5700", Input: "temperature.a"},
			{DeviceID: "s2", Object: flowURNtemp, Resource: "5700", Input: "temperature.b"},
		},
		Recipes: []catalog.Recipe{
			{Name: "diff", Operator: "subtract", Version: "v1",
				Inputs: []string{"temperature.a", "temperature.b"}, Outputs: []string{"temperature.diff"}},
		},
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 21, base), tempObs("s2", 18, base),
	}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	diff, ok := out.Updated["temperature.diff"]
	if !ok || diff.Value == nil || *diff.Value != 3 {
		t.Fatalf("diff=%+v, want 3", out.Updated)
	}

	// Saknad ingång ger inget resultat.
	out, err = EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 21, base),
	}, base, "m2", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if _, ok := out.Updated["temperature.diff"]; ok {
		t.Fatal("subtract without both inputs must not produce a result")
	}

	// Uncertain på en ingång ger uncertain resultat.
	uncertain := tempObs("s2", 18, base)
	uncertain.Quality = things.QualityUncertain
	out, err = EvaluateBatchFlow(&cfg, nil, nil, []things.Observation{
		tempObs("s1", 21, base), uncertain,
	}, base, "m3", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if q := out.Updated["temperature.diff"].Quality; q != "uncertain" {
		t.Fatalf("quality=%q, want uncertain", q)
	}
}

// Per-recept-parametrar: två nivågivare på olika höjd i samma avlopp,
// varsin offset, gemensam sammanvägning. Samma avstånd ska ge olika nivå.
func TestPerRecipeLevelOffsets(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "sewer", Version: "v1",
			Required: []string{"distance1", "distance2", "level1", "level2", "level"},
			Category: "sewer",
		},
		ParamDefaults: map[string]float64{"sensor1bottom": 2.0, "sensor2bottom": 1.0},
		Recipes: []catalog.Recipe{
			{Name: "level-1", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance1"}, Outputs: []string{"level1"},
				Params: []string{"sensor1bottom"}},
			{Name: "level-2", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance2"}, Outputs: []string{"level2"},
				Params: []string{"sensor2bottom"}},
			{Name: "mean-level", Operator: "mean", Version: "v1",
				Inputs: []string{"level1", "level2"}, Outputs: []string{"level"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "sewer-2", Tenant: "t", Name: "Avlopp 2",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "sewer", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance1": {}, "distance2": {}, "level1": {}, "level2": {}, "level": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: flowURNdistance, Resource: "5700", Input: "distance1"},
			{DeviceID: "s2", Object: flowURNdistance, Resource: "5700", Input: "distance2"},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	dist := 0.5
	out, err := EvaluateBatchFlow(cfg, nil, nil, []things.Observation{
		{Signal: things.ParseSignalID("s1", "", flowURNdistance, "5700"), Value: &dist,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("s2", "", flowURNdistance, "5700"), Value: &dist,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if got := *out.Updated["level1"].Value; got != 1.5 {
		t.Fatalf("level1=%v, want 1.5 (2.0-0.5)", got)
	}
	if got := *out.Updated["level2"].Value; got != 0.5 {
		t.Fatalf("level2=%v, want 0.5 (1.0-0.5)", got)
	}
	if got := *out.Updated["level"].Value; got != 1.0 {
		t.Fatalf("level=%v, want mean 1.0", got)
	}
}

// Per-recept-parametrar: två olika enhetskonverteringar i samma mall
// (byggnadens energi J->kWh och effekt W->kW).
func TestPerRecipeLinearConversions(t *testing.T) {
	c := catalog.New()
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "building", Version: "v1",
			Required: []string{"energy", "power", "energyKwh", "powerKw"},
			Category: "building",
		},
		ParamDefaults: map[string]float64{"energyFactor": 1.0 / 3600000.0, "powerFactor": 1.0 / 1000.0},
		Recipes: []catalog.Recipe{
			{Name: "energy-si", Operator: "linear", Version: "v1",
				Inputs: []string{"energy"}, Outputs: []string{"energyKwh"},
				Params: []string{"energyFactor"}},
			{Name: "power-si", Operator: "linear", Version: "v1",
				Inputs: []string{"power"}, Outputs: []string{"powerKw"},
				Params: []string{"powerFactor"}},
		},
	}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "building-1", Tenant: "t", Name: "Byggnad 1",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"energy": {}, "power": {}, "energyKwh": {}, "powerKw": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s-e", Object: flowURNtemp, Resource: "5700", Input: "energy"},
			{DeviceID: "s-p", Object: flowURNtemp, Resource: "5700", Input: "power"},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	joule, watt := 3600000.0, 2000.0
	out, err := EvaluateBatchFlow(cfg, nil, nil, []things.Observation{
		{Signal: things.ParseSignalID("s-e", "", flowURNtemp, "5700"), Value: &joule,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("s-p", "", flowURNtemp, "5700"), Value: &watt,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base, "m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	if got := *out.Updated["energyKwh"].Value; got < 0.999 || got > 1.001 {
		t.Fatalf("energyKwh=%v, want 1.0", got)
	}
	if got := *out.Updated["powerKw"].Value; got < 1.999 || got > 2.001 {
		t.Fatalf("powerKw=%v, want 2.0", got)
	}
}

// Seed-backed SI-parity: building/v3 och sink/v3 ur catalog-seed.json
// kompilerar och konverterar v1-råvärden (J->kWh, W->kW) med låsta
// faktorer. Låser att seed-innehållet faktiskt sluter konverteringsgapet,
// inte bara att motorn kan linear (se TestPerRecipeLinearConversions).
func TestSeedV3SIConversions(t *testing.T) {
	raw, err := os.ReadFile("../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := catalog.ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	c := catalog.New()
	for _, spec := range seed.Templates {
		if err := c.PutTemplate(spec); err != nil {
			t.Fatalf("publish %s/%s: %v", spec.Template.ID, spec.Template.Version, err)
		}
	}

	compile := func(thingID, templateID string) catalog.EffectiveConfig {
		t.Helper()
		props := map[string]things.Property{"energy": {}, "power": {}, "energyKwh": {}, "powerKw": {}}
		if templateID == "sink" {
			for _, id := range []string{"temperature", "presence", "distance", "digitalInput", "illuminance", "humidity"} {
				props[id] = things.Property{}
			}
		} else {
			props["temperature"] = things.Property{}
		}
		cfg, err := c.Compile(catalog.ObjectSpec{
			ThingID: thingID, Tenant: "t", Name: thingID,
			Location: rtLoc(), Metadata: rtMeta(),
			TemplateID: templateID, TemplateVersion: "v3",
			Properties: props,
			Bindings: []things.Binding{
				{DeviceID: "s-e", Object: "urn:oma:lwm2m:ext:3331", Resource: "5700", Input: "energy"},
				{DeviceID: "s-p", Object: "urn:oma:lwm2m:ext:3328", Resource: "5700", Input: "power"},
			},
		})
		if err != nil {
			t.Fatalf("compile %s/v3: %v", templateID, err)
		}
		return *cfg
	}

	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	joule, watt := 3600000.0, 2000.0
	obs := []things.Observation{
		{Signal: things.ParseSignalID("s-e", "", "urn:oma:lwm2m:ext:3331", "5700"), Value: &joule,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("s-p", "", "urn:oma:lwm2m:ext:3328", "5700"), Value: &watt,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}

	for _, templateID := range []string{"building", "sink"} {
		cfg := compile("si-"+templateID, templateID)
		out, err := EvaluateBatchFlow(&cfg, nil, nil, obs, base, "m1", 1)
		if err != nil {
			t.Fatalf("%s flow: %v", templateID, err)
		}
		if got := *out.Updated["energyKwh"].Value; got < 0.999 || got > 1.001 {
			t.Fatalf("%s energyKwh=%v, want 1.0", templateID, got)
		}
		if got := *out.Updated["powerKw"].Value; got < 1.999 || got > 2.001 {
			t.Fatalf("%s powerKw=%v, want 2.0", templateID, got)
		}
	}
}
