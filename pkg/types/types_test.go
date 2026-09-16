package types

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestThingValueValidate(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	v := ThingValueV1{
		Schema:     SchemaV1,
		ThingID:    "t1",
		Tenant:     "acme",
		PropertyID: "temp",
		Quality:    "ok",
		ObservedAt: ts,
		ReceivedAt: ts,
	}
	if err := v.Validate(); err != nil {
		t.Fatalf("giltigt värde ska passera: %v", err)
	}
	bad := v
	bad.Quality = "good"
	if err := bad.Validate(); err == nil {
		t.Fatal("kvalitet good ska nekas (strikt ok/uncertain/invalid)")
	}
	missing := v
	missing.ObservedAt = time.Time{}
	if err := missing.Validate(); err == nil {
		t.Fatal("saknad observedAt ska nekas")
	}
}

// Regression kvalitet: tom/okänd kvalitet ska nekas strikt (B12).
// Tom sträng är inte ett giltigt enum-värde, i linje med
// EvaluateBatch/ShouldBecomeCurrent som kräver ok/uncertain.
func TestThingValueValidateRejectsEmptyAndUnknownQuality(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	base := ThingValueV1{
		Schema:     SchemaV1,
		ThingID:    "t1",
		Tenant:     "acme",
		PropertyID: "temp",
		ObservedAt: ts,
	}
	for _, q := range []string{"", "good", "bogus", "INVALID", "Ok"} {
		v := base
		v.Quality = q
		if err := v.Validate(); err == nil {
			t.Fatalf("kvalitet %q ska nekas", q)
		}
	}
	for _, q := range []string{"ok", "uncertain", "invalid"} {
		v := base
		v.Quality = q
		if err := v.Validate(); err != nil {
			t.Fatalf("kvalitet %q ska passera: %v", q, err)
		}
	}
}

// Regression: saknade identitetsfält ska nekas (tenant-läckage-skydd).
func TestThingValueValidateRequiresIdentity(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	base := ThingValueV1{
		Schema:     SchemaV1,
		ThingID:    "t1",
		Tenant:     "acme",
		PropertyID: "temp",
		Quality:    "ok",
		ObservedAt: ts,
	}
	for _, mutate := range []struct {
		name string
		fn   func(*ThingValueV1)
	}{
		{"thingId", func(v *ThingValueV1) { v.ThingID = "" }},
		{"tenant", func(v *ThingValueV1) { v.Tenant = "" }},
		{"propertyId", func(v *ThingValueV1) { v.PropertyID = "" }},
		{"observedAt", func(v *ThingValueV1) { v.ObservedAt = time.Time{} }},
	} {
		v := base
		mutate.fn(&v)
		if err := v.Validate(); err == nil {
			t.Fatalf("saknad %s ska nekas", mutate.name)
		}
	}
}

func TestThingValueTimeFieldsRoundtrip(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	v := ThingValueV1{
		Schema:     SchemaV1,
		ThingID:    "t1",
		Tenant:     "acme",
		PropertyID: "temp",
		Quality:    "uncertain",
		ObservedAt: ts,
		ReceivedAt: ts.Add(time.Second),
		ComputedAt: ts.Add(2 * time.Second),
	}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var back ThingValueV1
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !back.ReceivedAt.Equal(v.ReceivedAt) || !back.ComputedAt.Equal(v.ComputedAt) {
		t.Fatalf("tidsfält överlevde ej roundtrip: %+v", back)
	}
	if back.Schema != SchemaV1 {
		t.Fatalf("schema = %q, want %q", back.Schema, SchemaV1)
	}
}

// Regression L7/P12: NaN/Inf får ej passera Validate (json.Body skulle bli nil).
func TestThingValueValidateRejectsNaNInf(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	base := ThingValueV1{
		Schema:     SchemaV1,
		ThingID:    "t1",
		Tenant:     "acme",
		PropertyID: "temp",
		Quality:    "ok",
		ObservedAt: ts,
	}
	nan := base
	nanVal := math.NaN()
	nan.Value = &nanVal
	if err := nan.Validate(); err == nil {
		t.Fatal("NaN ska nekas")
	}
	inf := base
	infVal := math.Inf(1)
	inf.Value = &infVal
	if err := inf.Validate(); err == nil {
		t.Fatal("+Inf ska nekas")
	}
	ok := base
	v := 21.5
	ok.Value = &v
	if err := ok.Validate(); err != nil {
		t.Fatalf("giltigt värde ska passera: %v", err)
	}
}

// Regression P12: TopicName/ContentType/Body kontrakt.
func TestThingValuesEventTopicContract(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	v := 1.0
	ev := ThingValuesEventV1{
		Schema:         SchemaV1,
		EventID:        "v1/acme/t1/m1",
		Tenant:         "acme",
		ThingID:        "t1",
		ConfigRevision: 2,
		PublishedAt:    ts,
		Values: []ThingValueV1{{
			Schema: SchemaV1, ThingID: "t1", Tenant: "acme", PropertyID: "temp",
			Value: &v, Quality: "ok", ObservedAt: ts, PublishedAt: ts, EventID: "v1/acme/t1/m1/temp",
		}},
	}
	if ev.TopicName() != TopicThingValues {
		t.Fatalf("TopicName=%q, want %q", ev.TopicName(), TopicThingValues)
	}
	if ev.ContentType() == "" {
		t.Fatal("ContentType krävs")
	}
	body := ev.Body()
	if len(body) == 0 {
		t.Fatal("Body får ej vara tom")
	}
	var back ThingValuesEventV1
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatalf("Body roundtrip: %v", err)
	}
	if back.EventID != ev.EventID || len(back.Values) != 1 || back.Values[0].PropertyID != "temp" {
		t.Fatalf("Body roundtrip mismatch: %+v", back)
	}
}
