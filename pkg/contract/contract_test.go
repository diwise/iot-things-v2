package contract

import (
	"encoding/json"
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
