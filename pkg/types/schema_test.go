package types

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// K1.1: exakt v1-schema. Tabellen låser required/optional per fält.
// Required-ändringar kräver schema-bump; additiva omitempty-fält är
// bakåtkompatibla.
func TestK1SchemaFieldTable(t *testing.T) {
	v := 1.0
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/bin-42/m1", Tenant: "t",
		ThingID: "bin-42", ThingType: "wastebin", ThingTypeVersion: "v1",
		ConfigRevision: 2, PublishedAt: mustTime(t, "2026-09-17T12:05:00Z"),
		Thing: &ThingDescriptorV1{
			ThingID: "bin-42", Name: "Soptunna 42",
			AlternativeName: "BinA", SubType: "WasteContainer",
			Category: "wastebin",
		},
		Values: []ThingValueV1{{
			Schema: "v1", ThingID: "bin-42", Tenant: "t",
			PropertyID: "fillRate", Value: &v, Quality: "ok",
			Unit: "pct", Source: "milesight:214/3330/5700",
			MessageID:      "m1",
			ObservedAt:     mustTime(t, "2026-09-17T12:00:00Z"),
			ReceivedAt:     mustTime(t, "2026-09-17T12:00:00Z"),
			ComputedAt:     mustTime(t, "2026-09-17T12:05:00Z"),
			PublishedAt:    mustTime(t, "2026-09-17T12:05:00Z"),
			ConfigRevision: 2, EventID: "v1/t/bin-42/m1/fillRate",
		}},
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"schema", "eventId", "tenant", "thingId", "thingType",
		"thingTypeVersion", "configRevision", "publishedAt", "thing", "values",
	} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("envelope saknar K1-fält %q: %s", key, raw)
		}
	}
	thing := decoded["thing"].(map[string]any)
	for _, key := range []string{"thingId", "name", "alternativeName", "subType", "category"} {
		if _, ok := thing[key]; !ok {
			t.Fatalf("deskriptor saknar K1-fält %q: %s", key, raw)
		}
	}
	val := decoded["values"].([]any)[0].(map[string]any)
	for _, key := range []string{
		"schema", "thingId", "tenant", "propertyId", "value", "quality",
		"unit", "source", "messageId", "observedAt", "receivedAt",
		"computedAt", "publishedAt", "configRevision", "eventId",
	} {
		if _, ok := val[key]; !ok {
			t.Fatalf("value saknar K1-fält %q: %s", key, raw)
		}
	}
}

// K1.1: additiva fält får inte ändra wire-format för gamla saker.
func TestK1DescriptorOmitsEmptyAdditiveFields(t *testing.T) {
	raw, _ := json.Marshal(ThingDescriptorV1{ThingID: "bin-42", Name: "Soptunna"})
	s := string(raw)
	if strings.Contains(s, "alternativeName") || strings.Contains(s, "subType") {
		t.Fatalf("tomma additiva fält ska utelämnas (omitempty): %s", s)
	}
}

// K1.2: identitet — NameOrID, TypeName, UUID, SafeURI.
func TestK1IdentityHelpers(t *testing.T) {
	d := ThingDescriptorV1{ThingID: "bin-42", Name: "Soptunna", AlternativeName: "BinA"}
	if got := d.NameOrID(); got != "BinA" {
		t.Fatalf("NameOrID=%q, want BinA", got)
	}
	d.AlternativeName = ""
	if got := d.NameOrID(); got != "Soptunna" {
		t.Fatalf("NameOrID=%q, want Soptunna", got)
	}
	d.Name = ""
	if got := d.NameOrID(); got != "bin-42" {
		t.Fatalf("NameOrID=%q, want bin-42", got)
	}
	if got := d.TypeName("wastebin"); got != "wastebin" {
		t.Fatalf("TypeName=%q, want wastebin", got)
	}
	d.SubType = "WasteContainer"
	if got := d.TypeName("wastebin"); got != "WasteContainer" {
		t.Fatalf("TypeName=%q, want WasteContainer", got)
	}
	if got := UUID("bin-42"); got != "bin-42" {
		t.Fatalf("UUID=%q", got)
	}
	if got := UUID("a/b/bin-42"); got != "bin-42" {
		t.Fatalf("UUID=%q, want bin-42", got)
	}
	// Paritet för alternativnamnshärledning (alternativeName > name > id):
	// mellanslag och åäö mappas till ":", tillåtna tecken bevaras.
	cases := map[string]string{
		"Greenhouse 1": "Greenhouse:1",
		"Växthus 1":    "V:xthus:1",
		"GH_1":         "GH_1",
		"Växthus-1":    "V:xthus-1",
	}
	for in, want := range cases {
		if got := SafeURISegment(in); got != want {
			t.Fatalf("SafeURISegment(%q)=%q, want %q", in, got, want)
		}
	}
}
