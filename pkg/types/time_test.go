package types

import (
	"testing"
	"time"
)

// K1.3 code review: konsumentens tid är max(values[].observedAt),
// aldrig transporttid. Tomma values ger nolltid.
func TestK1EventTimestampIsMaxObservedAt(t *testing.T) {
	at1 := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	at2 := time.Date(2026, 9, 17, 12, 3, 0, 0, time.UTC)
	now := time.Date(2026, 9, 17, 12, 5, 0, 0, time.UTC)
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/room-a/m1", Tenant: "t",
		ThingID: "room-a", ConfigRevision: 1, PublishedAt: now,
		Values: []ThingValueV1{
			{ThingID: "room-a", Tenant: "t", PropertyID: "a", Quality: "ok", ObservedAt: at1, PublishedAt: now},
			{ThingID: "room-a", Tenant: "t", PropertyID: "b", Quality: "ok", ObservedAt: at2, PublishedAt: now},
		},
	}
	if got := ev.EventTimestamp(); !got.Equal(at2) {
		t.Fatalf("EventTimestamp=%v, want %v", got, at2)
	}
	var empty ThingValuesEventV1
	if !empty.EventTimestamp().IsZero() {
		t.Fatalf("tomt event ska ge nolltid, fick %v", empty.EventTimestamp())
	}
}

// K1.5 code review: konsumentregel för kvalitet — invalid hoppar property,
// required styr entitet.
func TestK1ConsumeQuality(t *testing.T) {
	for _, q := range []string{"ok", "uncertain"} {
		if !ShouldEmitProperty(q) {
			t.Fatalf("quality %q ska emitteras", q)
		}
	}
	for _, q := range []string{"invalid", "", "good", "Ok"} {
		if ShouldEmitProperty(q) {
			t.Fatalf("quality %q ska hoppas över", q)
		}
	}
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 1.0
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/b/m", Tenant: "t",
		ThingID: "b", ConfigRevision: 1, PublishedAt: at,
		Values: []ThingValueV1{
			{ThingID: "b", Tenant: "t", PropertyID: "level", Value: &v, Quality: "ok", ObservedAt: at},
		},
	}
	if !ev.ShouldEmitEntity([]string{"level"}) {
		t.Fatal("komplett required ska emitteras")
	}
	if ev.ShouldEmitEntity([]string{"level", "fillRate"}) {
		t.Fatal("saknad required ska hoppa entiteten")
	}
	ev.Values[0].Quality = "invalid"
	if ev.ShouldEmitEntity([]string{"level"}) {
		t.Fatal("invalid required ska hoppa entiteten")
	}
	if !ev.ShouldEmitEntity(nil) {
		t.Fatal("tom required-lista är giltig config (valfria only)")
	}
}

// K1.4 code review: field-vägar values.<propertyId>.* — ersättare för
// gamla temperature.v/measured.level/percent/refDevices-vägar.
func TestK1ValueByProperty(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.5
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/room-a/m1", Tenant: "t",
		ThingID: "room-a", ConfigRevision: 1, PublishedAt: at,
		Values: []ThingValueV1{{
			ThingID: "room-a", Tenant: "t", PropertyID: "temperature.window",
			Value: &v, Quality: "ok", Unit: "Cel",
			Source: "s1/3303/5700", ObservedAt: at,
		}},
	}
	got := ev.ValueByProperty("temperature.window")
	if got == nil || got.PropertyID != "temperature.window" || *got.Value != 21.5 {
		t.Fatalf("ValueByProperty=%+v", got)
	}
	if ev.ValueByProperty("missing") != nil {
		t.Fatal("saknad egenskap ska ge nil (konsumenten hoppar property/entitet per required)")
	}
}
