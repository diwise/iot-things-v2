package types

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Konsumentkompatibilitet Etapp 7: konsumenter av thing.updated
// (kuvertet id/type/thing/tenant/timestamp) måste kunna mappa utan
// katalogjoin. Tabellen:
//
//	thing.updated.id        <- ThingValuesEventV1.ThingID (+ per value)
//	thing.updated.type      <- ThingValuesEventV1.ThingType (mall-ID)
//	thing.updated.thing.*   <- Values[] (propertyId/value/unit/quality/...)
//	thing.updated.tenant    <- Tenant
//	thing.updated.timestamp <- Values[].ObservedAt (mättid, ej transporttid)
func TestConsumerMappingComplete(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	now := at.Add(5 * time.Minute)
	v := 93.33
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/bin-42/m1", Tenant: "t",
		ThingID: "bin-42", ThingType: "wastebin", ThingTypeVersion: "v1",
		ConfigRevision: 2, PublishedAt: now,
		Values: []ThingValueV1{{
			Schema: "v1", ThingID: "bin-42", Tenant: "t",
			PropertyID: "fillRate", Value: &v, Quality: "uncertain",
			Unit: "pct", Source: "milesight:214/3330/5700",
			MessageID: "m1", ObservedAt: at, ReceivedAt: at,
			ComputedAt: now, PublishedAt: now, ConfigRevision: 2,
			EventID: "v1/t/bin-42/m1/fillRate",
		}},
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{
		"thingId", "thingType", "thingTypeVersion", "tenant",
		"configRevision", "publishedAt", "eventId", "values",
	} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("event saknar konsumentfält %q: %s", key, raw)
		}
	}
	vals := decoded["values"].([]any)
	val := vals[0].(map[string]any)
	for _, key := range []string{
		"propertyId", "value", "quality", "unit", "source",
		"observedAt", "messageId", "eventId",
	} {
		if _, ok := val[key]; !ok {
			t.Fatalf("value saknar konsumentfält %q: %s", key, raw)
		}
	}
	// Mättid, inte transporttid, är konsumentens tid.
	if val["observedAt"] != "2026-09-17T12:00:00Z" {
		t.Fatalf("observedAt=%v", val["observedAt"])
	}
	if got := ev.TopicName(); got != "things.v1.values" {
		t.Fatalf("topic=%q", got)
	}
	if !strings.Contains(ev.ContentType(), "v1") {
		t.Fatalf("contentType=%q", ev.ContentType())
	}
	if len(ev.Body()) == 0 {
		t.Fatal("body required")
	}
}

// Golden JSON: kontraktet är stabilt; ändringar kräver medvetet beslut
// (samma princip som konsumentens kuvertlås). EventID formateras som
// StableEventID i internal/application (v1/tenant/thing/messageID[/egenskap]);
// golden hårdkodar samma strängar eftersom pkg/types inte får importera
// internal (importcykel). ReceivedAt/ComputedAt sätts alltid i produktion
// (buildThingValuesEvent: ReceivedAt=e.ReceivedAt, ComputedAt=PublishedAt),
// så fixturen använder skilda mättids- och publiceringstider för att låsa
// att tidsfälten finns med och att ID:na är oberoende av PublishedAt.
func TestGoldenEventJSON(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	now := at.Add(5 * time.Minute)
	v := 21.5
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/room-a/m1", Tenant: "t",
		ThingID: "room-a", ThingType: "room", ThingTypeVersion: "v1",
		ConfigRevision: 1, PublishedAt: now,
		Values: []ThingValueV1{{
			Schema: "v1", ThingID: "room-a", Tenant: "t",
			PropertyID: "temperature.window", Value: &v, Quality: "ok",
			Unit: "Cel", Source: "s1/3303/5700",
			MessageID: "m1", ObservedAt: at, ReceivedAt: at,
			ComputedAt: now, PublishedAt: now,
			ConfigRevision: 1, EventID: "v1/t/room-a/m1/temperature.window",
		}},
	}
	raw := string(ev.Body())
	for _, want := range []string{
		`"schema":"v1"`,
		`"eventId":"v1/t/room-a/m1"`,
		`"tenant":"t"`,
		`"thingId":"room-a"`,
		`"thingType":"room"`,
		`"thingTypeVersion":"v1"`,
		`"propertyId":"temperature.window"`,
		`"value":21.5`,
		`"quality":"ok"`,
		`"unit":"Cel"`,
		`"observedAt":"2026-09-17T12:00:00Z"`,
		`"receivedAt":"2026-09-17T12:00:00Z"`,
		`"computedAt":"2026-09-17T12:05:00Z"`,
		`"publishedAt":"2026-09-17T12:05:00Z"`,
		`"eventId":"v1/t/room-a/m1/temperature.window"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("golden saknar %s:\n%s", want, raw)
		}
	}
}

// Relationshändelser bär from/to-descriptors så att konsumenter kan routa
// och rendera utan kataloguppslag. Fälten är additiva (omitempty): äldre
// konsumenter som bara läser relationId/type/fromId/toId påverkas inte.
func TestGoldenRelationChangeFromTo(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := RelationChangeV1{
		RelationID: "partof-tank-1", Type: "partOf",
		FromID: "tank-1", ToID: "greenhouse-a",
		Tenant: "t", Revision: 2, ChangedAt: at,
		FromThingType: "tank",
		From:          &ThingDescriptorV1{ThingID: "tank-1", Name: "Tank 1", Category: "tank"},
		ToThingType:   "greenhouse",
		To:            &ThingDescriptorV1{ThingID: "greenhouse-a", Name: "Växthus A", Category: "greenhouse"},
	}
	raw := string(ev.Body())
	for _, want := range []string{
		`"relationId":"partof-tank-1"`,
		`"type":"partOf"`,
		`"fromId":"tank-1"`,
		`"toId":"greenhouse-a"`,
		`"fromThingType":"tank"`,
		`"toThingType":"greenhouse"`,
		`"thingId":"tank-1"`,
		`"thingId":"greenhouse-a"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("golden saknar %s:\n%s", want, raw)
		}
	}
	var back RelationChangeV1
	if err := json.Unmarshal([]byte(raw), &back); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if back.FromThingType != "tank" || back.From == nil || back.From.Name != "Tank 1" {
		t.Fatalf("from=%+v, want descriptor", back.From)
	}
	if back.ToThingType != "greenhouse" || back.To == nil || back.To.Name != "Växthus A" {
		t.Fatalf("to=%+v, want descriptor", back.To)
	}
	if back.TopicName() != TopicRelationChanges {
		t.Fatalf("topic=%q", back.TopicName())
	}
}
