package types

import (
	"testing"
	"time"
)

// K1.6: konsumentvariabelhärledning per pilotmall från låsta K1-primitiver.
// Testet låser att varje konsumentbehov kan deriveras utan katalogjoin.
// Tjänsten känner inte till sina konsumenter; testet verifierar endast
// att kontraktet bär det en extern konsument behöver.
func TestK1ConsumerBindingVariables(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	now := at.Add(5 * time.Minute)
	v := 33.0
	cases := []struct {
		name      string
		thingType string
		thing     *ThingDescriptorV1
		property  string
		required  []string
		wantID    string
		wantType  string
	}{
		{"wastebin", "wastebin",
			&ThingDescriptorV1{ThingID: "bin-42", Name: "Soptunna 42", AlternativeName: "BinA", SubType: "WasteContainer", Category: "wastebin"},
			"fillRate", []string{"fillRate"},
			"urn:ngsi-ld:WasteContainer:BinA", "WasteContainer"},
		{"room", "room",
			&ThingDescriptorV1{ThingID: "room-a", Name: "Rum A", Category: "room"},
			"temperature.average", []string{"temperature.average"},
			"urn:ngsi-ld:room:Rum:A", "room"},
		{"greenhouse", "greenhouse",
			&ThingDescriptorV1{ThingID: "gh-1", Name: "Växthus 1", AlternativeName: "Greenhouse 1", Category: "greenhouse"},
			"co2", []string{"co2"},
			"urn:ngsi-ld:greenhouse:Greenhouse:1", "greenhouse"},
	}
	for _, c := range cases {
		ev := ThingValuesEventV1{
			Schema: "v1", EventID: "v1/t/" + c.thing.ThingID + "/m1", Tenant: "t",
			ThingID: c.thing.ThingID, ThingType: c.thingType, ThingTypeVersion: "v1",
			ConfigRevision: 7, PublishedAt: now, Thing: c.thing,
			Values: []ThingValueV1{{
				Schema: "v1", ThingID: c.thing.ThingID, Tenant: "t",
				PropertyID: c.property, Value: &v, Quality: "ok",
				Source: "s1/3303/5700", MessageID: "m1",
				ObservedAt: at, ReceivedAt: at, ComputedAt: now,
				PublishedAt: now, ConfigRevision: 7,
				EventID: "v1/t/" + c.thing.ThingID + "/m1/" + c.property,
			}},
		}
		nameOrID := SafeURISegment(ev.Thing.NameOrID())
		typeName := SafeURISegment(ev.Thing.TypeName(ev.ThingType))
		if got := "urn:ngsi-ld:" + typeName + ":" + nameOrID; got != c.wantID {
			t.Fatalf("%s id=%q, want %q", c.name, got, c.wantID)
		}
		if got := ev.Thing.TypeName(ev.ThingType); SafeURISegment(got) != SafeURISegment(c.wantType) {
			t.Fatalf("%s type=%q, want %q", c.name, got, c.wantType)
		}
		if UUID(ev.ThingID) == "" {
			t.Fatalf("%s uuid tom", c.name)
		}
		if !ev.EventTimestamp().Equal(at) {
			t.Fatalf("%s timestamp=%v, want %v", c.name, ev.EventTimestamp(), at)
		}
		if ev.ConfigRevision != 7 {
			t.Fatalf("%s revision=%d, want 7", c.name, ev.ConfigRevision)
		}
		if ev.ValueByProperty(c.property) == nil {
			t.Fatalf("%s saknar field-väg values.%s", c.name, c.property)
		}
		if !ev.ShouldEmitEntity(c.required) {
			t.Fatalf("%s komplett required ska emitteras", c.name)
		}
		if !ShouldEmitProperty(ev.Values[0].Quality) {
			t.Fatalf("%s ok ska emitteras", c.name)
		}
	}
}
