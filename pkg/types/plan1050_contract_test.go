package types

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// PLAN1050 B3 producentkontrakt (låst baslinje Våg 0, additiv inom v1):
// delta+snapshot+removed+descriptor+versionsreferenser i samma event,
// generella namngivna relationsplatser med målversioner, lifecycle med
// bootstrap-flagga, kompletta descriptors, flera mål, neutrala identiteter.
// values[] behålls EN release som deprecated alias == delta[].
// Topics oförändrade, schema v1, contentType vnd.diwise.things.v1+json,
// nya fält omitempty-additiva.

func plan1050Time() (at, now time.Time) {
	at = time.Date(2026, 9, 17, 12, 0, 0, 123456000, time.UTC) // subsekundprecision
	now = at.Add(5 * time.Minute)
	return at, now
}

// Komplett descriptor + neutral identitet (godtyckliga ID:n, citerade ID:n,
// repeat-group-fall): producenten bär allt konsumenten behöver utan API-join,
// inga FIWARE-ID:n i källa.
func TestPlan1050CompleteDescriptorNeutralIdentity(t *testing.T) {
	at, now := plan1050Time()
	v := 21.5
	thing := &ThingDescriptorV1{
		ThingID: "sensor-42", Name: "Sensor 42",
		AlternativeName: "BinA", SubType: "WasteContainer", Category: "wastebin",
		Location:  &GeoLocationV1{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata:  map[string]string{"tags": "park, centrum", "description": "neutral"},
		VariantID: "wastebin-B", VariantVersion: "v1",
	}
	delta := []ThingValueV1{{
		Schema: "v1", ThingID: "sensor-42", Tenant: "t",
		PropertyID: "temperature.window", Value: &v, Quality: "ok",
		Unit: "Cel", Source: "s1/3303/5700", MessageID: "m1",
		ObservedAt: at, ReceivedAt: at, ComputedAt: now, PublishedAt: now,
		ConfigRevision: 3, EventID: "v1/t/sensor-42/m1/temperature.window",
	}}
	snapshot := []ThingSnapshotValueV1{
		{PropertyID: "temperature.window", Value: &v, Quality: "ok", Unit: "Cel",
			ObservedAt: at, ReceivedAt: at, Source: "s1/3303/5700", MessageID: "m1", ConfigRevision: 3},
		{PropertyID: "humidity", Value: func() *float64 { x := 55.0; return &x }(), Quality: "uncertain", Unit: "%",
			ObservedAt: at, ReceivedAt: at, Source: "s1/3304/5700", MessageID: "m0", ConfigRevision: 3},
	}
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/sensor-42/m1", Tenant: "t",
		ThingID: "sensor-42", ThingType: "wastebin", ThingTypeVersion: "v1",
		ConfigRevision: 3, PublishedAt: now, Thing: thing,
		Delta: delta, Snapshot: snapshot, Values: delta,
	}
	raw := string(ev.Body())
	for _, want := range []string{
		`"eventId":"v1/t/sensor-42/m1"`,
		`"thingType":"wastebin"`, `"thingTypeVersion":"v1"`,
		`"configRevision":3`,
		`"delta":[`, `"snapshot":[`, `"values":[`,
		`"propertyId":"temperature.window"`, `"propertyId":"humidity"`,
		`"quality":"ok"`, `"quality":"uncertain"`,
		`"unit":"Cel"`,
		`"alternativeName":"BinA"`, `"subType":"WasteContainer"`,
		`"variantId":"wastebin-B"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("kontrakt saknar %s:\n%s", want, raw)
		}
	}
	// Subsekundprecision bevaras (RFC3339Nano, ej trunkerad till sekund).
	if !strings.Contains(raw, "12:00:00.123456") {
		t.Fatalf("subsekundprecision saknas:\n%s", raw)
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// Values-alias == delta (en release).
	if len(ev.Values) != len(ev.Delta) {
		t.Fatalf("values len=%d, delta len=%d (ska vara lika)", len(ev.Values), len(ev.Delta))
	}
	// Snapshot-läsning: current vs senaste delta (tidigare giltigt efter ogiltig).
	if got := ev.SnapshotByProperty("humidity"); got == nil || got.Quality != "uncertain" {
		t.Fatalf("snapshot humidity=%+v, want uncertain", got)
	}
	if got := ev.ValueByProperty("temperature.window"); got == nil {
		t.Fatal("delta value saknas")
	}
	if !ev.ShouldEmitEntity([]string{"temperature.window", "humidity"}) {
		t.Fatal("komplett required med snapshot ska emitteras")
	}
	if ev.TopicName() != TopicThingValues || ev.ContentType() != ContentTypeV1 {
		t.Fatalf("topic/content=%q/%q", ev.TopicName(), ev.ContentType())
	}
}

// Femläges-semantik: ny ogiltig obs -> delta invalid-post, snapshot behåller
// tidigare giltigt (ersätter ALDRIG); noll/false bevaras (nil != 0).
func TestPlan1050InvalidDeltaKeepsSnapshot(t *testing.T) {
	at, now := plan1050Time()
	prev := 18.0
	zero := 0.0
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/gauge-1/m2", Tenant: "t",
		ThingID: "gauge-1", ThingType: "gauge", ThingTypeVersion: "v1",
		ConfigRevision: 2, PublishedAt: now,
		Thing: &ThingDescriptorV1{ThingID: "gauge-1", Name: "Mätare", Category: "gauge"},
		Delta: []ThingValueV1{{
			Schema: "v1", ThingID: "gauge-1", Tenant: "t",
			PropertyID: "distance", Value: nil, Quality: "invalid",
			Unit: "m", Source: "d/3330/5700", MessageID: "m2",
			ObservedAt: at, ReceivedAt: at, ComputedAt: now, PublishedAt: now,
			ConfigRevision: 2, EventID: "v1/t/gauge-1/m2/distance",
		}, {
			Schema: "v1", ThingID: "gauge-1", Tenant: "t",
			PropertyID: "presence", Value: &zero, Quality: "ok",
			Unit: "", Source: "d/3200/5500", MessageID: "m2",
			ObservedAt: at, ReceivedAt: at, ComputedAt: now, PublishedAt: now,
			ConfigRevision: 2, EventID: "v1/t/gauge-1/m2/presence",
		}},
		Snapshot: []ThingSnapshotValueV1{
			{PropertyID: "distance", Value: &prev, Quality: "ok", Unit: "m",
				ObservedAt: at.Add(-time.Hour), ReceivedAt: at.Add(-time.Hour),
				Source: "d/3330/5700", MessageID: "m1", ConfigRevision: 2},
			{PropertyID: "presence", Value: &zero, Quality: "ok", Unit: "",
				ObservedAt: at, ReceivedAt: at, Source: "d/3200/5500", MessageID: "m2", ConfigRevision: 2},
		},
	}
	ev.Values = ev.Delta
	if err := ev.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// Ogiltig delta ersätter aldrig snapshot: snapshot distance fortfarande 18 ok.
	snap := ev.SnapshotByProperty("distance")
	if snap == nil || snap.Value == nil || *snap.Value != 18 || snap.Quality != "ok" {
		t.Fatalf("snapshot ska behålla tidigare giltigt: %+v", snap)
	}
	// Noll bevaras (0, inte null).
	zeroSnap := ev.SnapshotByProperty("presence")
	if zeroSnap == nil || zeroSnap.Value == nil || *zeroSnap.Value != 0 {
		t.Fatalf("noll ska bevaras (nil != 0): %+v", zeroSnap)
	}
	// ShouldEmitEntity mot snapshot (inte senaste ogiltiga delta): required
	// distance finns med ok i snapshot -> emittera (ogiltig delta hoppar
	// property individuellt, men entiteten står kvar på snapshot).
	if !ev.ShouldEmitEntity([]string{"distance"}) {
		t.Fatal("snapshot required ok ska emitteras trots ogiltig delta")
	}
	// Delta invalid hoppar property.
	if ShouldEmitProperty(ev.Delta[0].Quality) {
		t.Fatal("invalid delta ska inte emitteras")
	}
}

// Borttagen egenskap vid config-byte: snapshot saknas + removed[].
func TestPlan1050RemovedProperty(t *testing.T) {
	_, now := plan1050Time()
	ev := ThingValuesEventV1{
		Schema: "v1", EventID: "v1/t/room-a/m3", Tenant: "t",
		ThingID: "room-a", ThingType: "room", ThingTypeVersion: "v2",
		ConfigRevision: 5, PublishedAt: now,
		Thing: &ThingDescriptorV1{ThingID: "room-a", Name: "Rum A", Category: "room"},
		Delta: []ThingValueV1{},
		Snapshot: []ThingSnapshotValueV1{
			{PropertyID: "temperature.window", Quality: "ok", Unit: "Cel",
				ObservedAt: now, ReceivedAt: now, ConfigRevision: 5},
		},
		Removed: []string{"humidity.old"},
		Values:  []ThingValueV1{},
	}
	if ev.SnapshotByProperty("humidity.old") != nil {
		t.Fatal("borttagen ska saknas i snapshot")
	}
	found := false
	for _, r := range ev.Removed {
		if r == "humidity.old" {
			found = true
		}
	}
	if !found {
		t.Fatalf("removed[] ska lista borttagen: %+v", ev.Removed)
	}
}

// Generella relationsplatser: flera mål, målversioner, neutrala identiteter,
// previousToId vid flytt (atomärt remove+set samma revision), removed per länk.
func TestPlan1050GeneralRelationsMultipleTargets(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	fromDesc := &ThingDescriptorV1{ThingID: "room-a", Name: "Rum A", Category: "room"}
	toB := &ThingDescriptorV1{ThingID: "building-a", Name: "Hus A", Category: "building"}
	toC := &ThingDescriptorV1{ThingID: "building-b", Name: "Hus B", Category: "building"}
	// RelationID inkluderar To för unicitet vid flera mål samma revision
	// (annars skulle andra länken dedupliceras; duplikat (tenant,relation,
	// from,to) = idempotent, olika To = distinkta ID:n). Föreslagen
	// precisering till A (baslinjen nämner form utan To; opaka gamla ID:n
	// förblir läsbara).
	set1 := RelationChangeV1{
		RelationID: "v1/t/room-a/relations/monitors/building-a/7/set",
		Type:       "monitors", FromID: "room-a", ToID: "building-a",
		Tenant: "t", Revision: 7, ChangedAt: at,
		FromThingType: "room", FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: "building", ToThingTypeVersion: "v2", To: toB,
	}
	set2 := RelationChangeV1{
		RelationID: "v1/t/room-a/relations/monitors/building-b/7/set",
		Type:       "monitors", FromID: "room-a", ToID: "building-b",
		Tenant: "t", Revision: 7, ChangedAt: at,
		FromThingType: "room", FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: "building", ToThingTypeVersion: "v1", To: toC,
	}
	// Flytt partOf: set med previousToId (samma revision, ingen separat removed).
	move := RelationChangeV1{
		RelationID: "v1/t/room-a/relations/partOf/building-b/8/set",
		Type:       "partOf", FromID: "room-a", ToID: "building-b", PreviousToID: "building-a",
		Tenant: "t", Revision: 8, ChangedAt: at,
		FromThingType: "room", FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: "building", ToThingTypeVersion: "v1", To: toC,
	}
	removed := RelationChangeV1{
		RelationID: "v1/t/room-a/relations/monitors/building-a/9/removed",
		Type:       "monitors", FromID: "room-a", ToID: "building-a", Removed: true,
		Tenant: "t", Revision: 9, ChangedAt: at,
		FromThingType: "room", FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: "building", ToThingTypeVersion: "v2", To: toB,
	}
	for _, ev := range []RelationChangeV1{set1, set2, move, removed} {
		if err := ev.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		raw := string(ev.Body())
		for _, want := range []string{
			`"tenant":"t"`, `"fromId":"room-a"`,
			`"fromThingType":"room"`, `"fromThingTypeVersion":"v1"`,
			`"toThingType":"building"`,
		} {
			if !strings.Contains(raw, want) {
				t.Fatalf("relation saknar %s:\n%s", want, raw)
			}
		}
		if ev.TopicName() != TopicRelationChanges || ev.ContentType() != ContentTypeV1 {
			t.Fatalf("topic/content=%q/%q", ev.TopicName(), ev.ContentType())
		}
	}
	if move.PreviousToID != "building-a" || move.Removed {
		t.Fatalf("flytt ska vara set med previousToId (atomärt): %+v", move)
	}
	if !removed.Removed || removed.To == nil {
		t.Fatalf("removed ska bära sista to-descriptor: %+v", removed)
	}
	// To null tolereras vid läsning (bruten referens -> target:null, ej tyst).
	broken := removed
	broken.To = nil
	var back RelationChangeV1
	raw, _ := json.Marshal(broken)
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("round-trip null To: %v", err)
	}
	if back.To != nil {
		t.Fatal("null To ska överleva round-trip")
	}
}

// Lifecycle med fryst thing + bootstrap-flagga (beskrivande saker utan
// mätningar kan skapas/uppdateras).
func TestPlan1050LifecycleBootstrap(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	for _, kind := range []ThingLifecycleKind{ThingCreated, ThingUpdated, ThingDeleted} {
		ev := ThingLifecycleV1{
			Schema: "v1", EventID: "v1/t/room-a/lifecycle/" + string(kind) + "/4",
			Tenant: "t", ThingID: "room-a", Kind: kind,
			ThingType: "room", ThingTypeVersion: "v1",
			ConfigRevision: 4, ChangedAt: at,
			Thing: &ThingDescriptorV1{ThingID: "room-a", Name: "Rum A", Category: "room"},
		}
		if kind != ThingDeleted {
			ev.Bootstrap = true
		}
		if err := ev.Validate(); err != nil {
			t.Fatalf("validate %s: %v", kind, err)
		}
		raw := string(ev.Body())
		if !strings.Contains(raw, `"kind":"`+string(kind)+`"`) {
			t.Fatalf("lifecycle saknar kind %s:\n%s", kind, raw)
		}
		if ev.TopicName() != TopicLifecycleChanges {
			t.Fatalf("topic=%q", ev.TopicName())
		}
	}
}
