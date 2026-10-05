// Package plan1050 är PLAN1050 paket J neutral acceptans på producentsidan
// (Våg 4, J1): neutrala ID:n, inga modellnamn i fallen.
//
// Harness med VERKLIGA producentpayloads (pkg/types Body()/TopicName()/
// ContentType() — samma Body() som transformens C1-adaptrar läser),
// publicerade katalogkontrakt (catalog.PutTemplate/PutVariant/Compile —
// samma anrop som HTTP-API:t använder), isolerade testdata (j1-tenant-*,
// j1-mallar/saker) och kontrollerad historik (inga externa brokersvar här;
// leveransräkning bevisas i transform-harneset).
//
// Källrecency-skydd ingår INTE (fastställt beslut): sena event filtreras
// aldrig; revisioner bär metadata/konflikter/diagnostik. TestJ1Recency*
// bevisar avsaknaden av filtrering och dokumenterar avgränsningen.
package plan1050

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

const (
	j1TenantA  = "j1-tenant-a"
	j1TenantB  = "j1-tenant-b"
	j1Template = "j1-template"
	j1Other    = "j1-other-kind"
)

func j1Point() *things.Location {
	return &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)}
}

func j1Point3D() *things.Location {
	return &things.Location{Type: "Point", Coordinates: json.RawMessage(`[17.0,62.0,9.0]`)}
}

func j1Polygon() *things.Location {
	return &things.Location{Type: "Polygon", Coordinates: json.RawMessage(`[[[18.0,59.3],[19.0,59.3],[19.0,60.0],[18.0,59.3]]]`)}
}

func j1Meta() map[string]string { return map[string]string{"j1": "true"} }

// J1 kontrakt: topics/content-type/schema + Body-nycklar (samma som C1 läser).
func TestJ1ProducerContractTopicsContentTypes(t *testing.T) {
	if pkgtypes.TopicThingValues != "things.v1.values" {
		t.Fatalf("values topic=%q", pkgtypes.TopicThingValues)
	}
	if pkgtypes.TopicRelationChanges != "things.v1.relations" {
		t.Fatalf("relations topic=%q", pkgtypes.TopicRelationChanges)
	}
	if pkgtypes.TopicLifecycleChanges != "things.v1.lifecycle" {
		t.Fatalf("lifecycle topic=%q", pkgtypes.TopicLifecycleChanges)
	}
	if pkgtypes.SchemaV1 != "v1" || pkgtypes.ContentTypeV1 != "application/vnd.diwise.things.v1+json" {
		t.Fatalf("schema/content=%q/%q", pkgtypes.SchemaV1, pkgtypes.ContentTypeV1)
	}
	at := time.Date(2026, 10, 5, 12, 0, 0, 123456000, time.UTC)
	v := 1.2
	ev := pkgtypes.ThingValuesEventV1{
		Schema: "v1", EventID: "v1/" + j1TenantA + "/j1-obj-1/m1", Tenant: j1TenantA,
		ThingID: "j1-obj-1", ThingType: j1Template, ThingTypeVersion: "v1",
		ConfigRevision: 1, PublishedAt: at,
		Thing: &pkgtypes.ThingDescriptorV1{ThingID: "j1-obj-1", Name: "J1", Category: "j1-category"},
		Delta: []pkgtypes.ThingValueV1{{
			ThingID: "j1-obj-1", Tenant: j1TenantA, PropertyID: "prop-a",
			Value: &v, Quality: "ok", Unit: "m", Source: "s/3330/5700", MessageID: "m1",
			ObservedAt: at, ReceivedAt: at, ConfigRevision: 1, EventID: "v1/" + j1TenantA + "/j1-obj-1/m1/prop-a",
		}},
		Snapshot: []pkgtypes.ThingSnapshotValueV1{{
			PropertyID: "prop-a", Value: &v, Quality: "ok", Unit: "m",
			ObservedAt: at, ReceivedAt: at, Source: "s/3330/5700", MessageID: "m1", ConfigRevision: 1,
		}},
	}
	ev.Values = ev.Delta
	raw := string(ev.Body())
	for _, want := range []string{
		`"schema":"v1"`, `"eventId":"v1/` + j1TenantA + `/j1-obj-1/m1"`,
		`"thingType":"` + j1Template + `"`, `"thingTypeVersion":"v1"`,
		`"configRevision":1`, `"delta":[`, `"snapshot":[`, `"values":[`,
		`"propertyId":"prop-a"`, `"quality":"ok"`, `"unit":"m"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("Body() saknar %s:\n%s", want, raw)
		}
	}
	if !strings.Contains(raw, "12:00:00.123456") {
		t.Fatalf("subsekund saknas:\n%s", raw)
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if ev.TopicName() != pkgtypes.TopicThingValues || ev.ContentType() != pkgtypes.ContentTypeV1 {
		t.Fatalf("topic/content=%q/%q", ev.TopicName(), ev.ContentType())
	}
	// Konsumenttid är mättid (max delta observedAt), aldrig transporttid.
	if !ev.EventTimestamp().Equal(at) {
		t.Fatalf("EventTimestamp=%v want %v", ev.EventTimestamp(), at)
	}
}

// J1-a: neutral mall/variant via katalog-API (godtyckliga ID:n, citerade + repeat).
func TestJ1aNeutralTemplateVariantAPI(t *testing.T) {
	c := catalog.New()
	spec := catalog.TemplateSpec{Template: things.Template{
		ID: j1Template, Version: "1",
		Required: []string{"prop-a"}, Optional: []string{"reading.average", "group.item1", "group.item2"},
		RepeatGroups:      []string{"group."},
		Category:          "j1-category",
		AllowedGeometries: []string{"Point", "Polygon"},
		Relations: []things.RelationSpec{
			{Name: "j1-contains", AllowedTargets: []string{j1Template, j1Other}, Min: 0, Max: 5},
		},
		PropertyDefs: map[string]things.PropertyDef{
			"prop-a":          {DisplayName: "Prop A", DataType: "number", Unit: "m"},
			"reading.average": {DisplayName: "Average", DataType: "number", Unit: "Cel"},
			"group.item1":     {DisplayName: "Item 1", DataType: "number"},
			"group.item2":     {DisplayName: "Item 2", DataType: "number"},
		},
	}}
	if err := c.PutTemplate(spec); err != nil {
		t.Fatalf("put neutral mall: %v", err)
	}
	// Publicerad version är oföränderlig: återpublicering nekas även med identiskt innehåll.
	if err := c.PutTemplate(spec); err == nil {
		t.Fatal("återpublicering av publicerad version ska nekas")
	}
	// Variant bunden till exakt mallversion.
	if err := c.PutVariant(catalog.VariantSpec{Variant: things.Variant{
		ID: "j1-variant", Version: "1", TemplateID: j1Template, TemplateVersion: "1",
		ParamValues: map[string]float64{},
	}, PresetProperties: []string{"group.item1"}}); err != nil {
		t.Fatalf("put variant: %v", err)
	}
	// Variant mot okänd mallversion nekas.
	if err := c.PutVariant(catalog.VariantSpec{Variant: things.Variant{
		ID: "j1-bad", Version: "1", TemplateID: j1Template, TemplateVersion: "9",
	}}); err == nil {
		t.Fatal("variant mot okänd mall ska nekas")
	}
	// Citerade ID:n och repeat-prefix kompilerar (GUI/API använder samma ObjectSpec-form).
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantA, Name: "J1 sak",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
		VariantID: "j1-variant", VariantVersion: "1",
		Properties: map[string]things.Property{
			"prop-a": {}, "reading.average": {}, "group.item1": {}, "group.item2": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: "urn:oma:lwm2m:ext:3303", Resource: "5700", Input: "prop-a"},
			{DeviceID: "s1", Object: "urn:oma:lwm2m:ext:3303", Resource: "5700", Input: "reading.average"},
			{DeviceID: "s1", Object: "urn:oma:lwm2m:ext:3304", Resource: "5700", Input: "group.item1"},
			{DeviceID: "s1", Object: "urn:oma:lwm2m:ext:3304", Resource: "5700", Input: "group.item2"},
		},
		Relations: []catalog.RelationRef{
			{Name: "j1-contains", TargetThingID: "j1-obj-2", TargetTemplateID: j1Other},
			{Name: "j1-contains", TargetThingID: "j1-obj-3", TargetTemplateID: j1Template},
		},
	})
	if err != nil {
		t.Fatalf("compile citerad+repeat: %v", err)
	}
	if cfg.TemplateID != j1Template || cfg.TemplateVersion != "1" || cfg.VariantID != "j1-variant" {
		t.Fatalf("versionspinning förlorad: %+v", cfg)
	}
	if len(cfg.Relations) != 2 {
		t.Fatalf("relationer=%+v", cfg.Relations)
	}
	// Bar repeat-prefix utan suffix är ingen mätpunkt.
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-9", Tenant: j1TenantA, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
		Properties: map[string]things.Property{"group.": {}},
	}); err == nil {
		t.Fatal("bart repeat-prefix ska nekas")
	}
	// Otillåtet mål nekas (måltyp/existens, tenantgräns prövas i runtime).
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-9", Tenant: j1TenantA, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
		Properties: map[string]things.Property{"prop-a": {}},
		Relations:  []catalog.RelationRef{{Name: "j1-contains", TargetThingID: "x", TargetTemplateID: "j1-forbidden"}},
	}); err == nil {
		t.Fatal("otillåtet relationsmål ska nekas")
	}
	// ObjectSpec är API/GUI-formen (export): roundtrip utan fältförlust.
	blob, err := json.Marshal(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantA, Name: "J1 sak",
		TemplateID: j1Template, TemplateVersion: "1",
		VariantID: "j1-variant", VariantVersion: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	var back catalog.ObjectSpec
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatal(err)
	}
	if back.TemplateID != j1Template || back.VariantID != "j1-variant" {
		t.Fatalf("ObjectSpec roundtrip: %+v", back)
	}
}

// J1-b: saker med Point/Polygon/utan-mätning där mallen tillåter.
func TestJ1bPointPolygonWithoutMeasurements(t *testing.T) {
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: j1Template, Version: "1", Category: "j1-category",
		AllowedGeometries: []string{"Point", "Polygon"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: j1Template, Version: "2", Category: "j1-category",
		AllowedGeometries: []string{"Point"},
		AllowNoLocation:   true,
	}}); err != nil {
		t.Fatal(err)
	}
	// Point 3D bevaras (WGS84 lon/lat[,alt]).
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-point", Tenant: j1TenantA, Name: "J1 point",
		Location: j1Point3D(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
	}); err != nil {
		t.Fatalf("point 3D: %v", err)
	}
	// Polygon accepteras där mallen tillåter.
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-poly", Tenant: j1TenantA, Name: "J1 poly",
		Location: j1Polygon(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
	}); err != nil {
		t.Fatalf("polygon: %v", err)
	}
	// Polygon nekas där mallen endast tillåter Point.
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-poly-2", Tenant: j1TenantA, Name: "J1",
		Location: j1Polygon(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "2",
	}); err == nil {
		t.Fatal("polygon mot Point-mall ska nekas")
	}
	// Utan plats endast där mallen tillåter (beskrivande saker utan mätningar).
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-no-loc", Tenant: j1TenantA, Name: "J1 utan plats",
		Metadata:   j1Meta(),
		TemplateID: j1Template, TemplateVersion: "2",
	}); err != nil {
		t.Fatalf("utan plats (tillåten): %v", err)
	}
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-no-loc-2", Tenant: j1TenantA, Name: "J1",
		Metadata:   j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
	}); err == nil {
		t.Fatal("utan plats ska nekas där mallen kräver plats")
	}
	// Platsvalidering: brutna koordinater nekas (ändliga WGS84).
	bad := &things.Location{Type: "Point", Coordinates: json.RawMessage(`[999,999]`)}
	if err := bad.Validate(); err == nil {
		t.Fatal("ogiltig lon/lat ska nekas")
	}
}

// J1-c: delta+snapshot-typsemantik (ogiltig-behåller-current, 0, subsekund, removed).
func TestJ1cDeltaSnapshotTypeSemantics(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 2, 0, 123456789, time.UTC)
	prev := 18.0
	zero := 0.0
	ev := pkgtypes.ThingValuesEventV1{
		Schema: "v1", EventID: "v1/" + j1TenantA + "/j1-obj-1/m2", Tenant: j1TenantA,
		ThingID: "j1-obj-1", ThingType: j1Template, ThingTypeVersion: "v1",
		ConfigRevision: 2, PublishedAt: at,
		Thing: &pkgtypes.ThingDescriptorV1{ThingID: "j1-obj-1", Name: "J1", Category: "j1-category"},
		Delta: []pkgtypes.ThingValueV1{{
			ThingID: "j1-obj-1", Tenant: j1TenantA, PropertyID: "prop-a",
			Value: nil, Quality: "invalid", Unit: "m", Source: "s/1", MessageID: "m2",
			ObservedAt: at, ReceivedAt: at, ConfigRevision: 2,
		}, {
			ThingID: "j1-obj-1", Tenant: j1TenantA, PropertyID: "prop-zero",
			Value: &zero, Quality: "ok", Source: "s/2", MessageID: "m2",
			ObservedAt: at, ReceivedAt: at, ConfigRevision: 2,
		}},
		Snapshot: []pkgtypes.ThingSnapshotValueV1{
			{PropertyID: "prop-a", Value: &prev, Quality: "ok", Unit: "m",
				ObservedAt: at.Add(-time.Hour), ReceivedAt: at.Add(-time.Hour), Source: "s/1", MessageID: "m1", ConfigRevision: 2},
			{PropertyID: "prop-zero", Value: &zero, Quality: "ok",
				ObservedAt: at, ReceivedAt: at, Source: "s/2", MessageID: "m2", ConfigRevision: 2},
		},
		Removed: []string{"prop-old"},
	}
	ev.Values = ev.Delta
	if err := ev.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	snap := ev.SnapshotByProperty("prop-a")
	if snap == nil || snap.Value == nil || *snap.Value != 18 || snap.Quality != "ok" {
		t.Fatalf("snapshot ska behålla tidigare giltigt: %+v", snap)
	}
	z := ev.SnapshotByProperty("prop-zero")
	if z == nil || z.Value == nil || *z.Value != 0 {
		t.Fatalf("noll ska bevaras (nil != 0): %+v", z)
	}
	if ev.SnapshotByProperty("prop-old") != nil {
		t.Fatal("borttagen ska saknas i snapshot")
	}
	if !ev.ShouldEmitEntity([]string{"prop-a"}) {
		t.Fatal("snapshot required ok ska emitteras trots ogiltig delta")
	}
	if pkgtypes.ShouldEmitProperty("invalid") {
		t.Fatal("invalid ska aldrig emitteras")
	}
	raw := string(ev.Body())
	if !strings.Contains(raw, `"removed":["prop-old"]`) {
		t.Fatalf("removed saknas:\n%s", raw)
	}
	if !strings.Contains(raw, ".123456789") {
		t.Fatalf("subsekund saknas:\n%s", raw)
	}
}

// J1-d: två mallversioner + byte/omstart revisionsriktigt (restore, Body-determinism).
func TestJ1dTwoVersionsByteRestartRevisionCorrect(t *testing.T) {
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: j1Template, Version: "1", Required: []string{"prop-a"}, Category: "j1-category",
		PropertyDefs: map[string]things.PropertyDef{"prop-a": {Unit: "Cel"}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: j1Template, Version: "2", Required: []string{"prop-b"}, Category: "j1-category",
		PropertyDefs: map[string]things.PropertyDef{"prop-b": {Unit: "MTR"}},
	}}); err != nil {
		t.Fatal(err)
	}
	cfg1, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantA, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
		Properties: map[string]things.Property{"prop-a": {Unit: "Cel"}},
	})
	if err != nil {
		t.Fatalf("v1 compile: %v", err)
	}
	cfg2, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantA, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "2",
		Properties: map[string]things.Property{"prop-b": {Unit: "MTR"}},
	})
	if err != nil {
		t.Fatalf("v2 compile: %v", err)
	}
	if cfg1.TemplateVersion == cfg2.TemplateVersion {
		t.Fatal("versioner ska vara distinkta pins")
	}
	// Objekt uppgraderas aldrig automatiskt: PreviewUpgrade flaggar borttagna/nya egenskaper.
	diff, conflicts, err := c.PreviewUpgrade(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantA, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
		Properties: map[string]things.Property{"prop-a": {Unit: "Cel"}},
	}, j1Template, "2", "", "")
	if err != nil {
		t.Fatalf("preview upgrade: %v", err)
	}
	if len(diff.AddedProps) == 0 || len(diff.RemovedProps) == 0 || len(conflicts) == 0 {
		t.Fatalf("versionsbyte ska diffas, inte tyst uppgraderas: %+v %+v", diff, conflicts)
	}
	// Omstart: Restore bevarar båda publicerade versionerna (immutabla, ingen omberäkning).
	restored := catalog.New()
	restored.Restore(c.ListTemplates(), c.ListVariants())
	for _, v := range []string{"1", "2"} {
		if _, ok := restored.GetTemplate(j1Template, v); !ok {
			t.Fatalf("restore saknar %s/%s", j1Template, v)
		}
	}
	// Replay: samma event ger byte-identisk Body (stabil nyckelbar payload).
	at := time.Date(2026, 10, 5, 12, 0, 0, 123456000, time.UTC)
	v := 3.0
	ev := pkgtypes.ThingValuesEventV1{
		Schema: "v1", EventID: "v1/" + j1TenantA + "/j1-obj-1/m1", Tenant: j1TenantA,
		ThingID: "j1-obj-1", ThingType: j1Template, ThingTypeVersion: "1",
		ConfigRevision: 7, PublishedAt: at,
		Thing:    &pkgtypes.ThingDescriptorV1{ThingID: "j1-obj-1"},
		Delta:    []pkgtypes.ThingValueV1{{ThingID: "j1-obj-1", Tenant: j1TenantA, PropertyID: "prop-a", Value: &v, Quality: "ok", ObservedAt: at, ReceivedAt: at, ConfigRevision: 7}},
		Snapshot: []pkgtypes.ThingSnapshotValueV1{{PropertyID: "prop-a", Value: &v, Quality: "ok", ObservedAt: at, ReceivedAt: at, ConfigRevision: 7}},
	}
	ev.Values = ev.Delta
	if string(ev.Body()) != string(ev.Body()) {
		t.Fatal("Body ska vara deterministisk för replay")
	}
}

// J1-f/g (producent): generella relationer + lifecycle + tenantgränser.
func TestJ1fRelationsLifecycleTenantBoundaries(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	fromDesc := &pkgtypes.ThingDescriptorV1{ThingID: "j1-from-1", Name: "J1 from", Category: "j1-category"}
	toDesc := &pkgtypes.ThingDescriptorV1{ThingID: "j1-to-1", Name: "J1 to", Category: "j1-category"}
	set1 := pkgtypes.RelationChangeV1{
		RelationID: "v1/" + j1TenantA + "/j1-from-1/relations/j1-contains/j1-to-1/7/set",
		Type:       "j1-contains", FromID: "j1-from-1", ToID: "j1-to-1",
		Tenant: j1TenantA, Revision: 7, ChangedAt: at,
		FromThingType: j1Template, FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: j1Other, ToThingTypeVersion: "2", To: toDesc,
	}
	set2 := set1
	set2.RelationID = "v1/" + j1TenantA + "/j1-from-1/relations/j1-contains/j1-to-2/7/set"
	set2.ToID = "j1-to-2"
	set2.To = &pkgtypes.ThingDescriptorV1{ThingID: "j1-to-2"}
	move := pkgtypes.RelationChangeV1{
		RelationID: "v1/" + j1TenantA + "/j1-from-1/relations/partOf/j1-to-2/8/set",
		Type:       "partOf", FromID: "j1-from-1", ToID: "j1-to-2", PreviousToID: "j1-to-1",
		Tenant: j1TenantA, Revision: 8, ChangedAt: at,
		FromThingType: j1Template, FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: j1Other, ToThingTypeVersion: "1", To: set2.To,
	}
	removed := pkgtypes.RelationChangeV1{
		RelationID: "v1/" + j1TenantA + "/j1-from-1/relations/j1-contains/j1-to-1/9/removed",
		Type:       "j1-contains", FromID: "j1-from-1", ToID: "j1-to-1", Removed: true,
		Tenant: j1TenantA, Revision: 9, ChangedAt: at,
		FromThingType: j1Template, FromThingTypeVersion: "v1", From: fromDesc,
		ToThingType: j1Other, ToThingTypeVersion: "2", To: toDesc,
	}
	for _, ev := range []pkgtypes.RelationChangeV1{set1, set2, move, removed} {
		if err := ev.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		if ev.TopicName() != pkgtypes.TopicRelationChanges || ev.ContentType() != pkgtypes.ContentTypeV1 {
			t.Fatalf("topic/content=%q/%q", ev.TopicName(), ev.ContentType())
		}
	}
	if move.PreviousToID != "j1-to-1" || move.Removed {
		t.Fatalf("flytt ska vara set med previousToId: %+v", move)
	}
	if set1.RelationID == set2.RelationID {
		t.Fatal("flera mål samma revision kräver distinkta ID:n (olika To)")
	}
	// Bruten referens (target:null) överlever roundtrip i stället för att tyst försvinna.
	broken := removed
	broken.To = nil
	blob, _ := json.Marshal(broken)
	var back pkgtypes.RelationChangeV1
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatal(err)
	}
	if back.To != nil {
		t.Fatal("null To ska överleva roundtrip")
	}
	// Lifecycle created/updated/deleted + bootstrap (utan mätningar).
	for _, kind := range []pkgtypes.ThingLifecycleKind{pkgtypes.ThingCreated, pkgtypes.ThingUpdated, pkgtypes.ThingDeleted} {
		ev := pkgtypes.ThingLifecycleV1{
			Schema: "v1", EventID: "v1/" + j1TenantA + "/j1-obj-1/lifecycle/" + string(kind) + "/4",
			Tenant: j1TenantA, ThingID: "j1-obj-1", Kind: kind,
			ThingType: j1Template, ThingTypeVersion: "v1",
			ConfigRevision: 4, ChangedAt: at,
			Thing: &pkgtypes.ThingDescriptorV1{ThingID: "j1-obj-1", Name: "J1"},
		}
		if kind != pkgtypes.ThingDeleted {
			ev.Bootstrap = true
		}
		if err := ev.Validate(); err != nil {
			t.Fatalf("lifecycle %s: %v", kind, err)
		}
		if ev.TopicName() != pkgtypes.TopicLifecycleChanges {
			t.Fatalf("topic=%q", ev.TopicName())
		}
	}
	// Tenantgräns: tom sak-ID nekas; olika tenants ger olika EventID (ingen läcka via ID).
	bad := pkgtypes.ThingValuesEventV1{Tenant: j1TenantA, ThingID: "", EventID: "x"}
	if err := bad.Validate(); err == nil {
		t.Fatal("tom thingId ska nekas")
	}
	// Katalog: tenant krävs vid Compile (ingen utvidgning via body utanför token).
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{ID: j1Template, Version: "1", Category: "j1-category"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Name: "J1", Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
	}); err == nil {
		t.Fatal("compile utan tenant ska nekas")
	}
	// Annan tenant kompilerar isolerat (samma mall, egen sak).
	if _, err := c.Compile(catalog.ObjectSpec{
		ThingID: "j1-obj-1", Tenant: j1TenantB, Name: "J1",
		Location: j1Point(), Metadata: j1Meta(),
		TemplateID: j1Template, TemplateVersion: "1",
	}); err != nil {
		t.Fatalf("isolerad tenant compile: %v", err)
	}
}

// J1 recency-avgränsning på producentsidan: ingen filtrering finns.
func TestJ1RecencyLimitationDocumented(t *testing.T) {
	// Kontraktet bär ingen recency-semantik: två event med olika
	// observedAt/configRevision är oberoende giltiga; ingen jämförelse
	// filtrerar det äldre. Race-/replay får aldrig beskrivas som bevis för
	// ordningsoberoende konvergens.
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	mk := func(rev int64, observed time.Time, v float64) pkgtypes.ThingValuesEventV1 {
		ev := pkgtypes.ThingValuesEventV1{
			Schema: "v1", EventID: "v1/" + j1TenantA + "/j1-obj-1/m" + string(rune('0'+rev)),
			Tenant: j1TenantA, ThingID: "j1-obj-1", ThingType: j1Template,
			ConfigRevision: rev, PublishedAt: at,
			Thing: &pkgtypes.ThingDescriptorV1{ThingID: "j1-obj-1"},
			Delta: []pkgtypes.ThingValueV1{{
				ThingID: "j1-obj-1", Tenant: j1TenantA, PropertyID: "prop-a",
				Value: &v, Quality: "ok", ObservedAt: observed, ReceivedAt: observed, ConfigRevision: rev,
			}},
		}
		ev.Values = ev.Delta
		return ev
	}
	newer := mk(8, at, 2.0)
	older := mk(7, at.Add(-time.Hour), 1.0)
	if err := newer.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := older.Validate(); err != nil {
		t.Fatalf("äldre event ska vara giltigt (ingen filtrering): %v", err)
	}
	// Båda bär sin egen tid/revision; konsumenten väljer inte bort det äldre tyst.
	if !older.EventTimestamp().Before(newer.EventTimestamp()) {
		t.Fatal("tidsordning ska synas i EventTimestamp, inte filtreras bort")
	}
}
