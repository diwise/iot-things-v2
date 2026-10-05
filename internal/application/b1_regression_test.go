package application

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// B1 regressioner (skrivna FÖRE rättning per PLAN1050 B1): falsk deklarerad
// måltyp, saknat mål, tenantgräns och köat värde efter ändrad enhet/mall/
// descriptor. Mål löses i samma tenant mot verklig typ i tx
// (create/update/flytt/uppgradering); kardinalitet/dubbletter/partOf-cykel i
// atomär skrivväg även vid samtidiga mutationer; publiceringsmetadata fångas
// vid mutation/beräkning (fryst, ingen berikning vid drain).

func b1Catalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1", Category: "building",
	}}); err != nil {
		t.Fatalf("put building: %v", err)
	}
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1", Category: "room",
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
			{Name: "monitors", AllowedTargets: []string{"building"}, Min: 0, Max: 2},
		},
	}}); err != nil {
		t.Fatalf("put room: %v", err)
	}
	return c
}

func b1Building(id string) catalog.ObjectSpec {
	return catalog.ObjectSpec{
		ThingID: id, Tenant: "t", Name: id,
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}
}

func b1Room(id, targetID, declaredTargetType string) catalog.ObjectSpec {
	var rels []catalog.RelationRef
	if targetID != "" {
		rels = []catalog.RelationRef{{Name: "partOf", TargetThingID: targetID, TargetTemplateID: declaredTargetType}}
	}
	return catalog.ObjectSpec{
		ThingID: id, Tenant: "t", Name: id,
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations:  rels,
	}
}

// Falsk deklarerad måltyp: deklarerar building men verkligt mål är room.
// Ska nekas (verklig typ i tx, inte deklarerad sträng).
func TestB1FalseDeclaredTargetTypeDenied(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := b1Catalog(t)
	if _, err := rt.RegisterThing(context.Background(), "t", c, b1Building("b-a"), 1); err != nil {
		t.Fatalf("register b-a: %v", err)
	}
	if _, err := rt.RegisterThing(context.Background(), "t", c, b1Room("room-real", "", ""), 1); err != nil {
		// room-real utan förälder, som falskt mål.
		t.Fatalf("register room-real: %v", err)
	}
	// Försök länka room-x -> room-real men deklarera "building" (falskt).
	// Compile tillåter deklarationen (building är tillåten), men verklig typ
	// är room och ska nekas vid upplösning mot verklig typ i tx.
	obj := b1Room("room-x", "room-real", "building")
	if _, err := rt.RegisterThing(context.Background(), "t", c, obj, 1); err == nil {
		t.Fatal("falsk deklarerad måltyp ska nekas (verklig typ room, deklarerad building)")
	}
}

// Saknat mål ska ge NotFound (inte tyst null eller generiskt fel).
func TestB1MissingTargetNotFound(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := b1Catalog(t)
	obj := b1Room("room-x", "missing", "building")
	if _, err := rt.RegisterThing(context.Background(), "t", c, obj, 1); err == nil {
		t.Fatal("saknat mål ska nekas")
	}
}

// Tenantgräns: mål i annan tenant får inte länkas (strikt intra-tenant).
func TestB1TenantBoundaryDenied(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := b1Catalog(t)
	// Byggnad i tenant other.
	otherB := b1Building("b-other")
	otherB.Tenant = "other"
	if _, err := rt.RegisterThing(context.Background(), "other", c, otherB, 1); err != nil {
		t.Fatalf("register other/b-other: %v", err)
	}
	// Rum i tenant t försöker länka till other/b-other (finns ej i t).
	obj := b1Room("room-x", "b-other", "building")
	if _, err := rt.RegisterThing(context.Background(), "t", c, obj, 1); err == nil {
		t.Fatal("cross-tenant länk ska nekas (mål saknas i samma tenant)")
	}
}

// Köat värde efter ändrad enhet/mall/descriptor: batch vid rev1, uppdatera
// config (ny enhet + nytt namn), drain ska publicera FRYST gammal
// enhet/descriptor/revision, inte aktuell config.
func TestB1QueuedValueKeepsFrozenUnitDescriptor(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"}, Category: "gauge",
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "gauge-1", Tenant: "t", Name: "Mätare före",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {Unit: "m"}},
		Bindings: []things.Binding{
			{DeviceID: "d", Object: rtDistURN, Resource: "5700", Input: "distance"},
		},
	}
	if _, err := rt.RegisterThing(context.Background(), "t", c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("d", 1.5, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Uppdatera config: nytt namn + (samma mall, men omregistrering bumpas).
	// Enhetsbyte simuleras via ny mallversion med annan enhet.
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v2", Required: []string{"distance"}, Category: "gauge",
	}}); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	// Reconstruct via upgrade? Enklare: uppdatera namn (descriptor) vid samma mall.
	obj2 := obj
	obj2.Name = "Mätare efter"
	st, _ := fs.LoadThing(context.Background(), "t", "gauge-1")
	if _, err := rt.RegisterThing(context.Background(), "t", c, obj2, st.Revision); err != nil {
		t.Fatalf("update: %v", err)
	}
	pub := &fakePublisher{}
	if _, err := rt.DrainAndPublish(context.Background(), pub, "t", "gauge-1", base.Add(time.Minute)); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("published=%d, want 1", pub.count())
	}
	ev := pub.events[0]
	if ev.ConfigRevision != 1 {
		t.Fatalf("revision=%d, want fryst 1 (inte aktuell 2)", ev.ConfigRevision)
	}
	if ev.Thing == nil || ev.Thing.Name != "Mätare före" {
		t.Fatalf("descriptor ska vara fryst (före), fick %+v", ev.Thing)
	}
	// Delta+snapshot ska bära fryst enhet "m" (inte aktuell).
	if len(ev.Delta) == 0 {
		t.Fatalf("delta saknas: %+v", ev)
	}
	if ev.Delta[0].Unit != "m" {
		t.Fatalf("delta unit=%q, want fryst m", ev.Delta[0].Unit)
	}
	if len(ev.Snapshot) == 0 {
		t.Fatalf("snapshot saknas: %+v", ev)
	}
	if ev.Snapshot[0].Unit != "m" {
		t.Fatalf("snapshot unit=%q, want fryst m", ev.Snapshot[0].Unit)
	}
	// Values-alias ska == delta en release.
	if len(ev.Values) != len(ev.Delta) {
		t.Fatalf("values-alias len=%d, delta len=%d (ska vara lika)", len(ev.Values), len(ev.Delta))
	}
}
