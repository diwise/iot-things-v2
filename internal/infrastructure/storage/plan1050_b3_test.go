package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

// PLAN1050 B3 verifiering med isolerad PostgreSQL (ocachad, -count=1):
// rollback, cancellation, startfel, omstart och samtidig mutation.
// Isolerade testdata per test (separata tenants) så att parallella workers
// inte kolliderar; verifiera att testen KÖRDES (ej skippades) via PG.
// Race-täckning körs via `go test -race` (samtidighet/livscykel).

func b3Catalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"}, Category: "gauge",
	}}); err != nil {
		t.Fatalf("put gauge: %v", err)
	}
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "node", Version: "v1", Category: "node",
		Relations: []things.RelationSpec{{Name: "partOf", AllowedTargets: []string{"node"}, Min: 0, Max: 1}},
	}}); err != nil {
		t.Fatalf("put node: %v", err)
	}
	return c
}

func b3Loc() *things.Location {
	return &things.Location{Type: "Point", Coordinates: []byte(`[18.06,59.33]`)}
}

func b3Meta() map[string]string { return map[string]string{"b3": "true"} }

// Rollback: cykel nekas atomärt i PG - blob, revision, länkar och outbox orörda.
func TestPlan1050B3PgRollbackCycleDenied(t *testing.T) {
	s := pgTestStore(t)
	tenant := "b3-rollback"
	ctx := context.Background()
	c := b3Catalog(t)
	rt := application.NewRuntime(application.RuntimeStore(s))
	mk := func(id, parent string) catalog.ObjectSpec {
		obj := catalog.ObjectSpec{ThingID: id, Tenant: tenant, Name: id,
			Location: b3Loc(), Metadata: b3Meta(),
			TemplateID: "node", TemplateVersion: "v1",
			Properties: map[string]things.Property{}}
		if parent != "" {
			obj.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "node"}}
		}
		return obj
	}
	if _, err := rt.RegisterThing(ctx, tenant, c, mk("a", ""), 1); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if _, err := rt.RegisterThing(ctx, tenant, c, mk("b", "a"), 1); err != nil {
		t.Fatalf("register b->a: %v", err)
	}
	stBefore, _ := application.RuntimeStore(s).LoadThing(ctx, tenant, "a")
	rawBefore := len(application.RuntimeStore(s).PendingRawEvents(ctx, tenant, "a")) +
		len(application.RuntimeStore(s).PendingRawEvents(ctx, tenant, "b"))
	// a->b skulle sluta cykeln a->b->a.
	if _, err := rt.RegisterThing(ctx, tenant, c, mk("a", "b"), stBefore.Revision); err == nil {
		t.Fatal("cycle must be denied (rollback)")
	}
	stAfter, ok := application.RuntimeStore(s).LoadThing(ctx, tenant, "a")
	if !ok || stAfter.Revision != stBefore.Revision {
		t.Fatalf("blob must survive denied cycle (rollback): before=%d after=%+v", stBefore.Revision, stAfter)
	}
	rawAfter := len(application.RuntimeStore(s).PendingRawEvents(ctx, tenant, "a")) +
		len(application.RuntimeStore(s).PendingRawEvents(ctx, tenant, "b"))
	if rawAfter != rawBefore {
		t.Fatalf("denied cycle must queue no events (rollback): before=%d after=%d", rawBefore, rawAfter)
	}
}

// Cancellation: avbruten ctx under CommitBatch lämnar inget (rollback, inga
// partiella blob/historik/outbox/dedup).
func TestPlan1050B3PgCancellationRollback(t *testing.T) {
	s := pgTestStore(t)
	tenant := "b3-cancel"
	ctx := context.Background()
	c := b3Catalog(t)
	rt := application.NewRuntime(application.RuntimeStore(s))
	obj := catalog.ObjectSpec{ThingID: "g1", Tenant: tenant, Name: "Mätare",
		Location: b3Loc(), Metadata: b3Meta(),
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}},
		Bindings:   []things.Binding{{DeviceID: "d", Object: "urn:oma:lwm2m:ext:3330", Resource: "5700", Input: "distance"}}}
	if _, err := rt.RegisterThing(ctx, tenant, c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Avbryt före batch.
	v := 1.5
	obs := things.Observation{
		Signal: things.ParseSignalID("d", "", "urn:oma:lwm2m:ext:3330", "5700"),
		Value:  &v, Quality: things.QualityOK,
		ObservedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
	}
	if _, err := rt.HandleBatch(cancelCtx, tenant, "m-cancel", []things.Observation{obs}, obs.ObservedAt); err == nil {
		t.Fatal("cancelled ctx must fail (no silent success)")
	}
	// Inget synligt: ingen historik, ingen pending, ingen dedup.
	store := application.RuntimeStore(s)
	if n := len(store.History(ctx, tenant, "g1", "", time.Time{}, time.Time{}, 0, 0)); n != 0 {
		t.Fatalf("history=%d after cancelled batch, want 0 (rollback)", n)
	}
	if pending := store.PendingEvents(ctx, tenant, "g1"); len(pending) != 0 {
		t.Fatalf("pending=%d after cancelled batch, want 0 (rollback)", len(pending))
	}
	if store.DedupCheck(ctx, tenant, "m-cancel", "g1") {
		t.Fatal("dedup must not be marked after cancellation (rollback)")
	}
}

// Startfel: felaktig anslutningssträng nekas snabbt (ingen hängning, inget
// partiellt tillstånd). Separat från NewPostgres Ping-timeout.
func TestPlan1050B3PgStartErrorFastFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	bad := NewConfig("127.0.0.1", "postgres", "wrong", "59999", "diwise", "disable")
	if _, err := NewPostgres(ctx, bad); err == nil {
		t.Fatal("bad conn must fail fast (startfel)")
	}
}

// Omstart med fryst revision: batch rev1, update till rev2, omstart (nytt
// Runtime över samma PgStore), drain publicerar fortfarande rev1 fryst.
func TestPlan1050B3PgRestartFrozenRevision(t *testing.T) {
	s := pgTestStore(t)
	tenant := "b3-restart"
	ctx := context.Background()
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"}, Category: "gauge",
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	rt1 := application.NewRuntime(application.RuntimeStore(s))
	obj := catalog.ObjectSpec{ThingID: "g1", Tenant: tenant, Name: "Före",
		Location: b3Loc(), Metadata: b3Meta(),
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {Unit: "m"}},
		Bindings:   []things.Binding{{DeviceID: "d", Object: "urn:oma:lwm2m:ext:3330", Resource: "5700", Input: "distance"}}}
	if _, err := rt1.RegisterThing(ctx, tenant, c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 2.5
	obs := things.Observation{
		Signal: things.ParseSignalID("d", "", "urn:oma:lwm2m:ext:3330", "5700"),
		Value:  &v, Unit: "m", Quality: things.QualityOK,
		ObservedAt: base, ReceivedAt: base,
	}
	if _, err := rt1.HandleBatch(ctx, tenant, "m1", []things.Observation{obs}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Config-byte före drain (rev1 -> rev2, nytt namn).
	obj2 := obj
	obj2.Name = "Efter"
	st, _ := application.RuntimeStore(s).LoadThing(ctx, tenant, "g1")
	if _, err := rt1.RegisterThing(ctx, tenant, c, obj2, st.Revision); err != nil {
		t.Fatalf("update: %v", err)
	}
	// Omstart: nytt Runtime över samma PgStore.
	rt2 := application.NewRuntime(application.RuntimeStore(s))
	pub := &b3Publisher{}
	if _, err := rt2.DrainAndPublish(ctx, pub, tenant, "g1", base.Add(time.Minute)); err != nil {
		t.Fatalf("drain after restart: %v", err)
	}
	if len(pub.values) != 1 {
		t.Fatalf("published=%d, want 1", len(pub.values))
	}
	ev := pub.values[0]
	if ev.ConfigRevision != 1 {
		t.Fatalf("revision=%d, want fryst 1 efter omstart (inte aktuell 2)", ev.ConfigRevision)
	}
	if ev.Thing == nil || ev.Thing.Name != "Före" {
		t.Fatalf("descriptor ska vara fryst (Före) efter omstart: %+v", ev.Thing)
	}
	if len(ev.Delta) == 0 || ev.Delta[0].Unit != "m" {
		t.Fatalf("delta unit ska vara fryst m: %+v", ev.Delta)
	}
	if len(ev.Snapshot) == 0 || ev.Snapshot[0].Unit != "m" {
		t.Fatalf("snapshot unit ska vara fryst m: %+v", ev.Snapshot)
	}
}

// Samtidig mutation mellan instanser (race): 10 batchar över 2 Runtime över
// samma PgStore konvergerar utan förlorade skrivningar (senaste mättid vinner).
func TestPlan1050B3PgConcurrentBatches(t *testing.T) {
	s := pgTestStore(t)
	tenant := "b3-concurrent"
	ctx := context.Background()
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"}, Category: "gauge",
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	rtA := application.NewRuntime(application.RuntimeStore(s))
	obj := catalog.ObjectSpec{ThingID: "g1", Tenant: tenant, Name: "Mätare",
		Location: b3Loc(), Metadata: b3Meta(),
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}},
		Bindings:   []things.Binding{{DeviceID: "d", Object: "urn:oma:lwm2m:ext:3330", Resource: "5700", Input: "distance"}}}
	if _, err := rtA.RegisterThing(ctx, tenant, c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	rtB := application.NewRuntime(application.RuntimeStore(s))
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	errs := make([]error, 10)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			at := base.Add(time.Duration(i) * time.Minute)
			v := float64(i)
			obs := things.Observation{
				Signal: things.ParseSignalID("d", "", "urn:oma:lwm2m:ext:3330", "5700"),
				Value:  &v, Quality: things.QualityOK, ObservedAt: at, ReceivedAt: at,
			}
			rt := rtA
			if i%2 == 1 {
				rt = rtB
			}
			_, errs[i] = rt.HandleBatch(ctx, tenant, itoaB3(i), []things.Observation{obs}, at)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	st, ok := application.RuntimeStore(s).LoadThing(ctx, tenant, "g1")
	if !ok {
		t.Fatal("thing missing after concurrent batches")
	}
	// Äldre mättid skriver aldrig över nyare: slutvärde är max observerat (9).
	var curVal float64
	if cur, ok := st.Current["distance"]; !ok || cur.Value == nil {
		t.Fatalf("current saknas: %+v", st.Current)
	} else {
		curVal = *cur.Value
	}
	if curVal != 9 {
		t.Fatalf("current=%v, want 9 (latest observedAt wins across instances)", curVal)
	}
}

func itoaB3(i int) string {
	return string(rune('a'+i)) + "b3"
}

// b3Publisher samlar values-events för B3-verifiering.
type b3Publisher struct {
	mu     sync.Mutex
	values []pkgtypes.ThingValuesEventV1
}

func (p *b3Publisher) PublishThingValues(_ context.Context, ev pkgtypes.ThingValuesEventV1) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.values = append(p.values, ev)
	return nil
}

func (p *b3Publisher) PublishMessage(_ context.Context, _ messaging.TopicMessage) error {
	return nil
}
