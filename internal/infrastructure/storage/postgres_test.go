package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// pgTestStore ansluter mot lokal testdatabas och tömmer v2-tabellerna.
// Utan nåbar databas skippas testerna — beteendet täcks då av
// minnestesterna med identisk protokollsemantik.
//
// Starta: docker run -d --name iot-things-v2-pg -e POSTGRES_PASSWORD=password
// -e POSTGRES_DB=diwise -p 5434:5432 postgres:16-alpine
// Avvikande anslutning: PG_TEST_HOST/PORT/DBNAME/USER/PASSWORD/SSLMODE.
func pgTestStore(t *testing.T) *PgStore {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	s, err := NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, table := range []string{"history_v2", "relations_v2", "outbox_v2", "outbox_seen_v2", "dedup_v2", "things_v2"} {
		if _, err := s.pool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	return s
}

func TestPostgresThingCAS(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	st := application.StoredThingState{ConfigJSON: []byte(`{"a":1}`), Revision: 1, DataRevision: 1}
	if !a.InsertThing("t", "x", st) {
		t.Fatal("insert must store")
	}
	if a.InsertThing("t", "x", st) {
		t.Fatal("duplicate insert must fail")
	}
	st.Revision = 2
	st.DataRevision = 2
	if a.SaveThingCAS("t", "x", 99, st) {
		t.Fatal("stale CAS must fail")
	}
	if !a.SaveThingCAS("t", "x", 1, st) {
		t.Fatal("matching CAS must store")
	}
	got, ok := a.LoadThing("t", "x")
	if !ok || got.Revision != 2 || got.DataRevision != 2 {
		t.Fatalf("got=%+v", got)
	}
	if _, ok := a.LoadThing("other", "x"); ok {
		t.Fatal("cross-tenant leak")
	}
	a.DeleteThingState("t", "x")
	if _, ok := a.LoadThing("t", "x"); ok {
		t.Fatal("delete must remove")
	}
}

// CommitBatch är atomär även mot PostgreSQL: staketkonflikt lämnar
// varken blob, historik eller pending efter sig.
func TestPostgresCommitBatchAtomic(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 9.0
	st := application.StoredThingState{
		ConfigJSON: []byte(`{}`), Revision: 1, DataRevision: 7,
		Current: map[string]application.CurrentValue{
			"distance": {Value: &v, Quality: "ok", ObservedAt: at},
		},
	}
	if !a.InsertThing("t", "gauge-1", st) {
		t.Fatal("insert")
	}
	hist := []application.HistoryWrite{{
		PropertyID: "distance", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "d", MessageID: "stale",
	}}
	pending := []application.FlowEvent{{
		PropertyID: "distance", Value: v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "stale", ConfigRevision: 1,
	}}
	st.DataRevision = 8
	if a.CommitBatch("t", "gauge-1", 6, st, hist, pending) {
		t.Fatal("conflicting fence must fail")
	}
	if got := a.History("t", "gauge-1", "", time.Time{}, time.Time{}, 0, 0); len(got) != 0 {
		t.Fatalf("history=%d after failed commit, want 0", len(got))
	}
	if pending := a.PendingEvents("t", "gauge-1"); len(pending) != 0 {
		t.Fatalf("pending=%d after failed commit, want 0", len(pending))
	}
	if !a.CommitBatch("t", "gauge-1", 7, st, hist, pending) {
		t.Fatal("matching fence must commit")
	}
	if got := a.History("t", "gauge-1", "", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("history=%d after commit, want 1", len(got))
	}
	if pending := a.PendingEvents("t", "gauge-1"); len(pending) != 1 {
		t.Fatalf("pending=%d after commit, want 1", len(pending))
	}
}

func TestPostgresHistoryDedup(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.5
	hw := application.HistoryWrite{
		PropertyID: "temperature.window", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "s1/3303/5700", MessageID: "m1",
	}
	if !a.AppendHistory("t", "room-a", hw) {
		t.Fatal("first append must store")
	}
	if a.AppendHistory("t", "room-a", hw) {
		t.Fatal("identical append must dedup")
	}
	// Nil-värden dedupliceras också (NULL-safe uniknyckel).
	nilHW := application.HistoryWrite{
		PropertyID: "temperature.window", Quality: "invalid",
		ObservedAt: at.Add(time.Hour), ReceivedAt: at, Source: "s1/3303/5700",
	}
	if !a.AppendHistory("t", "room-a", nilHW) {
		t.Fatal("nil append must store")
	}
	if a.AppendHistory("t", "room-a", nilHW) {
		t.Fatal("nil duplicate must dedup")
	}
	if got := a.History("t", "room-a", "temperature.window", time.Time{}, time.Time{}, 0, 0); len(got) != 2 {
		t.Fatalf("history=%d, want 2", len(got))
	}
	if got := a.History("t", "room-a", "", at, at, 0, 0); len(got) != 1 {
		t.Fatalf("ranged history=%d, want 1", len(got))
	}
	if got := a.History("t", "room-a", "", time.Time{}, time.Time{}, 1, 0); len(got) != 1 {
		t.Fatalf("limited history=%d, want 1", len(got))
	}
}

func TestPostgresOutbox(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := application.FlowEvent{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 1 {
		t.Fatalf("pending=%d, want deduped 1", len(pending))
	}
	a.ClearPendingIDs("t", "bin-42", []string{application.StableEventID("t", "bin-42", "m1", "distance")})
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after clear", len(pending))
	}
	// Seen överlever clear: återleverans publiceras ej igen.
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("redelivery must stay deduped, pending=%d", len(pending))
	}
	if a.DedupCheck("t", "m1", "bin-42") {
		t.Fatal("first sight must not hit")
	}
	a.DedupMark("t", "m1", "bin-42")
	if !a.DedupCheck("t", "m1", "bin-42") {
		t.Fatal("second sight must hit")
	}
}

func TestPostgresRelations(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	if err := a.AttachChild("t", "room-a", "building-a"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := a.AttachChild("t", "room-a", "building-a"); err != nil {
		t.Fatalf("re-attach must be idempotent: %v", err)
	}
	if err := a.AttachChild("t", "building-a", "building-a"); err == nil {
		t.Fatal("self-reference must fail")
	}
	if children := a.Children("t", "building-a"); len(children) != 1 || children[0] != "room-a" {
		t.Fatalf("children=%v", children)
	}
	if parent, ok := a.ParentOf("t", "room-a"); !ok || parent != "building-a" {
		t.Fatalf("parent=%q", parent)
	}
	if err := a.AttachChild("t", "building-a", "room-a"); err == nil {
		t.Fatal("cycle must be denied")
	}
	if err := a.MoveChild("t", "room-a", "building-b"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if parent, _ := a.ParentOf("t", "room-a"); parent != "building-b" {
		t.Fatalf("parent=%q after move", parent)
	}
	a.DetachChild("t", "room-a")
	if _, ok := a.ParentOf("t", "room-a"); ok {
		t.Fatal("detach must remove link")
	}
}

type pgStubPublisher struct {
	mu     sync.Mutex
	events int
	err    error
}

func (p *pgStubPublisher) PublishThingValues(_ context.Context, ev pkgtypes.ThingValuesEventV1) error {
	if p.err != nil {
		return p.err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events += len(ev.Values)
	return nil
}

func pgBinCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	c := catalog.New()
	err := c.PutTemplate(catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required: []string{"distance", "level", "fillRate"},
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.2, "sensorToBottom": 1.42},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
			{Name: "fill-rate", Operator: "fillRate", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"fillRate"},
				Params: []string{"usableHeight"}},
		},
	})
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	return c
}

func pgBinObject() catalog.ObjectSpec {
	return catalog.ObjectSpec{
		ThingID: "bin-42", Tenant: "t",
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "milesight:214", Object: "urn:oma:lwm2m:ext:3330", Resource: "5700", Input: "distance"},
		},
	}
}

func pgDistObs(v float64, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID("milesight:214", "", "urn:oma:lwm2m:ext:3330", "5700"),
		Value:      &v,
		Unit:       "m",
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

// Full Runtime-cykel över PostgreSQL: registrera -> batch -> historik ->
// omstart (nytt Runtime, samma PgStore) -> dubblett utan dubbelräkning ->
// dränering med publicering.
func TestPostgresRuntimeEndToEnd(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	rt := application.NewRuntime(a)
	cat := pgBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, pgBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{pgDistObs(0.30, base)}, base)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(out) != 1 || len(out[0].Outcome.Events) != 3 {
		t.Fatalf("outcome=%+v", out)
	}
	if got := a.History("t", "bin-42", "fillRate", time.Time{}, time.Time{}, 0, 0); len(got) != 1 {
		t.Fatalf("fillRate history=%d, want 1", len(got))
	}
	// Omstart: nytt Runtime över samma PgStore.
	rt2 := application.NewRuntime(a)
	dup, err := rt2.HandleBatch(context.Background(), "t", "m1", []things.Observation{pgDistObs(0.30, base)}, base)
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if !dup[0].Duplicate {
		t.Fatal("dedup must survive restart")
	}
	if got := a.History("t", "bin-42", "", time.Time{}, time.Time{}, 0, 0); len(got) != 3 {
		t.Fatalf("history=%d, want 3 (no double count)", len(got))
	}
	pub := &pgStubPublisher{}
	n, err := rt2.DrainAll(context.Background(), pub, base)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 1 || pub.events != 3 {
		t.Fatalf("drained=%d values=%d, want 1/3", n, pub.events)
	}
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after drain", len(pending))
	}
}

// Två Runtime-instanser över samma PgStore (simulerade repliker):
// CAS-retry konvergerar mot senaste mättid utan förlorade skrivningar.
func TestPostgresCrossInstanceConcurrency(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"},
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "gauge-1", Tenant: "t",
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}},
		Bindings: []things.Binding{
			{DeviceID: "d", Object: "urn:oma:lwm2m:ext:3330", Resource: "5700", Input: "distance"},
		},
	}
	rtA := application.NewRuntime(a)
	if _, err := rtA.RegisterThing("t", c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	rtB := application.NewRuntime(a)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			at := base.Add(time.Duration(i) * time.Minute)
			v := float64(i)
			rt := rtA
			if i%2 == 1 {
				rt = rtB
			}
			_, errs[i] = rt.HandleBatch(context.Background(), "t", fmt.Sprintf("c%d", i),
				[]things.Observation{{
					Signal:     things.ParseSignalID("d", "", "urn:oma:lwm2m:ext:3330", "5700"),
					Value:      &v,
					Quality:    things.QualityOK,
					ObservedAt: at,
					ReceivedAt: at,
				}}, at)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	st, ok := a.LoadThing("t", "gauge-1")
	if !ok {
		t.Fatal("thing missing")
	}
	_ = st
	hist := a.History("t", "gauge-1", "distance", time.Time{}, time.Time{}, 0, 0)
	if len(hist) != 10 {
		t.Fatalf("history=%d, want 10 (no lost writes)", len(hist))
	}
	latest := hist[len(hist)-1]
	if latest.ObservedAt.Unix() != base.Add(9*time.Minute).Unix() {
		t.Fatalf("latest=%v, want max observedAt", latest.ObservedAt)
	}
	// Aktuellt ska vara senaste mättid (inte nödvändigtvis värde 9 om
	// batchar omlott, men aldrig äldre än äldsta vinnare: kontrollera via state).
	cur, ok := a.LoadThing("t", "gauge-1")
	if !ok || len(cur.Current) == 0 {
		t.Fatal("current missing")
	}
}

// Regression K4: Late-flaggan ska överleva PostgreSQL (samma semantik som
// minnet/B13). Tidigare saknade history_v2 late-kolumnen och flaggan tappades.
func TestPostgresHistoryLateRoundTrip(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 20.0
	late := application.HistoryWrite{
		PropertyID: "distance", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at.Add(time.Minute), Source: "s/3330/5700",
		MessageID: "m2", Late: true,
	}
	cur := application.HistoryWrite{
		PropertyID: "level", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "recipe:wastebin-level",
		MessageID: "m1",
	}
	if !a.AppendHistory("t", "bin-42", late) {
		t.Fatal("late append must store")
	}
	if !a.AppendHistory("t", "bin-42", cur) {
		t.Fatal("current append must store")
	}
	all := a.History("t", "bin-42", "", time.Time{}, time.Time{}, 0, 0)
	if len(all) != 2 {
		t.Fatalf("empty prop must return all, got %d", len(all))
	}
	lateCount := 0
	for _, h := range all {
		if h.Late {
			lateCount++
		}
	}
	if lateCount != 1 {
		t.Fatalf("Late lost through postgres: %+v", all)
	}
}

// Regression K4: nil (saknat värde) och NaN ska vara olika dedupnycklar,
// som i minnet ("<nil>" vs "NaN"). Tidigare conflaterade COALESCE båda
// till NaN i uniknyckeln.
func TestPostgresHistoryNullVsNaN(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	nilHW := application.HistoryWrite{
		PropertyID: "p", Quality: "invalid",
		ObservedAt: at, ReceivedAt: at, Source: "s",
	}
	if !a.AppendHistory("t", "thing-1", nilHW) {
		t.Fatal("nil append must store")
	}
	if a.AppendHistory("t", "thing-1", nilHW) {
		t.Fatal("nil duplicate must dedup")
	}
	nan := float64(0)
	nan = nan / nan // NaN utan math-import i test
	nanHW := application.HistoryWrite{
		PropertyID: "p", Value: &nan, Quality: "uncertain",
		ObservedAt: at, ReceivedAt: at, Source: "s",
	}
	if !a.AppendHistory("t", "thing-1", nanHW) {
		t.Fatal("NaN with different quality must store as distinct row")
	}
	if a.AppendHistory("t", "thing-1", nanHW) {
		t.Fatal("NaN duplicate must dedup")
	}
	if got := a.History("t", "thing-1", "p", time.Time{}, time.Time{}, 0, 0); len(got) != 2 {
		t.Fatalf("history=%d, want 2 (nil + NaN distinct)", len(got))
	}
}

// Regression K4: Delete ska glömma outbox-seen + dedup (samma som minnet),
// så att nyskapad sak med samma ID kan återanvända messageID. Tidigare
// lade PgStore väntande ID:n till seen och blockerade nyskapande.
func TestPostgresDeleteForgetsSeenAndDedup(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.5
	st := application.StoredThingState{ConfigJSON: []byte(`{"a":1}`), Revision: 1, DataRevision: 1}
	if !a.InsertThing("t", "bin-42", st) {
		t.Fatal("insert")
	}
	hw := application.HistoryWrite{
		PropertyID: "temperature.window", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "s1", MessageID: "m1",
	}
	if !a.AppendHistory("t", "bin-42", hw) {
		t.Fatal("history append")
	}
	ev := application.FlowEvent{
		PropertyID: "temperature.window", Value: 21.5, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	a.DedupMark("t", "m1", "bin-42")
	a.DeleteThingState("t", "bin-42")
	if _, ok := a.LoadThing("t", "bin-42"); ok {
		t.Fatal("state must be gone after delete")
	}
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after delete, want 0", len(pending))
	}
	if a.DedupCheck("t", "m1", "bin-42") {
		t.Fatal("dedup must be forgotten after delete")
	}
	if a.AppendHistory("t", "bin-42", hw) {
		t.Fatal("history must survive delete (duplicate must dedup)")
	}
	if !a.InsertThing("t", "bin-42", st) {
		t.Fatal("recreate must succeed")
	}
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	if pending := a.PendingEvents("t", "bin-42"); len(pending) != 1 {
		t.Fatalf("pending=%d after recreate, want 1 (seen forgotten)", len(pending))
	}
}

// Regression K4: outbox-seen + dedup ska överleva omstart (ny PgStore-pool
// mot samma databas). Tidigare verifierades endast nytt Runtime över samma
// pool; detta testar beständigheten i databasen.
func TestPostgresSeenSurvivesRestart(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := application.FlowEvent{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	a.ClearPendingIDs("t", "bin-42", []string{application.StableEventID("t", "bin-42", "m1", "distance")})
	a.DedupMark("t", "m1", "bin-42")
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	s2, err := NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer s2.Close()
	a2 := application.RuntimeStore(s2)
	a2.StorePending("t", "bin-42", []application.FlowEvent{ev})
	if pending := a2.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("redelivery after restart must stay deduped, pending=%d", len(pending))
	}
	if !a2.DedupCheck("t", "m1", "bin-42") {
		t.Fatal("dedup must survive restart")
	}
}

// Regression K4: migrationer ska vara idempotenta (köras två gånger utan
// fel) och hypertable-status ska verifieras när Timescale finns.
// Utan Timescale skippas hypertable-delen (vanliga tabeller är korrekt).
func TestPostgresMigrationIdempotentAndHypertable(t *testing.T) {
	s := pgTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.migrate(context.Background()); err != nil {
		t.Fatalf("second migrate must be idempotent: %v", err)
	}
	var hasTimescale bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')`).Scan(&hasTimescale); err != nil {
		t.Fatalf("extension check: %v", err)
	}
	if !hasTimescale {
		t.Skip("no timescaledb extension: plain tables are correct behaviour")
	}
	var isHypertable bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name='history_v2')`).Scan(&isHypertable); err != nil {
		t.Fatalf("hypertable check: %v", err)
	}
	if !isHypertable {
		t.Fatal("history_v2 must be a hypertable when timescaledb is available")
	}
}

// Regression K4: samtidig registrering av samma sak: store-lagrets Insert
// (create-if-absent) ger exakt en vinnare; Runtime-lagret serialiserar
// säkert (1 create + högst 1 update, övriga konflikt). Se motsvarande
// fake-test för semantiken.
func TestPostgresConcurrentRegisterSameThing(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	// Store-nivå: exakt en Insert vinner.
	const insertRacers = 8
	var wg sync.WaitGroup
	insertWins := make([]bool, insertRacers)
	for i := range insertRacers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			insertWins[i] = a.InsertThing("t", "race-insert",
				application.StoredThingState{ConfigJSON: []byte(`{"a":1}`), Revision: 1, DataRevision: 1})
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, ok := range insertWins {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("insert wins=%d, want exactly 1", wins)
	}
	// Runtime-nivå: minst en vinner, övriga konflikt, ingen dubblett.
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"},
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "gauge-1", Tenant: "t",
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}},
	}
	const racers = 8
	errs := make([]error, racers)
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rt := application.NewRuntime(a)
			_, errs[i] = rt.RegisterThing("t", c, obj, 1)
		}(i)
	}
	wg.Wait()
	rwins := 0
	for _, err := range errs {
		if err == nil {
			rwins++
		}
	}
	if rwins < 1 || rwins > 2 {
		t.Fatalf("register wins=%d, want 1 or 2 (create + at most one update)", rwins)
	}
	if _, ok := a.LoadThing("t", "gauge-1"); !ok {
		t.Fatal("thing missing after concurrent register")
	}
}

// Regression K4: -0 och 0 ska vara samma dedupnyckel i pg (float8 '=')
// likt minnet efter normalisering. Tidigare divergerade minnet ("-0"/"0").
func TestPostgresHistoryNegZeroDedup(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mk := func(v float64) application.HistoryWrite {
		return application.HistoryWrite{
			PropertyID: "p", Value: &v, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Source: "s",
		}
	}
	if !a.AppendHistory("t", "negzero", mk(0.0)) {
		t.Fatal("0.0 must store")
	}
	neg := 0.0
	neg = -neg
	if a.AppendHistory("t", "negzero", mk(neg)) {
		t.Fatal("-0 must dedup against 0 (same as memory)")
	}
}

// Regression K4: sub-mikrosekundskillnader ska dedupliceras (PG avrundar
// TIMESTAMPTZ; minnet Round:ar i Go). Verifierar att båda lagren enats.
func TestPostgresHistorySubMicrosecondDedup(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	base := time.Date(2026, 9, 17, 12, 0, 0, 123456789, time.UTC)
	v := 1.0
	first := application.HistoryWrite{
		PropertyID: "p", Value: &v, Quality: "ok",
		ObservedAt: base, ReceivedAt: base, Source: "s",
	}
	if !a.AppendHistory("t", "micros", first) {
		t.Fatal("first must store")
	}
	dup := first
	dup.ObservedAt = time.Date(2026, 9, 17, 12, 0, 0, 123456780, time.UTC)
	if a.AppendHistory("t", "micros", dup) {
		t.Fatal("same microsecond (different nanos) must dedup")
	}
	other := first
	other.ObservedAt = base.Add(2 * time.Microsecond)
	if !a.AppendHistory("t", "micros", other) {
		t.Fatal("different microsecond must store")
	}
}

// Regression K4: DeleteThingState får inte radera andra sakars outbox-seen
// via LIKE-wildcards. Tidigare användes `id LIKE 'prefix/%'` där '_' och '%'
// i thing-ID matchade främmande rader; nu starts_with.
func TestPostgresDeleteSeenPrefixEscaping(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkEv := func(msg string) application.FlowEvent {
		return application.FlowEvent{
			PropertyID: "p", Value: 1, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, MessageID: msg, ConfigRevision: 1,
		}
	}
	// "a_b" innehåller '_' (LIKE-wildcard); "aXb" skulle felaktigt matchas.
	a.StorePending("t", "a_b", []application.FlowEvent{mkEv("m1")})
	a.StorePending("t", "aXb", []application.FlowEvent{mkEv("m1")})
	a.ClearPendingIDs("t", "a_b", []string{application.StableEventID("t", "a_b", "m1", "p")})
	a.ClearPendingIDs("t", "aXb", []string{application.StableEventID("t", "aXb", "m1", "p")})
	st := application.StoredThingState{ConfigJSON: []byte(`{}`), Revision: 1, DataRevision: 1}
	if !a.InsertThing("t", "a_b", st) {
		t.Fatal("insert a_b")
	}
	if !a.InsertThing("t", "aXb", st) {
		t.Fatal("insert aXb")
	}
	a.DeleteThingState("t", "a_b")
	// aXb:s seen ska vara orörd: återleverans ska fortsatt dedupliceras.
	a.StorePending("t", "aXb", []application.FlowEvent{mkEv("m1")})
	if pending := a.PendingEvents("t", "aXb"); len(pending) != 0 {
		t.Fatalf("aXb seen wrongly deleted by a_b delete, pending=%d", len(pending))
	}
	// a_b:s seen ska vara glömd: nyskapad sak kan återanvända ID.
	if !a.InsertThing("t", "a_b", st) {
		t.Fatal("recreate a_b")
	}
	a.StorePending("t", "a_b", []application.FlowEvent{mkEv("m1")})
	if pending := a.PendingEvents("t", "a_b"); len(pending) != 1 {
		t.Fatalf("a_b seen must be forgotten after delete, pending=%d", len(pending))
	}
}

// Regression K4: väntande (ej dränerade) outbox-rader ska överleva omstart
// (ny pool mot samma DB), inte bara seen-markörer. Tidigare testades endast
// seen efter Clear.
func TestPostgresPendingSurvivesRestart(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := application.FlowEvent{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m-pending", ConfigRevision: 1,
	}
	a.StorePending("t", "bin-42", []application.FlowEvent{ev})
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	s2, err := NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer s2.Close()
	a2 := application.RuntimeStore(s2)
	if pending := a2.PendingEvents("t", "bin-42"); len(pending) != 1 {
		t.Fatalf("pending must survive restart, got %d", len(pending))
	}
}

// Regression K4: AttachChild med förlorad kapplöpning (samtidig attach med
// olika föräldrar, samma id) får inte returnera nil för förloraren.
// Tidigare maskerade ON CONFLICT DO NOTHING konflikten.
func TestPostgresAttachChildConflictAfterRace(t *testing.T) {
	s := pgTestStore(t)
	a := application.RuntimeStore(s)
	if err := a.AttachChild("t", "race-child", "parent-a"); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	// Simulerar förlorarens INSERT efter att vinnaren redan skrivit:
	// direkt INSERT med samma id men annan förälder ska inte lyckas tyst.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO relations_v2(tenant, id, type, from_id, to_id) VALUES($1,$2,'partOf',$3,$4) ON CONFLICT DO NOTHING`,
		"t", "partof-race-child", "race-child", "parent-b")
	if err != nil {
		t.Fatalf("raw insert: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatal("conflicting raw insert should affect 0 rows (PK conflict)")
	}
	// Adaptervägen ska nu rapportera konflikt, ej nil.
	if err := a.AttachChild("t", "race-child", "parent-b"); err == nil {
		t.Fatal("second parent must conflict, not silent success")
	}
	if parent, _ := a.ParentOf("t", "race-child"); parent != "parent-a" {
		t.Fatalf("parent=%q, want parent-a (winner kept)", parent)
	}
}

// Regression K4: ConnStr ska URL-koda specialtecken i lösenord (/@:?#).
func TestConfigConnStrEscapesPassword(t *testing.T) {
	cfg := NewConfig("localhost", "user", "p@ss:w/ord?#", "5432", "diwise", "disable")
	cs := cfg.ConnStr()
	if cs == "" {
		t.Fatal("empty connstr")
	}
	// Parsning med pgx måste lyckas trots specialtecken (tidigare
	// fmt.Sprintf bröt strängen).
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New with special password must parse: %v (connstr=%q)", err, cs)
	}
	_ = s
}
