package storage

import (
	"testing"
	"time"
)

func fptr(v float64) *float64 { return &v }

func TestThingUpsertGetList(t *testing.T) {
	s := NewMemThingStore()
	s.Upsert(ThingRecord{ID: "t1", Tenant: "acme", Revision: 3, Data: []byte(`{"n":1}`)})
	data, rev, ok := s.Get("acme", "t1")
	if !ok {
		t.Fatal("väntat träff")
	}
	if rev != 3 {
		t.Fatalf("Upsert ska behålla inkommande revision, fick %d", rev)
	}
	if string(data) != `{"n":1}` {
		t.Fatalf("fel data: %s", data)
	}
	// Upsert behåller inkommande revision även vid konflikt.
	s.Upsert(ThingRecord{ID: "t1", Tenant: "acme", Revision: 4, Data: []byte(`{"n":2}`)})
	if _, rev, _ := s.Get("acme", "t1"); rev != 4 {
		t.Fatalf("väntat revision 4, fick %d", rev)
	}
	s.Upsert(ThingRecord{ID: "t2", Tenant: "acme", Revision: 1})
	s.Upsert(ThingRecord{ID: "t9", Tenant: "other", Revision: 1})
	list := s.ListByTenant("acme")
	if len(list) != 2 || list[0].ID != "t1" || list[1].ID != "t2" {
		t.Fatalf("fel ListByTenant: %+v", list)
	}
	if _, _, ok := s.Get("acme", "missing"); ok {
		t.Fatal("väntat miss för okänt ID")
	}
}

func TestHistoryAppendIdempotentAndList(t *testing.T) {
	s := NewMemHistoryStore()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rec := HistoryRecord{
		Tenant: "acme", ThingID: "t1", PropertyID: "temp",
		ObservedAt: base, ReceivedAt: base, Value: fptr(21.5),
		Quality: "ok", Source: "s1", MessageID: "m1",
	}
	if !s.AppendIdempotent(rec) {
		t.Fatal("första append ska lyckas")
	}
	// Dubblett enligt nyckel tenant|thing|prop|observed|source|quality|value
	// ska hoppas över, även med annat MessageID/ReceivedAt.
	dup := rec
	dup.MessageID = "m2"
	dup.ReceivedAt = base.Add(time.Second)
	if s.AppendIdempotent(dup) {
		t.Fatal("dubblett ska hoppas över (idempotent)")
	}
	// Annat värde => ny rad.
	other := rec
	other.Value = fptr(22.5)
	if !s.AppendIdempotent(other) {
		t.Fatal("annat värde ska ge ny rad")
	}
	// Annan source => ny rad.
	otherSrc := rec
	otherSrc.Source = "s2"
	if !s.AppendIdempotent(otherSrc) {
		t.Fatal("annan source ska ge ny rad")
	}
	// Annan kvalitet med samma värde/tid/källa => ny rad (får ej tappas).
	otherQ := rec
	otherQ.Quality = "uncertain"
	if !s.AppendIdempotent(otherQ) {
		t.Fatal("annan kvalitet ska ge ny rad")
	}
	got := s.List("acme", "t1", "temp", time.Time{}, time.Time{}, 0)
	if len(got) != 4 {
		t.Fatalf("väntat 4 rader, fick %d", len(got))
	}
	// Tidsfilter + limit.
	later := rec
	later.ObservedAt = base.Add(time.Hour)
	later.Value = fptr(23)
	later.Source = "s1"
	// Säkerställ unik nyckel (nytt värde + ny tid).
	if !s.AppendIdempotent(later) {
		t.Fatal("senare rad ska läggas till")
	}
	filtered := s.List("acme", "t1", "temp", base.Add(30*time.Minute), time.Time{}, 0)
	if len(filtered) != 1 {
		t.Fatalf("väntat 1 filtrerad rad, fick %d", len(filtered))
	}
	limited := s.List("acme", "t1", "temp", time.Time{}, time.Time{}, 2)
	if len(limited) != 2 {
		t.Fatalf("väntat limit 2, fick %d", len(limited))
	}
}

func TestRelationSingleParentAndSelfRef(t *testing.T) {
	s := NewMemRelationStore()
	if err := s.Add(RelationRecord{ID: "r1", Tenant: "acme", Type: "partOf", FromID: "child", ToID: "parent1"}); err != nil {
		t.Fatalf("första partOf ska gå: %v", err)
	}
	// Andra partOf-förälder för samma barn nekas.
	if err := s.Add(RelationRecord{ID: "r2", Tenant: "acme", Type: "partOf", FromID: "child", ToID: "parent2"}); err == nil {
		t.Fatal("andra partOf-förälder ska nekas")
	}
	// Självreferens nekas.
	if err := s.Add(RelationRecord{ID: "r3", Tenant: "acme", Type: "partOf", FromID: "x", ToID: "x"}); err == nil {
		t.Fatal("självreferens ska nekas")
	}
	// Parent/Children.
	p, ok := s.Parent("acme", "child")
	if !ok || p.ToID != "parent1" {
		t.Fatalf("fel Parent: %+v %v", p, ok)
	}
	kids := s.Children("acme", "parent1")
	if len(kids) != 1 || kids[0].FromID != "child" {
		t.Fatalf("fel Children: %+v", kids)
	}
	// Move till ny förälder.
	if err := s.Move("acme", "child", "parent2"); err != nil {
		t.Fatalf("Move ska gå: %v", err)
	}
	p, _ = s.Parent("acme", "child")
	if p.ToID != "parent2" {
		t.Fatalf("väntat parent2 efter Move, fick %q", p.ToID)
	}
	// Move till sig själv nekas.
	if err := s.Move("acme", "child", "child"); err == nil {
		t.Fatal("Move till sig själv ska nekas")
	}
}

func TestOutboxAppendClear(t *testing.T) {
	o := NewMemOutbox()
	if o.Len() != 0 {
		t.Fatal("tom utbox ska ha len 0")
	}
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !o.Append(OutboxEvent{ID: "e1", Tenant: "acme", ThingID: "t1", PropertyID: "temp", ObservedAt: ts, Revision: 1}) {
		t.Fatal("första e1 ska lagras")
	}
	if !o.Append(OutboxEvent{ID: "e2", Tenant: "acme", ThingID: "t1", PropertyID: "hum", ObservedAt: ts, Revision: 2}) {
		t.Fatal("första e2 ska lagras")
	}
	if o.Len() != 2 {
		t.Fatalf("väntat len 2, fick %d", o.Len())
	}
	// Retry med samma ID ska dedupliceras (idempotent outbox).
	if o.Append(OutboxEvent{ID: "e1", Tenant: "acme", ThingID: "t1", PropertyID: "temp", ObservedAt: ts, Revision: 1}) {
		t.Fatal("dubblett-ID e1 ska hoppas över")
	}
	if o.Len() != 2 {
		t.Fatalf("väntat len 2 efter dubblett, fick %d", o.Len())
	}
	events := o.ListAndClear()
	if len(events) != 2 || events[0].ID != "e1" || events[1].ID != "e2" {
		t.Fatalf("fel events: %+v", events)
	}
	if o.Len() != 0 {
		t.Fatal("utbox ska vara tömd efter ListAndClear")
	}
	if again := o.ListAndClear(); len(again) != 0 {
		t.Fatalf("andra ListAndClear ska vara tom, fick %+v", again)
	}
	// Sedda ID:n överlever Clear: retry efter publicering publiceras ej igen.
	if o.Append(OutboxEvent{ID: "e1", Tenant: "acme", ThingID: "t1", PropertyID: "temp", ObservedAt: ts, Revision: 1}) {
		t.Fatal("e1 efter Clear ska fortsatt dedupliceras")
	}
}

func TestDedupSeenOrMark(t *testing.T) {
	d := NewMemDedup()
	if d.SeenOrMark("acme", "m1", "t1") {
		t.Fatal("första gången ska vara false (inte sett)")
	}
	if !d.SeenOrMark("acme", "m1", "t1") {
		t.Fatal("andra gången ska vara true (redan sett)")
	}
	// Samma messageID men annat thing => separat nyckel.
	if d.SeenOrMark("acme", "m1", "t2") {
		t.Fatal("annat thing ska ge false")
	}
	// Samma messageID+thing men annan tenant => separat nyckel (isolering).
	if d.SeenOrMark("other", "m1", "t1") {
		t.Fatal("annan tenant ska ge false (ingen cross-tenant-kollision)")
	}
	// Tomt messageID dedupliceras aldrig.
	if d.SeenOrMark("acme", "", "t1") {
		t.Fatal("tomt messageID ska alltid ge false")
	}
}

func TestRelationCycleDenied(t *testing.T) {
	s := NewMemRelationStore()
	if err := s.Add(RelationRecord{ID: "r1", Tenant: "acme", Type: "partOf", FromID: "b", ToID: "a"}); err != nil {
		t.Fatalf("b->a ska gå: %v", err)
	}
	if err := s.Add(RelationRecord{ID: "r2", Tenant: "acme", Type: "partOf", FromID: "c", ToID: "b"}); err != nil {
		t.Fatalf("c->b ska gå: %v", err)
	}
	// a->c skulle sluta cykeln a->c->b->a och ska nekas.
	if err := s.Add(RelationRecord{ID: "r3", Tenant: "acme", Type: "partOf", FromID: "a", ToID: "c"}); err == nil {
		t.Fatal("partOf-cykel a->c->b->a ska nekas")
	}
	// Move som skapar cykel ska också nekas: flytta a under c.
	if err := s.Add(RelationRecord{ID: "r4", Tenant: "acme", Type: "partOf", FromID: "a", ToID: "root"}); err != nil {
		t.Fatalf("a->root ska gå: %v", err)
	}
	if err := s.Move("acme", "a", "c"); err == nil {
		t.Fatal("Move a->c ska nekas (cykel via c->b->a)")
	}
	// Samma kedja i annan tenant ska inte påverkas (isolering).
	if err := s.Add(RelationRecord{ID: "r3", Tenant: "other", Type: "partOf", FromID: "a", ToID: "c"}); err != nil {
		t.Fatalf("samma kant i annan tenant ska gå: %v", err)
	}
}

// Regression tenant-isolering: Get/List ska aldrig läcka över tenant-gräns.
func TestTenantIsolationThingAndHistory(t *testing.T) {
	ts := NewMemThingStore()
	ts.Upsert(ThingRecord{ID: "secret", Tenant: "t1", Revision: 1, Data: []byte(`{"n":1}`)})
	if _, _, ok := ts.Get("t2", "secret"); ok {
		t.Fatal("Get i annan tenant ska missa (ingen läcka)")
	}
	if _, _, ok := ts.Get("", "secret"); ok {
		t.Fatal("Get med tom tenant ska missa")
	}
	if got := ts.ListByTenant("t2"); len(got) != 0 {
		t.Fatalf("ListByTenant(t2) ska vara tom, fick %+v", got)
	}
	if got := ts.ListByTenant(""); len(got) != 0 {
		t.Fatalf("ListByTenant(tom) ska vara tom, fick %+v", got)
	}

	hs := NewMemHistoryStore()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	hs.AppendIdempotent(HistoryRecord{
		Tenant: "t1", ThingID: "thing-1", PropertyID: "temp",
		ObservedAt: base, ReceivedAt: base, Value: fptr(1),
		Quality: "ok", Source: "s1", MessageID: "m1",
	})
	if got := hs.List("t2", "thing-1", "temp", time.Time{}, time.Time{}, 0); len(got) != 0 {
		t.Fatalf("historik i annan tenant ska vara tom, fick %+v", got)
	}
	if got := hs.List("t1", "thing-1", "other-prop", time.Time{}, time.Time{}, 0); len(got) != 0 {
		t.Fatalf("fel property ska ge tomt resultat, fick %+v", got)
	}
}

// Regression L6/L10: Has/Mark split för atomär dedup; ForgetThing rensar per sak.
func TestDedupHasMarkForget(t *testing.T) {
	d := NewMemDedup()
	if d.Has("t", "m1", "thing") {
		t.Fatal("Has före Mark ska vara false")
	}
	d.Mark("t", "m1", "thing")
	if !d.Has("t", "m1", "thing") {
		t.Fatal("Has efter Mark ska vara true")
	}
	if !d.SeenOrMark("t", "m1", "thing") {
		t.Fatal("SeenOrMark efter Mark ska vara true")
	}
	d.Mark("t", "", "thing") // no-op, ska ej panika
	if d.Has("t", "", "thing") {
		t.Fatal("tomt ID ska aldrig Has")
	}
	d.ForgetThing("t", "thing")
	if d.Has("t", "m1", "thing") {
		t.Fatal("Has efter ForgetThing ska vara false")
	}
	// Annan sak påverkas ej.
	d.Mark("t", "m1", "other")
	d.ForgetThing("t", "thing")
	if !d.Has("t", "m1", "other") {
		t.Fatal("ForgetThing får ej rensa annan sak")
	}
}

// Regression L7: Forget rensar väntande + sedda ID:n per sak utan att röra andra.
func TestOutboxForget(t *testing.T) {
	o := NewMemOutbox()
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o.Append(OutboxEvent{ID: "v1/t/a/m1/p", Tenant: "t", ThingID: "a", ObservedAt: ts})
	o.Append(OutboxEvent{ID: "v1/t/b/m1/p", Tenant: "t", ThingID: "b", ObservedAt: ts})
	o.ClearIDs("t", "a", []string{"v1/t/a/m1/p"}) // levererad: väntande borta, sedd kvar
	if o.Len() != 1 {
		t.Fatalf("len=%d, want 1", o.Len())
	}
	if o.Append(OutboxEvent{ID: "v1/t/a/m1/p", Tenant: "t", ThingID: "a", ObservedAt: ts}) {
		t.Fatal("levererad ID ska fortsatt dedupliceras före Forget")
	}
	o.Forget("t", "a")
	if o.Len() != 1 {
		t.Fatalf("len=%d efter Forget(a), want 1 (b kvar)", o.Len())
	}
	if !o.Append(OutboxEvent{ID: "v1/t/a/m1/p", Tenant: "t", ThingID: "a", ObservedAt: ts}) {
		t.Fatal("Forget ska rensa sedda ID:n så att nyskapad sak kan återanvända ID")
	}
	if got := o.PeekFor("t", "b"); len(got) != 1 {
		t.Fatalf("PeekFor(b)=%d, want 1 (orörd)", len(got))
	}
}
