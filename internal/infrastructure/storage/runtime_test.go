package storage

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
)

func newAdapter(t *testing.T) *RuntimeAdapter {
	t.Helper()
	s, err := New(context.Background(), NewConfig("localhost", "u", "p", "5432", "diwise", "disable"))
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	return NewRuntimeAdapter(s)
}

func TestAdapterThingRoundTrip(t *testing.T) {
	a := newAdapter(t)
	v := 21.5
	st := application.StoredThingState{
		ConfigJSON: []byte(`{"templateId":"room","templateVersion":"v1"}`),
		Revision:   3, DataRevision: 4,
		Current: map[string]application.CurrentValue{
			"temperature.window": {
				Value: &v, Quality: "ok",
				ObservedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
				Source:     "s1/3303/5700",
			},
		},
		LastMessageID: "m1",
		LastEvents: []application.FlowEvent{{
			PropertyID: "temperature.window", Value: 21.5, Quality: "ok",
			ObservedAt: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
			MessageID:  "m1",
		}},
	}
	if !a.InsertThing(context.Background(), "t", "room-a", st) {
		t.Fatal("insert must store new thing")
	}
	got, ok := a.LoadThing(context.Background(), "t", "room-a")
	if !ok {
		t.Fatal("stored thing missing")
	}
	if got.Revision != 3 || got.DataRevision != 4 || string(got.ConfigJSON) != string(st.ConfigJSON) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Current["temperature.window"].Value == nil || *got.Current["temperature.window"].Value != 21.5 {
		t.Fatalf("current=%+v", got.Current)
	}
	if got.LastMessageID != "m1" || len(got.LastEvents) != 1 {
		t.Fatalf("markers=%+v", got)
	}
	if _, ok := a.LoadThing(context.Background(), "other", "room-a"); ok {
		t.Fatal("cross-tenant leak")
	}
	a.DeleteThingState(context.Background(), "t", "room-a")
	if _, ok := a.LoadThing(context.Background(), "t", "room-a"); ok {
		t.Fatal("delete must remove state")
	}
}

func TestAdapterHistoryPendingDedup(t *testing.T) {
	a := newAdapter(t)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.5
	hw := application.HistoryWrite{
		PropertyID: "temperature.window", Value: &v, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, Source: "s1/3303/5700", MessageID: "m1",
	}
	if !a.AppendHistory(context.Background(), "t", "room-a", hw) {
		t.Fatal("first append must store")
	}
	if a.AppendHistory(context.Background(), "t", "room-a", hw) {
		t.Fatal("identical append must dedup")
	}
	ev := application.FlowEvent{
		PropertyID: "temperature.window", Value: 21.5, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	a.StorePending(context.Background(), "t", "room-a", []application.FlowEvent{ev})
	a.StorePending(context.Background(), "t", "room-a", []application.FlowEvent{ev})
	if pending := a.PendingEvents(context.Background(), "t", "room-a"); len(pending) != 1 {
		t.Fatalf("pending=%d, want deduped 1", len(pending))
	}
	a.ClearPendingIDs(context.Background(), "t", "room-a", []string{application.StableEventID("t", "room-a", "m1", "temperature.window")})
	if pending := a.PendingEvents(context.Background(), "t", "room-a"); len(pending) != 0 {
		t.Fatalf("pending=%d after clear, want 0", len(pending))
	}
	if a.DedupHit("t", "m1", "room-a") {
		t.Fatal("first sight must not hit")
	}
	if !a.DedupHit("t", "m1", "room-a") {
		t.Fatal("second sight must hit")
	}
	if a.DedupHit("other", "m1", "room-a") {
		t.Fatal("dedup must be tenant-scoped")
	}
}

// Regression TV-04: tenants "a/b" och "a~b" (samt sak-ID:n med "/" vs "~")
// får aldrig deduplicera eller rensa varandras outbox-rader.
func TestAdapterOutboxTenantIsolation(t *testing.T) {
	a := newAdapter(t)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := application.FlowEvent{
		PropertyID: "p", Value: 1.0, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 1,
	}
	// Samma meddelande för kolliderande tenants lagras i båda.
	a.StorePending(context.Background(), "a/b", "c", []application.FlowEvent{ev})
	a.StorePending(context.Background(), "a~b", "c", []application.FlowEvent{ev})
	if n := len(a.PendingEvents(context.Background(), "a/b", "c")); n != 1 {
		t.Fatalf("pending a/b=%d, want 1", n)
	}
	if n := len(a.PendingEvents(context.Background(), "a~b", "c")); n != 1 {
		t.Fatalf("pending a~b=%d, want 1 (no cross-tenant false dedup)", n)
	}
	// Glömska för ena tenanten lämnar den andra intakt (pending + seen).
	a.DeleteThingState(context.Background(), "a/b", "c")
	if n := len(a.PendingEvents(context.Background(), "a~b", "c")); n != 1 {
		t.Fatalf("pending a~b after forget=%d, want 1", n)
	}
	a.StorePending(context.Background(), "a~b", "c", []application.FlowEvent{ev})
	if n := len(a.PendingEvents(context.Background(), "a~b", "c")); n != 1 {
		t.Fatalf("redelivery for survivor must stay deduped, pending=%d", n)
	}
	// Glömd tenant tar emot nya skrivningar igen.
	a.StorePending(context.Background(), "a/b", "c", []application.FlowEvent{ev})
	if n := len(a.PendingEvents(context.Background(), "a/b", "c")); n != 1 {
		t.Fatalf("pending a/b after forget=%d, want 1", n)
	}
	// Sak-ID:n med "/" vs "~" isoleras likadant.
	a.StorePending(context.Background(), "t", "x/y", []application.FlowEvent{ev})
	a.StorePending(context.Background(), "t", "x~y", []application.FlowEvent{ev})
	if n := len(a.PendingEvents(context.Background(), "t", "x/y")); n != 1 {
		t.Fatalf("pending x/y=%d, want 1", n)
	}
	if n := len(a.PendingEvents(context.Background(), "t", "x~y")); n != 1 {
		t.Fatalf("pending x~y=%d, want 1 (no cross-thing false dedup)", n)
	}
	a.DeleteThingState(context.Background(), "t", "x/y")
	if n := len(a.PendingEvents(context.Background(), "t", "x~y")); n != 1 {
		t.Fatalf("pending x~y after forget=%d, want 1", n)
	}
}

func TestAdapterRelations(t *testing.T) {
	a := newAdapter(t)
	if err := a.AttachChild(context.Background(), "t", "room-a", "building-a"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := a.AttachChild(context.Background(), "t", "room-a", "building-a"); err != nil {
		t.Fatalf("re-attach same edge must be idempotent: %v", err)
	}
	if children := a.Children(context.Background(), "t", "building-a"); len(children) != 1 || children[0] != "room-a" {
		t.Fatalf("children=%v", children)
	}
	if parent, ok := a.ParentOf(context.Background(), "t", "room-a"); !ok || parent != "building-a" {
		t.Fatalf("parent=%q", parent)
	}
	if err := a.MoveChild(context.Background(), "t", "room-a", "building-b"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if parent, _ := a.ParentOf(context.Background(), "t", "room-a"); parent != "building-b" {
		t.Fatalf("parent=%q after move", parent)
	}
	a.DetachChild(context.Background(), "t", "room-a")
	if _, ok := a.ParentOf(context.Background(), "t", "room-a"); ok {
		t.Fatal("detach must remove parent link")
	}
	if children := a.Children(context.Background(), "t", "building-a"); len(children) != 0 {
		t.Fatalf("children=%v after detach", children)
	}
}

func TestAdapterTenants(t *testing.T) {
	a := newAdapter(t)
	if !a.InsertThing(context.Background(), "b", "x", application.StoredThingState{Revision: 1}) {
		t.Fatal("insert b/x")
	}
	if !a.InsertThing(context.Background(), "a", "y", application.StoredThingState{Revision: 1}) {
		t.Fatal("insert a/y")
	}
	tenants := a.ListTenants(context.Background())
	if len(tenants) != 2 || tenants[0] != "a" || tenants[1] != "b" {
		t.Fatalf("tenants=%v", tenants)
	}
	if ids := a.ListThingIDs(context.Background(), "a"); len(ids) != 1 || ids[0] != "y" {
		t.Fatalf("ids=%v", ids)
	}
}

// Regression: EffectiveConfig/CurrentValue/FlowEvent round-trip via JSON-blob.
// Tenant får ej läcka; Delete bevarar historik men rensar outbox/dedup.
func TestAdapterFullRoundTripAndDeleteForgets(t *testing.T) {
	a := newAdapter(t)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.5
	st := application.StoredThingState{
		ConfigJSON: []byte(`{"TemplateID":"room","TemplateVersion":"v1","Properties":{"temperature.window":{"ID":"temperature.window","Unit":"Cel"}},"Bindings":[{"DeviceID":"s1","Object":"urn:oma:lwm2m:ext:3303","Resource":"5700","Input":"temperature.window"}],"ParamValues":{"usableHeight":1.2},"Recipes":[{"Name":"r","Operator":"mean","Version":"v1","Inputs":["temperature.window"],"Outputs":["avg"]}],"Relations":[{"Name":"partOf","TargetThingID":"b"}]}`),
		Revision:   7,
		Current: map[string]application.CurrentValue{
			"temperature.window": {Value: &v, Quality: "ok", ObservedAt: at, ReceivedAt: at, Source: "s1/3303/5700", MessageID: "m1"},
		},
		LastMessageID: "m1",
		LastEvents: []application.FlowEvent{{
			PropertyID: "temperature.window", Value: 21.5, Quality: "ok",
			ObservedAt: at, ReceivedAt: at, Sources: []string{"s1/3303/5700"},
			ConfigRevision: 7, MessageID: "m1",
		}},
	}
	if !a.InsertThing(context.Background(), "t", "room-a", st) {
		t.Fatal("insert must store new thing")
	}
	got, ok := a.LoadThing(context.Background(), "t", "room-a")
	if !ok {
		t.Fatal("missing")
	}
	if got.Revision != 7 {
		t.Fatalf("rev=%d, want 7", got.Revision)
	}
	if got.Current["temperature.window"].Source != "s1/3303/5700" || *got.Current["temperature.window"].Value != 21.5 {
		t.Fatalf("current lost: %+v", got.Current)
	}
	if len(got.LastEvents) != 1 || got.LastEvents[0].Sources[0] != "s1/3303/5700" {
		t.Fatalf("events lost: %+v", got.LastEvents)
	}
	// Historik + pending + dedup.
	hw := application.HistoryWrite{PropertyID: "temperature.window", Value: &v, Quality: "ok", ObservedAt: at, ReceivedAt: at, Source: "s1/3303/5700", MessageID: "m1"}
	if !a.AppendHistory(context.Background(), "t", "room-a", hw) {
		t.Fatal("history append")
	}
	ev := application.FlowEvent{PropertyID: "temperature.window", Value: 21.5, Quality: "ok", ObservedAt: at, ReceivedAt: at, MessageID: "m1", ConfigRevision: 7}
	a.StorePending(context.Background(), "t", "room-a", []application.FlowEvent{ev})
	if pending := a.PendingEvents(context.Background(), "t", "room-a"); len(pending) != 1 || pending[0].PropertyID != "temperature.window" {
		t.Fatalf("pending=%+v", pending)
	}
	if a.DedupCheck(context.Background(), "t", "m1", "room-a") {
		t.Fatal("DedupCheck före mark ska vara false")
	}
	a.DedupMark(context.Background(), "t", "m1", "room-a")
	if !a.DedupCheck(context.Background(), "t", "m1", "room-a") {
		t.Fatal("DedupCheck efter mark ska vara true")
	}
	// Delete: state borta, historik kvar (verifieras via List? här via re-append dedup),
	// outbox+dedup glömda.
	a.DeleteThingState(context.Background(), "t", "room-a")
	if _, ok := a.LoadThing(context.Background(), "t", "room-a"); ok {
		t.Fatal("state ska vara borta")
	}
	if pending := a.PendingEvents(context.Background(), "t", "room-a"); len(pending) != 0 {
		t.Fatalf("pending=%d efter delete, want 0", len(pending))
	}
	if a.DedupCheck(context.Background(), "t", "m1", "room-a") {
		t.Fatal("dedup ska vara glömd efter delete")
	}
	if a.AppendHistory(context.Background(), "t", "room-a", hw) {
		t.Fatal("historik ska vara bevarad (dubblett ska dedupliceras även efter delete)")
	}
}

// Regression Etapp 6: adaptern ska bevara Late och returnera alla
// egenskaper vid tom prop (samma semantik som Manager förväntar sig).
func TestAdapterHistoryLateAndEmptyProp(t *testing.T) {
	a := newAdapter(t)
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
	if !a.AppendHistory(context.Background(), "t", "bin-42", late) {
		t.Fatal("late append must store")
	}
	if !a.AppendHistory(context.Background(), "t", "bin-42", cur) {
		t.Fatal("current append must store")
	}
	all := a.History(context.Background(), "t", "bin-42", "", time.Time{}, time.Time{}, 0, 0)
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
		t.Fatalf("Late lost through adapter: %+v", all)
	}
}
