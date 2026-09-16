package application

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// fakeStore implementerar RuntimeStore med samma semantik som adaptern:
// innehålls-dedupad historik, ID-dedupad pending-kö, tenantnycklad dedup
// och partOf-regler (en förälder, cykelskydd).
type fakeStore struct {
	mu       sync.Mutex
	things   map[string]StoredThingState
	hist     []storedHistory
	histSeen map[string]struct{}
	pending  map[string][]FlowEvent
	pendSeen map[string]struct{}
	dedup    map[string]struct{}
	parents  map[string]string          // tenant\x00barn -> förälder
	children map[string]map[string]bool // tenant\x00förälder -> barn
}

type storedHistory struct {
	tenant  string
	thingID string
	w       HistoryWrite
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		things:   make(map[string]StoredThingState),
		histSeen: make(map[string]struct{}),
		pending:  make(map[string][]FlowEvent),
		pendSeen: make(map[string]struct{}),
		dedup:    make(map[string]struct{}),
		parents:  make(map[string]string),
		children: make(map[string]map[string]bool),
	}
}

func fkey(tenant, id string) string { return tenant + "\x00" + id }

func (f *fakeStore) LoadThing(tenant, thingID string) (StoredThingState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.things[fkey(tenant, thingID)]
	if !ok {
		return StoredThingState{}, false
	}
	cp := st
	cp.Current = copyCurrent(st.Current)
	cp.LastEvents = append([]FlowEvent(nil), st.LastEvents...)
	return cp, true
}

func copyCurrent(m map[string]CurrentValue) map[string]CurrentValue {
	if m == nil {
		return nil
	}
	out := make(map[string]CurrentValue, len(m))
	for k, v := range m {
		if v.Value != nil {
			c := *v.Value
			v.Value = &c
		}
		out[k] = v
	}
	return out
}

func (f *fakeStore) SaveThing(tenant, thingID string, st StoredThingState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st.Current = copyCurrent(st.Current)
	st.LastEvents = append([]FlowEvent(nil), st.LastEvents...)
	f.things[fkey(tenant, thingID)] = st
}

func (f *fakeStore) DeleteThingState(tenant, thingID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.things, fkey(tenant, thingID))
	// Spegla adaptern: rensa föräldralös outbox + dedup, bevara historik.
	key := fkey(tenant, thingID)
	delete(f.pending, key)
	suffix := "\x00" + thingID
	prefix := tenant + "\x00"
	for k := range f.dedup {
		if len(k) >= len(prefix)+len(suffix) && k[:len(prefix)] == prefix && k[len(k)-len(suffix):] == suffix {
			delete(f.dedup, k)
		}
	}
	sanitize := func(s string) string {
		out := make([]byte, 0, len(s))
		for i := 0; i < len(s); i++ {
			if s[i] == '/' {
				out = append(out, '~')
			} else {
				out = append(out, s[i])
			}
		}
		return string(out)
	}
	pfx := "v1/" + sanitize(tenant) + "/" + sanitize(thingID) + "/"
	for id := range f.pendSeen {
		if len(id) >= len(pfx) && id[:len(pfx)] == pfx {
			delete(f.pendSeen, id)
		}
	}
}

func (f *fakeStore) ListThingIDs(tenant string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for key := range f.things {
		parts := splitKey(key)
		if parts[0] == tenant {
			out = append(out, parts[1])
		}
	}
	sort.Strings(out)
	return out
}

func splitKey(key string) [2]string {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return [2]string{key[:i], key[i+1:]}
		}
	}
	return [2]string{key, ""}
}

func (f *fakeStore) ListTenants() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	for key := range f.things {
		seen[splitKey(key)[0]] = true
	}
	var out []string
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (f *fakeStore) AppendHistory(tenant, thingID string, h HistoryWrite) bool {
	val := "<nil>"
	if h.Value != nil {
		val = strconv.FormatFloat(*h.Value, 'g', -1, 64)
	}
	key := tenant + "|" + thingID + "|" + h.PropertyID + "|" +
		strconv.FormatInt(h.ObservedAt.UnixNano(), 10) + "|" + h.Source + "|" + h.Quality + "|" + val
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.histSeen[key]; ok {
		return false
	}
	f.histSeen[key] = struct{}{}
	cp := h
	if h.Value != nil {
		c := *h.Value
		cp.Value = &c
	}
	f.hist = append(f.hist, storedHistory{tenant, thingID, cp})
	return true
}

func (f *fakeStore) historyCount(tenant, thingID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, h := range f.hist {
		if h.tenant == tenant && h.thingID == thingID {
			n++
		}
	}
	return n
}

func (f *fakeStore) PendingEvents(tenant, thingID string) []FlowEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FlowEvent(nil), f.pending[fkey(tenant, thingID)]...)
}

func (f *fakeStore) StorePending(tenant, thingID string, events []FlowEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fkey(tenant, thingID)
	for _, e := range events {
		id := StableEventID(tenant, thingID, e.MessageID, e.PropertyID)
		if _, ok := f.pendSeen[id]; ok {
			continue
		}
		f.pendSeen[id] = struct{}{}
		f.pending[key] = append(f.pending[key], e)
	}
}

func (f *fakeStore) ClearPendingIDs(tenant, thingID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fkey(tenant, thingID)
	kept := f.pending[key][:0]
	for _, e := range f.pending[key] {
		if want[StableEventID(tenant, thingID, e.MessageID, e.PropertyID)] {
			continue
		}
		kept = append(kept, e)
	}
	if len(kept) == 0 {
		delete(f.pending, key)
	} else {
		f.pending[key] = kept
	}
}

func (f *fakeStore) DedupHit(tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := tenant + "\x00" + messageID + "\x00" + thingID
	if _, ok := f.dedup[key]; ok {
		return true
	}
	f.dedup[key] = struct{}{}
	return false
}

func (f *fakeStore) DedupCheck(tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.dedup[tenant+"\x00"+messageID+"\x00"+thingID]
	return ok
}

func (f *fakeStore) DedupMark(tenant, messageID, thingID string) {
	if messageID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dedup == nil {
		f.dedup = make(map[string]struct{})
	}
	f.dedup[tenant+"\x00"+messageID+"\x00"+thingID] = struct{}{}
}

func (f *fakeStore) Children(tenant, parentID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for child := range f.children[fkey(tenant, parentID)] {
		out = append(out, child)
	}
	sort.Strings(out)
	return out
}

func (f *fakeStore) ParentOf(tenant, childID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.parents[fkey(tenant, childID)]
	return p, ok
}

func (f *fakeStore) AttachChild(tenant, childID, parentID string) error {
	if childID == parentID {
		return fmt.Errorf("self-reference denied")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ck := fkey(tenant, childID)
	if cur, ok := f.parents[ck]; ok {
		if cur == parentID {
			return nil
		}
		return fmt.Errorf("child already has parent")
	}
	// Cykelskydd: följ kedjan från föräldern.
	for cur := parentID; ; {
		if cur == childID {
			return fmt.Errorf("cycle denied")
		}
		next, ok := f.parents[fkey(tenant, cur)]
		if !ok {
			break
		}
		cur = next
	}
	f.parents[ck] = parentID
	pk := fkey(tenant, parentID)
	if f.children[pk] == nil {
		f.children[pk] = make(map[string]bool)
	}
	f.children[pk][childID] = true
	return nil
}

func (f *fakeStore) MoveChild(tenant, childID, newParentID string) error {
	if childID == newParentID {
		return fmt.Errorf("self-reference denied")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ck := fkey(tenant, childID)
	old, ok := f.parents[ck]
	if !ok {
		return fmt.Errorf("no parent for child")
	}
	if old == newParentID {
		return nil
	}
	for cur := newParentID; ; {
		if cur == childID {
			return fmt.Errorf("cycle denied")
		}
		next, ok := f.parents[fkey(tenant, cur)]
		if !ok {
			break
		}
		cur = next
	}
	delete(f.children[fkey(tenant, old)], childID)
	f.parents[ck] = newParentID
	pk := fkey(tenant, newParentID)
	if f.children[pk] == nil {
		f.children[pk] = make(map[string]bool)
	}
	f.children[pk][childID] = true
	return nil
}

func (f *fakeStore) DetachChild(tenant, childID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ck := fkey(tenant, childID)
	if parent, ok := f.parents[ck]; ok {
		delete(f.children[fkey(tenant, parent)], childID)
		delete(f.parents, ck)
	}
}

// --- Hjälpare: soptunnekatalog och rumskatalog. ---

const (
	rtDistURN = "urn:oma:lwm2m:ext:3330"
	rtTempURN = "urn:oma:lwm2m:ext:3303"
)

func rtBinCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	c := catalog.New()
	err := c.PutTemplate(catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required: []string{"distance", "level", "fillRate"},
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
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
		t.Fatalf("put wastebin: %v", err)
	}
	err = c.PutVariant(catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 1.2},
	}})
	if err != nil {
		t.Fatalf("put variant: %v", err)
	}
	return c
}

func rtBinObject() catalog.ObjectSpec {
	return catalog.ObjectSpec{
		ThingID: "bin-42", Tenant: "t",
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "milesight:214", Object: rtDistURN, Resource: "5700", Input: "distance"},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	}
}

func rtDistObs(device string, v float64, at time.Time) things.Observation {
	return things.Observation{
		Signal:     things.ParseSignalID(device, "", rtDistURN, "5700"),
		Value:      &v,
		Unit:       "m",
		Quality:    things.QualityOK,
		ObservedAt: at,
		ReceivedAt: at,
	}
}

type fakePublisher struct {
	mu     sync.Mutex
	events []pkgtypes.ThingValuesEventV1
	err    error
}

func (p *fakePublisher) PublishThingValues(_ context.Context, ev pkgtypes.ThingValuesEventV1) error {
	if p.err != nil {
		return p.err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

// R1: dubblett (samma tenant/message/sak) räknar inte om men återpublicerar.
func TestRuntimeDuplicateRepublishesWithoutRecompute(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	first, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(first) != 1 || first[0].Duplicate || len(first[0].Outcome.Events) != 3 {
		t.Fatalf("first=%+v", first)
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d, want 3", n)
	}
	second, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
	if err != nil {
		t.Fatalf("duplicate batch: %v", err)
	}
	if len(second) != 1 || !second[0].Duplicate {
		t.Fatalf("duplicate must be flagged: %+v", second)
	}
	if len(second[0].Outcome.Events) != 3 {
		t.Fatalf("duplicate must return stored events for republication")
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d after duplicate, want unchanged 3", n)
	}
}

// R2: gammal revision nekas; korrekt revision uppdaterar konfig.
func TestRuntimeRevisionConflict(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 7); !errors.Is(err, things.ErrConflict) {
		t.Fatalf("stale revision must conflict, got %v", err)
	}
	obj := rtBinObject()
	obj.ParamOverrides = map[string]float64{"sensorToBottom": 1.50}
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("update with current revision: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	out, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Nivå = 1.50 - 0.30 = 1.20 med nya parametern (inte 1.12).
	if got := *out[0].Outcome.Updated["level"].Value; got < 1.199 || got > 1.201 {
		t.Fatalf("level=%v, want 1.20 (updated config)", got)
	}
}

// R3: omstart över samma store dränerar utan omberäkning eller dubbelhistorik.
func TestRuntimeRestartDrainsWithoutRecompute(t *testing.T) {
	fs := newFakeStore()
	rt1 := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt1.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt1.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// "Omstart": nytt runtime över samma store.
	rt2 := NewRuntime(fs)
	pub := &fakePublisher{}
	n, err := rt2.DrainAll(context.Background(), pub, base.Add(time.Minute))
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 1 || pub.count() != 1 {
		t.Fatalf("drain=%d published=%d, want 1/1", n, pub.count())
	}
	ev := pub.events[0]
	if ev.EventID == "" || ev.ConfigRevision != 1 || len(ev.Values) != 3 {
		t.Fatalf("event=%+v, want stable ID, rev 1, 3 values", ev)
	}
	for _, v := range ev.Values {
		if err := v.Validate(); err != nil {
			t.Fatalf("value invalid: %v", err)
		}
		if v.EventID == "" {
			t.Fatal("value event ID missing (consumer dedup)")
		}
	}
	// Samma meddelande efter omstart: dedup överlever (ingen omberäkning).
	dup, err := rt2.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
	if err != nil {
		t.Fatalf("duplicate after restart: %v", err)
	}
	if !dup[0].Duplicate {
		t.Fatal("dedup must survive restart")
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d, want 3 (no double count)", n)
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after drain, want 0", len(pending))
	}
}

// R4: misslyckad leverans behåller kön; återleverans ger samma ID:n.
func TestRuntimePublishFailureKeepsPending(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	pub := &fakePublisher{err: errors.New("broker down")}
	if _, err := rt.DrainAndPublish(context.Background(), pub, "t", "bin-42", base); err == nil {
		t.Fatal("publish failure must propagate")
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 3 {
		t.Fatalf("pending=%d after failure, want 3 kept", len(pending))
	}
	pub.err = nil
	if _, err := rt.DrainAndPublish(context.Background(), pub, "t", "bin-42", base); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if pub.count() != 1 || len(pub.events[0].Values) != 3 {
		t.Fatalf("published=%+v", pub.events)
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after success, want 0", len(pending))
	}
}

// R5: samtidiga batchar konvergerar mot senaste mättid utan förlorade skrivningar.
func TestRuntimeConcurrentBatches(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
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
			{DeviceID: "d", Object: rtDistURN, Resource: "5700", Input: "distance"},
		},
	}
	if _, err := rt.RegisterThing("t", c, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			at := base.Add(time.Duration(i) * time.Minute)
			v := float64(i)
			_, errs[i] = rt.HandleBatch(context.Background(), "t", fmt.Sprintf("m%d", i),
				[]things.Observation{rtDistObs("d", v, at)}, at)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	st, ok := fs.LoadThing("t", "gauge-1")
	if !ok {
		t.Fatal("thing missing")
	}
	if got := *st.Current["distance"].Value; got != 9 {
		t.Fatalf("current=%v, want 9 (latest observedAt)", got)
	}
	if n := fs.historyCount("t", "gauge-1"); n != 10 {
		t.Fatalf("history=%d, want 10 (no lost writes)", n)
	}
}

// R6: flytt bevarar tillstånd; delete med barn blockeras; historik bevaras.
func TestRuntimeRelationsMoveDelete(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := catalog.New()
	roomTmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1",
		RepeatGroups: []string{"temperature."},
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
		},
	}}
	buildingTmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1",
	}}
	if err := c.PutTemplate(roomTmpl); err != nil {
		t.Fatalf("put room: %v", err)
	}
	if err := c.PutTemplate(buildingTmpl); err != nil {
		t.Fatalf("put building: %v", err)
	}
	mkRoom := func(id, parent string) catalog.ObjectSpec {
		props := map[string]things.Property{"temperature.window": {}}
		var rels []catalog.RelationRef
		if parent != "" {
			rels = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		}
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t",
			TemplateID: "room", TemplateVersion: "v1",
			Properties: props,
			Bindings: []things.Binding{
				{DeviceID: "s-" + id, Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			},
			Relations: rels,
		}
	}
	mkBuilding := func(id string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t",
			TemplateID: "building", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
		}
	}
	if _, err := rt.RegisterThing("t", c, mkBuilding("building-a"), 1); err != nil {
		t.Fatalf("register building-a: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mkBuilding("building-b"), 1); err != nil {
		t.Fatalf("register building-b: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mkRoom("room-a", "building-a"), 1); err != nil {
		t.Fatalf("register room-a: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	obs := things.Observation{
		Signal:  things.ParseSignalID("s-room-a", "", rtTempURN, "5700"),
		Value:   func() *float64 { v := 21.0; return &v }(),
		Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base,
	}
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{obs}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := rt.MoveThing("t", "room-a", "building-b"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if parent, _ := fs.ParentOf("t", "room-a"); parent != "building-b" {
		t.Fatalf("parent=%q, want building-b", parent)
	}
	st, _ := fs.LoadThing("t", "room-a")
	if got := *st.Current["temperature.window"].Value; got != 21 {
		t.Fatalf("move must preserve state, current=%v", got)
	}
	if n := fs.historyCount("t", "room-a"); n != 1 {
		t.Fatalf("move must preserve history, history=%d", n)
	}
	if err := rt.DeleteThing("t", "building-b"); !errors.Is(err, things.ErrConflict) {
		t.Fatalf("delete with children must conflict, got %v", err)
	}
	if err := rt.DeleteThing("t", "room-a"); err != nil {
		t.Fatalf("delete leaf: %v", err)
	}
	if _, ok := fs.LoadThing("t", "room-a"); ok {
		t.Fatal("thing state must be gone after delete")
	}
	if n := fs.historyCount("t", "room-a"); n != 1 {
		t.Fatalf("history must survive delete, history=%d", n)
	}
	if err := rt.DeleteThing("t", "building-b"); err != nil {
		t.Fatalf("delete after detach: %v", err)
	}
}

// R7: tenant-isolering för dedup och tillstånd.
func TestRuntimeTenantIsolation(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	objA := rtBinObject()
	objA.Tenant = "a"
	objB := rtBinObject()
	objB.Tenant = "b"
	if _, err := rt.RegisterThing("a", cat, objA, 1); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if _, err := rt.RegisterThing("b", cat, objB, 1); err != nil {
		t.Fatalf("register b: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	for _, tenant := range []string{"a", "b"} {
		out, err := rt.HandleBatch(context.Background(), tenant, "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
		if err != nil {
			t.Fatalf("batch %s: %v", tenant, err)
		}
		if len(out) != 1 || out[0].Duplicate {
			t.Fatalf("tenant %s must process independently: %+v", tenant, out)
		}
	}
}

// R8: kontraktsmappning (enhet, schema, revision, stabila ID:n, validering).
func TestRuntimeContractMapping(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 5, 0, 0, time.UTC)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := buildThingValuesEvent("t", "bin-42", map[string]string{"fillRate": "pct"}, []FlowEvent{{
		PropertyID: "fillRate", Value: 93.3, Quality: "uncertain",
		ObservedAt: at, ReceivedAt: at,
		Sources:         []string{"milesight:214/3330/5700"},
		Available:       1,
		Expected:        1,
		Operator:        "fillRate",
		OperatorVersion: "v1",
		ConfigRevision:  4,
		MessageID:       "m1",
	}}, 4, now)
	if ev.Schema != pkgtypes.SchemaV1 || ev.Tenant != "t" || ev.ThingID != "bin-42" || ev.ConfigRevision != 4 {
		t.Fatalf("event=%+v", ev)
	}
	if len(ev.Values) != 1 {
		t.Fatalf("values=%d", len(ev.Values))
	}
	v := ev.Values[0]
	if v.Unit != "pct" || v.Quality != "uncertain" || v.Source != "milesight:214/3330/5700" {
		t.Fatalf("value=%+v", v)
	}
	if !v.ObservedAt.Equal(at) || !v.PublishedAt.Equal(now) {
		t.Fatalf("times=%+v", v)
	}
	if err := v.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if v.EventID == "" || ev.EventID == "" {
		t.Fatal("stable event IDs required")
	}
	again := buildThingValuesEvent("t", "bin-42", map[string]string{"fillRate": "pct"}, []FlowEvent{{
		PropertyID: "fillRate", Value: 93.3, Quality: "uncertain",
		ObservedAt: at, ReceivedAt: at,
		Sources:         []string{"milesight:214/3330/5700"},
		Available:       1,
		Expected:        1,
		Operator:        "fillRate",
		OperatorVersion: "v1",
		ConfigRevision:  4,
		MessageID:       "m1",
	}}, 4, now.Add(time.Hour))
	if again.Values[0].EventID != v.EventID || again.EventID != ev.EventID {
		t.Fatal("event IDs must be stable across redelivery (PublishedAt excluded)")
	}
}

// Regression L6/L10: fel under beräkning får inte lämna dedup utan state.
// Samma messageID efter fel ska kunna behandlas (ej dubblett).
func TestRuntimeDedupNotMarkedOnEvaluateError(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c2 := catalog.New()
	if err := c2.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"}, Optional: []string{"level"},
	},
		Recipes: []catalog.Recipe{{
			Name: "bad", Operator: "bogus-operator", Version: "v1",
			Inputs: []string{"distance"}, Outputs: []string{"level"},
		}},
	}); err != nil {
		t.Fatalf("put c2: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "g1", Tenant: "t",
		TemplateID: "gauge", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}, "level": {}},
		Bindings: []things.Binding{
			{DeviceID: "d", Object: rtDistURN, Resource: "5700", Input: "distance"},
		},
	}
	if _, err := rt.RegisterThing("t", c2, obj, 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	_, err := rt.HandleBatch(context.Background(), "t", "m-err", []things.Observation{rtDistObs("d", 1, base)}, base)
	if err == nil {
		t.Fatal("bogus operator must fail")
	}
	if fs.DedupCheck("t", "m-err", "g1") {
		t.Fatal("dedup must not be marked after evaluate failure (atomicity)")
	}
	if n := fs.historyCount("t", "g1"); n != 0 {
		t.Fatalf("history=%d after failure, want 0", n)
	}
}

// Regression L10: tomt messageID ska avvisas (stabila outbox-ID:n kräver ID).
func TestRuntimeRejectsEmptyMessageID(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "", []things.Observation{rtDistObs("milesight:214", 0.3, base)}, base); err == nil {
		t.Fatal("empty messageID must be rejected")
	} else if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("want validation error, got %v", err)
	}
	if n := fs.historyCount("t", "bin-42"); n != 0 {
		t.Fatalf("history=%d after rejected batch, want 0", n)
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after rejected batch, want 0", len(pending))
	}
}

// Regression B19: revision inkrementeras vid lyckad uppdatering.
func TestRuntimeRevisionIncrements(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	st, _ := fs.LoadThing("t", "bin-42")
	if st.Revision != 1 {
		t.Fatalf("rev=%d, want 1", st.Revision)
	}
	obj := rtBinObject()
	obj.ParamOverrides = map[string]float64{"sensorToBottom": 1.50}
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("update with current rev: %v", err)
	}
	st, _ = fs.LoadThing("t", "bin-42")
	if st.Revision != 2 {
		t.Fatalf("rev=%d after update, want 2 (increment)", st.Revision)
	}
	if _, err := rt.RegisterThing("t", cat, obj, 1); !errors.Is(err, things.ErrConflict) {
		t.Fatalf("stale rev 1 must conflict after increment, got %v", err)
	}
	if _, err := rt.RegisterThing("t", cat, obj, 2); err != nil {
		t.Fatalf("update with rev 2: %v", err)
	}
	st, _ = fs.LoadThing("t", "bin-42")
	if st.Revision != 3 {
		t.Fatalf("rev=%d, want 3", st.Revision)
	}
	if _, err := rt.RegisterThing("t", cat, obj, 0); !errors.Is(err, things.ErrValidation) {
		t.Fatalf("rev 0 must be validation error, got %v", err)
	}
}

// Regression H7: RegisterThing med dangling parent nekas; borttagen partOf lossas.
func TestRuntimeRegisterThingParentValidation(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1",
		Relations: []things.RelationSpec{{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1}},
	}}); err != nil {
		t.Fatalf("put room: %v", err)
	}
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1",
	}}); err != nil {
		t.Fatalf("put building: %v", err)
	}
	mkRoom := func(parent string) catalog.ObjectSpec {
		var rels []catalog.RelationRef
		if parent != "" {
			rels = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		}
		return catalog.ObjectSpec{
			ThingID: "room-x", Tenant: "t",
			TemplateID: "room", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
			Relations:  rels,
		}
	}
	mkBuilding := func(id string) catalog.ObjectSpec {
		return catalog.ObjectSpec{ThingID: id, Tenant: "t", TemplateID: "building", TemplateVersion: "v1", Properties: map[string]things.Property{}}
	}
	// Dangling parent ska nekas.
	if _, err := rt.RegisterThing("t", c, mkRoom("missing"), 1); err == nil {
		t.Fatal("dangling parent must be denied")
	} else if !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mkBuilding("b-a"), 1); err != nil {
		t.Fatalf("register b-a: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mkBuilding("b-b"), 1); err != nil {
		t.Fatalf("register b-b: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mkRoom("b-a"), 1); err != nil {
		t.Fatalf("register room->b-a: %v", err)
	}
	if parent, _ := fs.ParentOf("t", "room-x"); parent != "b-a" {
		t.Fatalf("parent=%q, want b-a", parent)
	}
	// Borttagen partOf ska koppla loss (lämna ej kvar gammal länk).
	st, _ := fs.LoadThing("t", "room-x")
	if _, err := rt.RegisterThing("t", c, mkRoom(""), st.Revision); err != nil {
		t.Fatalf("remove parent: %v", err)
	}
	if _, ok := fs.ParentOf("t", "room-x"); ok {
		t.Fatal("removed partOf must detach (no stale parent)")
	}
}

// Regression L6: drain stämplar batch-revision, ej aktuell revision.
func TestRuntimeDrainUsesBatchRevision(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// Uppdatera config (rev 1 -> 2) före drain.
	obj := rtBinObject()
	obj.ParamOverrides = map[string]float64{"sensorToBottom": 1.50}
	if _, err := rt.RegisterThing("t", cat, obj, 1); err != nil {
		t.Fatalf("update: %v", err)
	}
	pub := &fakePublisher{}
	if _, err := rt.DrainAndPublish(context.Background(), pub, "t", "bin-42", base.Add(time.Minute)); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("published=%d, want 1", pub.count())
	}
	if got := pub.events[0].ConfigRevision; got != 1 {
		t.Fatalf("event revision=%d, want batch revision 1 (not current 2)", got)
	}
}

// Regression P12: batch-EventID sanerar tenant/thing ("/" -> "~").
func TestRuntimeBatchEventIDSanitizes(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := buildThingValuesEvent("a/b", "c/d", map[string]string{}, []FlowEvent{{
		PropertyID: "p", Value: 1, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m/x",
	}}, 1, at)
	want := "v1/a~b/c~d/m~x"
	if ev.EventID != want {
		t.Fatalf("EventID=%q, want %q", ev.EventID, want)
	}
	if got := StableEventID("a/b", "c/d", "m/x", "p"); got != "v1/a~b/c~d/m~x/p" {
		t.Fatalf("StableEventID=%q", got)
	}
}

// Regression L7: partiellt lyckad drain rensar lyckade grupper direkt.
func TestRuntimePartialDrainClearsIncrementally(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch m1: %v", err)
	}
	if _, err := rt.HandleBatch(context.Background(), "t", "m2", []things.Observation{rtDistObs("milesight:214", 0.40, base.Add(time.Minute))}, base.Add(time.Minute)); err != nil {
		t.Fatalf("batch m2: %v", err)
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 6 {
		t.Fatalf("pending=%d, want 6 (2 batches x 3)", len(pending))
	}
	calls := 0
	failSecond := &failAfterNPublisher{n: 1, err: errors.New("broker down")}
	_ = calls
	if _, err := rt.DrainAndPublish(context.Background(), failSecond, "t", "bin-42", base.Add(2*time.Minute)); err == nil {
		t.Fatal("second group failure must propagate")
	}
	// Första gruppen (m1) ska vara rensad trots att andra fallerade.
	remaining := fs.PendingEvents("t", "bin-42")
	if len(remaining) != 3 {
		t.Fatalf("remaining=%d, want 3 (m2 only, m1 cleared incrementally)", len(remaining))
	}
	for _, e := range remaining {
		if e.MessageID != "m2" {
			t.Fatalf("remaining must be m2, got %q", e.MessageID)
		}
	}
}

// Regression L7/L10: delete rensar outbox+dedup men bevarar historik.
func TestRuntimeDeleteClearsOutboxDedup(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := rt.DeleteThing("t", "bin-42"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after delete, want 0 (no orphan)", len(pending))
	}
	if fs.DedupCheck("t", "m1", "bin-42") {
		t.Fatal("dedup must be forgotten after delete")
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d, want preserved 3", n)
	}
	// Nyskapad sak med samma ID ska kunna behandla samma messageID på nytt.
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	out, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base)
	if err != nil {
		t.Fatalf("batch after recreate: %v", err)
	}
	if len(out) != 1 || out[0].Duplicate {
		t.Fatalf("recreated thing must process anew, got %+v", out)
	}
}

type failAfterNPublisher struct {
	n     int
	calls int
	err   error
}

func (p *failAfterNPublisher) PublishThingValues(_ context.Context, _ pkgtypes.ThingValuesEventV1) error {
	if p.calls >= p.n {
		return p.err
	}
	p.calls++
	return nil
}

// Regression H3/H7: cykel via RegisterThing-relationer nekas.
func TestRuntimeRegisterCycleDenied(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "node", Version: "v1",
		Relations: []things.RelationSpec{{Name: "partOf", AllowedTargets: []string{"node"}, Min: 0, Max: 1}},
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	mk := func(id, parent string) catalog.ObjectSpec {
		var rels []catalog.RelationRef
		if parent != "" {
			rels = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "node"}}
		}
		return catalog.ObjectSpec{ThingID: id, Tenant: "t", TemplateID: "node", TemplateVersion: "v1", Properties: map[string]things.Property{}, Relations: rels}
	}
	if _, err := rt.RegisterThing("t", c, mk("a", ""), 1); err != nil {
		t.Fatalf("register a: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mk("b", "a"), 1); err != nil {
		t.Fatalf("register b->a: %v", err)
	}
	// a->b skulle sluta cykeln a->b->a.
	st, _ := fs.LoadThing("t", "a")
	if _, err := rt.RegisterThing("t", c, mk("a", "b"), st.Revision); err == nil {
		t.Fatal("cycle a->b->a via RegisterThing must be denied")
	}
	// Självreferens nekas redan i Compile, men verifiera via MoveThing också.
	if err := rt.MoveThing("t", "b", "b"); err == nil {
		t.Fatal("self-move must be denied")
	}
}
