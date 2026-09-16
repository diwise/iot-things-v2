package storage

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ThingRecord är ett minimalt minnesobjekt.
// Revision hanteras av anroparen: Upsert behåller inkommande Revision
// oförändrad (ingen auto-inkrementering här).
type ThingRecord struct {
	ID       string
	Tenant   string
	Revision int64
	Data     []byte
}

// MemThingStore är ett generiskt minneslager för things.
// Nollvärdet är inte brukbart utan NewMemThingStore (initierar karta),
// men Upsert/Get/ListByTenant tolererar nil-karta.
type MemThingStore struct {
	mu sync.Mutex
	m  map[string]ThingRecord
}

// NewMemThingStore skapar ett tomt thing-lager.
func NewMemThingStore() *MemThingStore {
	return &MemThingStore{m: make(map[string]ThingRecord)}
}

func thingKey(tenant, id string) string {
	return tenant + "\x00" + id
}

// Upsert lagrar rec och behåller rec.Revision som den är.
// Data kopieras för att undvika aliasing mot anroparen.
func (s *MemThingStore) Upsert(rec ThingRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]ThingRecord)
	}
	cp := rec
	if rec.Data != nil {
		cp.Data = append([]byte(nil), rec.Data...)
	}
	s.m[thingKey(rec.Tenant, rec.ID)] = cp
}

// Get returnerar (data, revision, ok). Data är en kopia.
func (s *MemThingStore) Get(tenant, id string) ([]byte, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.m[thingKey(tenant, id)]
	if !ok {
		return nil, 0, false
	}
	var cp []byte
	if rec.Data != nil {
		cp = append([]byte(nil), rec.Data...)
	}
	return cp, rec.Revision, true
}

// Delete tar bort ett record. Historik/dedup/outbox påverkas inte
// (värdehistorik bevaras vid borttagning av sak).
func (s *MemThingStore) Delete(tenant, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, thingKey(tenant, id))
}

// Insert lagrar rec endast om nyckeln saknas (create-if-absent för
// samtidig registrering). Returnerar false om posten redan finns.
func (s *MemThingStore) Insert(rec ThingRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]ThingRecord)
	}
	key := thingKey(rec.Tenant, rec.ID)
	if _, exists := s.m[key]; exists {
		return false
	}
	cp := rec
	if rec.Data != nil {
		cp.Data = append([]byte(nil), rec.Data...)
	}
	s.m[key] = cp
	return true
}

// CompareAndSwap lagrar rec endast om lagrad revision är expected
// (optimistisk låsning över instansgränser). Returnerar false vid
// konflikt eller saknad post.
func (s *MemThingStore) CompareAndSwap(tenant, id string, expected int64, rec ThingRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[thingKey(tenant, id)]
	if !ok || cur.Revision != expected {
		return false
	}
	cp := rec
	if rec.Data != nil {
		cp.Data = append([]byte(nil), rec.Data...)
	}
	s.m[thingKey(tenant, id)] = cp
	return true
}

// ListByTenant returnerar alla records för tenant, sorterade på ID.
func (s *MemThingStore) ListByTenant(tenant string) []ThingRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ThingRecord
	for _, rec := range s.m {
		if rec.Tenant != tenant {
			continue
		}
		cp := rec
		if rec.Data != nil {
			cp.Data = append([]byte(nil), rec.Data...)
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Tenants returnerar sorterade tenants med minst ett record (för
// uppstartsdränering av utboxen över alla tenants).
func (s *MemThingStore) Tenants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{})
	for _, rec := range s.m {
		seen[rec.Tenant] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for tenant := range seen {
		out = append(out, tenant)
	}
	sort.Strings(out)
	return out
}

// HistoryRecord är en minimal historikrad. Egna typer används lokalt
// för att undvika importcykel mot application/things; API kan anpassas av integrator.
// Late markerar sena observationer som sparas utan att bli aktuella (B13)
// och måste bevaras genom lagring/läsning (Etapp 6 P5).
type HistoryRecord struct {
	Tenant     string
	ThingID    string
	PropertyID string
	ObservedAt time.Time
	ReceivedAt time.Time
	Value      *float64
	Quality    string
	Source     string
	MessageID  string
	Late       bool
}

// MemHistoryStore lagrar historik i minnet med idempotent append.
type MemHistoryStore struct {
	mu   sync.Mutex
	recs []HistoryRecord
	seen map[string]struct{}
}

// NewMemHistoryStore skapar ett tomt historiklagar.
func NewMemHistoryStore() *MemHistoryStore {
	return &MemHistoryStore{seen: make(map[string]struct{})}
}

func historyDedupKey(tenant, thingID, propID string, observed time.Time, source, quality string, value *float64) string {
	val := "<nil>"
	if value != nil {
		// -0 conflatas med 0 för att matcha PostgreSQL (float8 '=' ser
		// -0 == 0 i UNIQUE-index; FormatFloat skiljer "-0"/"0").
		if *value == 0 {
			val = "0"
		} else {
			val = strconv.FormatFloat(*value, 'g', -1, 64)
		}
	}
	// Dedup-nyckel: tenant|thing|prop|observedUnixMicro|source|quality|value.
	// Tenant bevarar isolering; quality ingår så att en kvalitetsändring
	// (ok->uncertain) med samma värde/tid/källa inte tappas som dubblett.
	// MessageID/ReceivedAt ingår avsiktligt ej: retry med nytt ID men samma
	// innehåll ska dedupliceras.
	// Mättid normaliseras till mikrosekund (Round) för att matcha
	// PostgreSQL TIMESTAMPTZ som avrundar sub-mikrosekund vid lagring;
	// utan detta skulle två nanos som avrundas till samma mikrosekund
	// dedupliceras i pg men ej i minnet.
	return tenant + "|" + thingID + "|" + propID + "|" +
		strconv.FormatInt(canonicalObserved(observed).UnixNano(), 10) + "|" + source + "|" + quality + "|" + val
}

// canonicalObserved normaliserar mättid till mikrosekundprecision (samma som
// PostgreSQL TIMESTAMPTZ). Noll-tid lämnas orörd.
func canonicalObserved(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.Round(time.Microsecond)
}

func copyFloat(v *float64) *float64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

// AppendIdempotent lägger till rec om dedup-nyckeln inte setts tidigare.
// Returnerar true om raden lades till, false vid dubblett (hoppas över).
func (s *MemHistoryStore) AppendIdempotent(rec HistoryRecord) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	rec.ObservedAt = canonicalObserved(rec.ObservedAt)
	key := historyDedupKey(rec.Tenant, rec.ThingID, rec.PropertyID, rec.ObservedAt, rec.Source, rec.Quality, rec.Value)
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	cp := rec
	cp.Value = copyFloat(rec.Value)
	s.recs = append(s.recs, cp)
	return true
}

// List filtrerar på tenant/thing/prop (exakt matchning på prop om angiven;
// tom propID betyder alla egenskaper) och på ObservedAt.
// Noll-tid i from/to betyder ingen undre/övre gräns (inklusive gränser).
// limit > 0 kapar resultatet (äldst först), annars returneras allt.
// Sorterat på ObservedAt stigande.
func (s *MemHistoryStore) List(tenant, thingID, propID string, from, to time.Time, limit int) []HistoryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []HistoryRecord
	for _, r := range s.recs {
		if r.Tenant != tenant || r.ThingID != thingID {
			continue
		}
		if propID != "" && r.PropertyID != propID {
			continue
		}
		if !from.IsZero() && r.ObservedAt.Before(from) {
			continue
		}
		if !to.IsZero() && r.ObservedAt.After(to) {
			continue
		}
		cp := r
		cp.Value = copyFloat(r.Value)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			if out[i].ReceivedAt.Equal(out[j].ReceivedAt) {
				// Tredje tie-break per B13 (käll-ID-ordning) för
				// deterministisk ordning vid lika mättid+mottagning.
				return out[i].Source < out[j].Source
			}
			return out[i].ReceivedAt.Before(out[j].ReceivedAt)
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// RelationRecord är en minimal relation. Konvention: FromID = barn,
// ToID = förälder (gäller särskilt Type "partOf").
type RelationRecord struct {
	ID     string
	Tenant string
	Type   string
	FromID string
	ToID   string
}

// MemRelationStore lagrar relationer i minnet med cykel-skydd för partOf:
// självreferens (From==To) nekas alltid, längre partOf-cykler nekas via
// föräldrakedjan, och ett barn får högst en "partOf"-förälder
// (index per tenant+barn). Tenant ingår i alla nycklar; frågor filtrerar
// alltid på tenant så att cross-tenant-länkar varken lagras som giltiga
// hierarkier eller läcker via Children/Parent.
type MemRelationStore struct {
	mu          sync.Mutex
	byID        map[string]RelationRecord
	childParent map[string]string // tenant\x00barnID -> relations-ID (endast partOf)
}

// NewMemRelationStore skapar ett tomt relationslager.
func NewMemRelationStore() *MemRelationStore {
	return &MemRelationStore{
		byID:        make(map[string]RelationRecord),
		childParent: make(map[string]string),
	}
}

func relationIDKey(tenant, id string) string {
	return tenant + "\x00" + id
}

func relationChildKey(tenant, childID string) string {
	return tenant + "\x00" + childID
}

// partOfCreatesCycle rapporterar om kanten child->newParent skulle skapa en
// partOf-cykel inom tenant genom att följa föräldrakedjan från newParent.
// Anroparen måste hålla s.mu.
func (s *MemRelationStore) partOfCreatesCycle(tenant, childID, newParentID string) bool {
	cur := newParentID
	for i := 0; i <= len(s.byID)+1; i++ {
		if cur == childID {
			return true
		}
		relID, ok := s.childParent[relationChildKey(tenant, cur)]
		if !ok {
			return false
		}
		rec, ok := s.byID[relationIDKey(tenant, relID)]
		if !ok {
			return false
		}
		if rec.ToID == "" || rec.ToID == cur {
			return false
		}
		cur = rec.ToID
	}
	return true
}

// Add lägger till en relation. Returnerar fel vid självreferens,
// partOf-cykel, dubblett-ID eller andra partOf-förälder för samma barn.
// Samma barn->samma förälder med samma typ är idempotent (nil).
func (s *MemRelationStore) Add(rec RelationRecord) error {
	if rec.ID == "" {
		return fmt.Errorf("relation: missing ID")
	}
	if rec.Tenant == "" {
		return fmt.Errorf("relation: missing tenant")
	}
	if rec.FromID == "" || rec.ToID == "" {
		return fmt.Errorf("relation: missing FromID/ToID")
	}
	if rec.FromID == rec.ToID {
		return fmt.Errorf("relation: self-reference denied (%q)", rec.FromID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byID == nil {
		s.byID = make(map[string]RelationRecord)
	}
	if s.childParent == nil {
		s.childParent = make(map[string]string)
	}
	idKey := relationIDKey(rec.Tenant, rec.ID)
	if _, exists := s.byID[idKey]; exists {
		return fmt.Errorf("relation: duplicate ID %q", rec.ID)
	}
	if rec.Type == "partOf" {
		childKey := relationChildKey(rec.Tenant, rec.FromID)
		if existingID, ok := s.childParent[childKey]; ok {
			existing := s.byID[relationIDKey(rec.Tenant, existingID)]
			if existing.ToID == rec.ToID && existing.FromID == rec.FromID {
				return nil
			}
			return fmt.Errorf("relation: child %q already has partOf parent %q", rec.FromID, existing.ToID)
		}
		if s.partOfCreatesCycle(rec.Tenant, rec.FromID, rec.ToID) {
			return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", rec.FromID, rec.ToID)
		}
		s.childParent[childKey] = rec.ID
	}
	s.byID[idKey] = rec
	return nil
}

// Move flyttar ett barns partOf-förälder till newParentID.
// Fel vid självreferens, partOf-cykel eller om barnet saknar partOf-relation.
func (s *MemRelationStore) Move(tenant, childID, newParentID string) error {
	if childID == "" || newParentID == "" {
		return fmt.Errorf("relation: missing child/new parent")
	}
	if childID == newParentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	childKey := relationChildKey(tenant, childID)
	relID, ok := s.childParent[childKey]
	if !ok {
		return fmt.Errorf("relation: no partOf parent for child %q", childID)
	}
	idKey := relationIDKey(tenant, relID)
	rec, ok := s.byID[idKey]
	if !ok {
		return fmt.Errorf("relation: index corrupt for child %q", childID)
	}
	if rec.ToID != newParentID && s.partOfCreatesCycle(tenant, childID, newParentID) {
		return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", childID, newParentID)
	}
	rec.ToID = newParentID
	s.byID[idKey] = rec
	return nil
}

// Children returnerar relationer där ToID == parentID för tenant,
// sorterade på FromID.
func (s *MemRelationStore) Children(tenant, parentID string) []RelationRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []RelationRecord
	for _, r := range s.byID {
		if r.Tenant != tenant || r.ToID != parentID {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FromID < out[j].FromID })
	return out
}

// Parent returnerar barnets partOf-relation (FromID=barn). false om saknas.
func (s *MemRelationStore) Parent(tenant, childID string) (RelationRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	relID, ok := s.childParent[relationChildKey(tenant, childID)]
	if !ok {
		return RelationRecord{}, false
	}
	rec, ok := s.byID[relationIDKey(tenant, relID)]
	if !ok {
		return RelationRecord{}, false
	}
	return rec, true
}

// RemoveOutgoing tar bort alla relationer från fromID inom tenant (för
// borttagning av sak; inkommande barnlänkar blockerar borttagning i stället
// för att kaskaderas). Returnerar antal borttagna.
func (s *MemRelationStore) RemoveOutgoing(tenant, fromID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, rec := range s.byID {
		if rec.Tenant != tenant || rec.FromID != fromID {
			continue
		}
		delete(s.byID, key)
		if rec.Type == "partOf" {
			delete(s.childParent, relationChildKey(tenant, fromID))
		}
		removed++
	}
	return removed
}

// OutboxEvent är en minimal utbox-händelse. Payload bär den fullständiga
// händelsen (t.ex. serialiserad FlowEvent) så att omstart kan återpublicera
// utan omberäkning; ID är det stabila deduplicerings-ID:t.
type OutboxEvent struct {
	ID         string
	Tenant     string
	ThingID    string
	PropertyID string
	ObservedAt time.Time
	Revision   int64
	Payload    []byte
}

// MemOutbox är ett minnesbaserat utbox-lager med idempotent append på ID.
// Tomt ID kan inte dedupliceras och läggs alltid till. Sedda ID:n behålls
// även efter ListAndClear så att retry med samma ID inte återpubliceras
// (minnesstorlek växer obegränsat; retention enligt K4 återstår för drift).
type MemOutbox struct {
	mu     sync.Mutex
	events []OutboxEvent
	seen   map[string]struct{}
}

// NewMemOutbox skapar en tom utbox.
func NewMemOutbox() *MemOutbox {
	return &MemOutbox{seen: make(map[string]struct{})}
}

// Append lägger till en händelse. Returnerar false vid dubblett-ID
// (hoppas över) och true när händelsen lagrades. Payload kopieras.
func (o *MemOutbox) Append(ev OutboxEvent) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.seen == nil {
		o.seen = make(map[string]struct{})
	}
	if ev.ID != "" {
		if _, ok := o.seen[ev.ID]; ok {
			return false
		}
		o.seen[ev.ID] = struct{}{}
	}
	ev.Payload = append([]byte(nil), ev.Payload...)
	o.events = append(o.events, ev)
	return true
}

// ListAndClear returnerar alla händelser och tömmer utboxen.
// Sedda ID:n behålls för att retry med samma ID fortsatt ska dedupliceras.
func (o *MemOutbox) ListAndClear() []OutboxEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := o.events
	o.events = nil
	return out
}

// PeekFor returnerar kopior av väntande händelser för tenant+thing utan att
// tömma (för publicering med bekräftelse: rensa först efter lyckad leverans).
// Sorterat på ObservedAt, PropertyID, ID — samma ordning som PgStore
// (ORDER BY observed_at, property_id, id) för deterministisk dränering.
func (o *MemOutbox) PeekFor(tenant, thingID string) []OutboxEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []OutboxEvent
	for _, ev := range o.events {
		if ev.Tenant != tenant || ev.ThingID != thingID {
			continue
		}
		cp := ev
		cp.Payload = append([]byte(nil), ev.Payload...)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			if out[i].PropertyID != out[j].PropertyID {
				return out[i].PropertyID < out[j].PropertyID
			}
			return out[i].ID < out[j].ID
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	return out
}

// ClearIDs tar bort väntande händelser med givna ID:n för tenant+thing.
// Sedda ID:n behålls så att återleverans fortsatt dedupliceras. Används
// efter lyckad publicering så att händelser som tillkommit under pågående
// leverans inte tappas.
func (o *MemOutbox) ClearIDs(tenant, thingID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	origLen := len(o.events)
	kept := o.events[:0]
	for _, ev := range o.events {
		if ev.Tenant == tenant && ev.ThingID == thingID {
			if _, ok := want[ev.ID]; ok {
				continue
			}
		}
		kept = append(kept, ev)
	}
	for i := len(kept); i < origLen; i++ {
		o.events[i] = OutboxEvent{}
	}
	o.events = kept
}

// Len returnerar antal lagrade händelser.
func (o *MemOutbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.events)
}

// Forget tar bort alla väntande händelser för tenant+thing samt deras sedda
// ID:n (för borttagning av sak: historik bevaras annorstädes, men föräldralös
// outbox får inte bli kvar och blockera nyskapad sak med samma ID).
func (o *MemOutbox) Forget(tenant, thingID string) {
	if tenant == "" || thingID == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	origLen := len(o.events)
	dropped := make(map[string]struct{})
	kept := o.events[:0]
	for _, ev := range o.events {
		if ev.Tenant == tenant && ev.ThingID == thingID {
			if ev.ID != "" {
				dropped[ev.ID] = struct{}{}
			}
			continue
		}
		kept = append(kept, ev)
	}
	for i := len(kept); i < origLen; i++ {
		o.events[i] = OutboxEvent{}
	}
	o.events = kept
	for id := range dropped {
		delete(o.seen, id)
	}
	// Rensa även redan levererade (ej längre väntande) ID:n för saken så att
	// nyskapad sak med samma ID inte dedupliceras mot gammal livscykel.
	// Stabila ID:n har formen v1/<tenant>/<thing>/<msg>/<prop> med "/" -> "~".
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
	prefix := "v1/" + sanitize(tenant) + "/" + sanitize(thingID) + "/"
	for id := range o.seen {
		if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
			delete(o.seen, id)
		}
	}
}

// MemDedup är ett enkelt idempotens-filter per (tenant, messageID, thingID).
// Tenant ingår i nyckeln så att samma messageID/thingID i olika tenants
// inte kolliderar (tenant-isolering).
type MemDedup struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemDedup skapar ett tomt dedup-filter.
func NewMemDedup() *MemDedup {
	return &MemDedup{seen: make(map[string]struct{})}
}

func dedupKey(tenant, messageID, thingID string) string {
	return tenant + "\x00" + messageID + "\x00" + thingID
}

// SeenOrMark returnerar true om (tenant, messageID, thingID) redan setts,
// annars markeras kombinationen och false returneras.
// Tomt messageID kan inte dedupliceras: returnerar alltid false.
func (d *MemDedup) SeenOrMark(tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = make(map[string]struct{})
	}
	key := dedupKey(tenant, messageID, thingID)
	if _, ok := d.seen[key]; ok {
		return true
	}
	d.seen[key] = struct{}{}
	return false
}

// Has rapporterar om (tenant, messageID, thingID) redan setts utan att
// markera (check-only för atomär dedup: markera först efter lyckad persist).
// Tomt messageID ger alltid false.
func (d *MemDedup) Has(tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[dedupKey(tenant, messageID, thingID)]
	return ok
}

// Mark markerar (tenant, messageID, thingID) som sedd (no-op för tomt ID).
func (d *MemDedup) Mark(tenant, messageID, thingID string) {
	if messageID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = make(map[string]struct{})
	}
	d.seen[dedupKey(tenant, messageID, thingID)] = struct{}{}
}

// ForgetThing rensar alla dedup-markörer för en sak (för borttagning:
// nyskapad sak med samma ID ska kunna behandla samma messageID på nytt).
func (d *MemDedup) ForgetThing(tenant, thingID string) {
	if tenant == "" || thingID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	suffix := "\x00" + thingID
	prefix := tenant + "\x00"
	for key := range d.seen {
		if len(key) < len(prefix)+len(suffix) {
			continue
		}
		if key[:len(prefix)] != prefix {
			continue
		}
		if key[len(key)-len(suffix):] != suffix {
			continue
		}
		delete(d.seen, key)
	}
}
