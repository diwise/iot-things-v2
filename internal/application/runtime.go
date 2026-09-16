// Runtime Etapp 4: beständighet och leveranssäkerhet.
//
// Runtime äger behandlingsprotokollet mot ett RuntimeStore: en batch
// persisteras som en atomär enhet (dedupkontroll -> historik -> aktuellt ->
// outbox) under ett lås, så samtidiga uppdateringar av samma sak
// serialiseras och ingen partiell batch blir synlig. Retry konvergerar via
// idempotens: dedup per (tenant, messageID, sak) hoppar över omberäkning
// men återpublicerar samma händelser (at-least-once); historikappend är
// idempotent per innehåll; utbox-ID:n är stabila och överlever ClearIDs
// efter lyckad leverans. Omstart är säker: nytt Runtime över samma store
// dränerar väntande händelser utan omberäkning och utan dubblerad historik.
//
// Tillståndet (konfiguration, aktuellt, dedupmarkörer, historik, outbox)
// ligger i storet; Runtime självt håller inget beständigt tillstånd och kan
// därför startas om fritt. Minnesbackend: internal/infrastructure/storage.
// Beständig PostgreSQL-backend: K4.
package application

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// StoredThingState är det persisterade tillståndet per sak. Revision är
// konfigurationsversionen (API-ytan, If-Match); DataRevision är
// skrivstaketet för CAS (bumpas vid varje blob-skrivning, även batchar
// utan configändring) så att samtidiga skribenter upptäcks.
type StoredThingState struct {
	ConfigJSON      []byte                     `json:"config"`
	Revision        int64                      `json:"revision"`
	DataRevision    int64                      `json:"dataRev"`
	Current         map[string]CurrentValue    `json:"current,omitempty"`
	ProcessorStates map[string]StoredProcessor `json:"processors,omitempty"`
	LastMessageID   string                     `json:"lastMessageId,omitempty"`
	LastEvents      []FlowEvent                `json:"lastEvents,omitempty"`
}

// RuntimeStore är persistensgränsen för Runtime. Minnesimplementation:
// internal/infrastructure/storage RuntimeAdapter; beständig implementation:
// samma pakets PgStore. Metoderna är avsiktligt finkorniga. Tvärsnitts-
// atomicitet bygger på idempotens + revisionerad CAS i stället för
// distribuerade transaktioner: samtidig skrivning upptäcks (false) och
// anroparen läser om + räknar om + försöker igen (begränsat antal).
type RuntimeStore interface {
	LoadThing(tenant, thingID string) (StoredThingState, bool)
	// InsertThing lagrar ny sak; false om den redan finns.
	InsertThing(tenant, thingID string, st StoredThingState) bool
	// SaveThingCAS lagrar endast om lagrat skrivstaket är expected
	// (DataRevision, ej konfigurationsrevisionen).
	SaveThingCAS(tenant, thingID string, expected int64, st StoredThingState) bool
	// CommitBatch persisterar en omgång atomärt: blob-CAS på expected
	// staket + idempotent historik + idempotent pending. False vid
	// staketkonflikt (inget skrivs); anroparen läser om och försöker igen.
	CommitBatch(tenant, thingID string, expected int64, st StoredThingState, hist []HistoryWrite, pending []FlowEvent) bool
	DeleteThingState(tenant, thingID string)
	ListThingIDs(tenant string) []string
	ListTenants() []string
	AppendHistory(tenant, thingID string, h HistoryWrite) bool
	History(tenant, thingID, prop string, from, to time.Time, limit int) []HistoryWrite
	PendingEvents(tenant, thingID string) []FlowEvent
	StorePending(tenant, thingID string, events []FlowEvent)
	ClearPendingIDs(tenant, thingID string, ids []string)
	DedupCheck(tenant, messageID, thingID string) bool
	DedupMark(tenant, messageID, thingID string)
	Children(tenant, parentID string) []string
	ParentOf(tenant, childID string) (string, bool)
	AttachChild(tenant, childID, parentID string) error
	MoveChild(tenant, childID, newParentID string) error
	DetachChild(tenant, childID string)
}

// EventPublisher levererar konvergerade utbox-batchar. Produktions-
// implementation: TopicPublisher nedan (messaging-golang). Tester använder
// en fake.
type EventPublisher interface {
	PublishThingValues(ctx context.Context, ev pkgtypes.ThingValuesEventV1) error
}

// Runtime serialiserar batchar per instans. En instans betjänar alla
// tenants; tenant-isolering upprätthålls av storet och av att tillstånd
// alltid nycklas med tenant.
type Runtime struct {
	mu    sync.Mutex
	store RuntimeStore
}

// NewRuntime skapar ett runtime över store. Samma store kan delas av ett
// ersättande runtime vid omstart (dränering utan omberäkning).
func NewRuntime(store RuntimeStore) *Runtime {
	return &Runtime{store: store}
}

// RegisterThing kompilerar obj via cat och lagrar konfiguration + revision.
// Aktuellt tillstånd, historik och dedupmarkörer bevaras vid omregistrering.
// Gammal revision vid uppdatering nekas med ErrConflict (optimistisk
// låsning, B19): anroparen ska läsa aktuell revision och försöka igen.
// Vid lyckad uppdatering inkrementeras revisionen (If-Match-semantik);
// nyskapad sak får den begärda revisionen (kräver revision > 0).
func (rt *Runtime) RegisterThing(tenant string, cat *catalog.Catalog, obj catalog.ObjectSpec, revision int64) (*catalog.EffectiveConfig, error) {
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant must be provided", things.ErrValidation)
	}
	if revision <= 0 {
		return nil, fmt.Errorf("%w: revision must be positive", things.ErrValidation)
	}
	cfg, err := cat.Compile(obj)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: encode config: %w", err)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st, exists := rt.store.LoadThing(tenant, obj.ThingID)
	fence := st.DataRevision
	var newRevision int64
	if !exists {
		newRevision = revision
		st.DataRevision = 1
	} else {
		if st.Revision != revision {
			return nil, fmt.Errorf("%w: thing %q has revision %d, request had %d",
				things.ErrConflict, obj.ThingID, st.Revision, revision)
		}
		newRevision = revision + 1
		st.DataRevision++
	}
	// Relationsfel ska komma före blob-skrivning: validera mål och beräkna
	// planerade länkändringar utan att mutera ännu.
	type relOp struct {
		op     string // "attach", "move" eller "detach"
		target string
	}
	var relOps []relOp
	hasPartOf := false
	for _, rel := range cfg.Relations {
		// Hierarkimedlemskap (partOf) persisteras som barn->förälder;
		// övriga namngivna platser är vyer som härleds vid läsning.
		if rel.Name != "partOf" {
			continue
		}
		hasPartOf = true
		if _, ok := rt.store.LoadThing(tenant, rel.TargetThingID); !ok {
			return nil, fmt.Errorf("%w: relation target %q", things.ErrNotFound, rel.TargetThingID)
		}
		if cur, ok := rt.store.ParentOf(tenant, obj.ThingID); ok {
			if cur == rel.TargetThingID {
				continue
			}
			relOps = append(relOps, relOp{op: "move", target: rel.TargetThingID})
			continue
		}
		relOps = append(relOps, relOp{op: "attach", target: rel.TargetThingID})
	}
	if !hasPartOf {
		// Borttagen partOf i ny config ska koppla loss (lämna ej kvar gammal länk).
		if _, ok := rt.store.ParentOf(tenant, obj.ThingID); ok {
			relOps = append(relOps, relOp{op: "detach"})
		}
	}
	st.Revision = newRevision
	st.ConfigJSON = raw
	if st.Current == nil {
		st.Current = make(map[string]CurrentValue)
	}
	if !exists {
		if !rt.store.InsertThing(tenant, obj.ThingID, st) {
			return nil, fmt.Errorf("%w: thing %q created concurrently",
				things.ErrConflict, obj.ThingID)
		}
	} else if !rt.store.SaveThingCAS(tenant, obj.ThingID, fence, st) {
		return nil, fmt.Errorf("%w: thing %q changed concurrently",
			things.ErrConflict, obj.ThingID)
	}
	for _, op := range relOps {
		var err error
		switch op.op {
		case "attach":
			err = rt.store.AttachChild(tenant, obj.ThingID, op.target)
		case "move":
			err = rt.store.MoveChild(tenant, obj.ThingID, op.target)
		case "detach":
			rt.store.DetachChild(tenant, obj.ThingID)
		}
		if err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// BatchOutcome är resultatet per sak för en batch.
type BatchOutcome struct {
	ThingID   string
	Outcome   *FlowOutcome
	Duplicate bool // true när dedup hoppade över omberäkning (återpublicering)
}

// HandleBatch behandlar obs som en atomär omgång: observationer routas till
// saker via bindningsmatchning (delade strömmar inom tenant tillåtna),
// varje berörd sak behandlas exakt en gång mot låst konfigurationsrevision,
// och tillstånd + historik + outbox persisteras innan retur. Samma
// (tenant, messageID, sak) twice -> ingen omberäkning; lagrade händelser
// returneras för återpublicering.
func (rt *Runtime) HandleBatch(ctx context.Context, tenant, messageID string, obs []things.Observation, receivedAt time.Time) ([]BatchOutcome, error) {
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant must be provided", things.ErrValidation)
	}
	if messageID == "" {
		return nil, fmt.Errorf("%w: messageID must be provided", things.ErrValidation)
	}
	_ = ctx
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	states := make(map[string]*storedDecoded)
	for _, id := range rt.store.ListThingIDs(tenant) {
		st, ok := rt.store.LoadThing(tenant, id)
		if !ok {
			continue
		}
		cfg, err := decodeConfig(st.ConfigJSON)
		if err != nil {
			return nil, fmt.Errorf("runtime: thing %q has undecodable config: %w", id, err)
		}
		states[id] = &storedDecoded{state: st, config: cfg}
	}

	// Routa: bevara batchordning per sak (EvaluateBatchFlow sorterar ändå
	// deterministiskt, men stabil inmatning underlättar felsökning).
	grouped := make(map[string][]things.Observation)
	order := []string{}
	for _, o := range obs {
		for id, sd := range states {
			if len(matchingInputs(sd.config.Bindings, o.Signal)) == 0 {
				continue
			}
			if _, ok := grouped[id]; !ok {
				order = append(order, id)
			}
			grouped[id] = append(grouped[id], o)
		}
	}
	sort.Strings(order)

	outcomes := make([]BatchOutcome, 0, len(order))
	for _, id := range order {
		sd := states[id]
		// Atomär dedup (L6/L10): check-only före beräkning; markera först
		// efter lyckad persist så att fel/omstart inte lämnar dedup utan state.
		if rt.store.DedupCheck(tenant, messageID, id) {
			outcomes = append(outcomes, BatchOutcome{
				ThingID:   id,
				Outcome:   &FlowOutcome{Updated: map[string]CurrentValue{}, Events: append([]FlowEvent(nil), sd.state.LastEvents...)},
				Duplicate: true,
			})
			continue
		}
		outcome, err := rt.persistBatch(tenant, messageID, id, sd, grouped[id], receivedAt)
		if err != nil {
			return nil, err
		}
		outcomes = append(outcomes, BatchOutcome{ThingID: id, Outcome: outcome})
	}
	return outcomes, nil
}

// maxBatchAttempts gränsar CAS-omtag vid samtidig skrivning (annan instans
// eller tråd utanför rt.mu). Backoff med jitter mellan försöken gör
// svält praktiskt omöjlig; tar försöken slut felar rundan för
// återleverans via broker-retry (inget tillstånd har gått förlorat:
// omberäkning är deterministisk och alla skrivningar idempotenta).
const maxBatchAttempts = 10

// persistBatch beräknar en saks omgång och persisterar atomärt per försök:
// historik/outbox-skrivningar är idempotenta, sak-blobben skyddas av CAS på
// revision. Vid CAS-konflikt läses färskt tillstånd om och rundan räknas om
// (deterministisk: samma resultat givet samma indata) innan nytt försök.
func (rt *Runtime) persistBatch(tenant, messageID, id string, sd *storedDecoded, obs []things.Observation, receivedAt time.Time) (*FlowOutcome, error) {
	var outcome *FlowOutcome
	for attempt := range maxBatchAttempts {
		var err error
		outcome, err = EvaluateBatchFlow(sd.config, sd.state.Current, sd.state.ProcessorStates, obs, receivedAt, messageID, sd.state.Revision)
		if err != nil {
			return nil, fmt.Errorf("runtime: thing %q: %w", id, err)
		}
		for k, v := range outcome.Updated {
			if sd.state.Current == nil {
				sd.state.Current = make(map[string]CurrentValue)
			}
			sd.state.Current[k] = v
		}
		for k, v := range outcome.ProcStates {
			if sd.state.ProcessorStates == nil {
				sd.state.ProcessorStates = make(map[string]StoredProcessor)
			}
			// Djupkopiera tillståndsblobben: outcome/store/anropare får ej
			// dela StateJSON-underliggande array (ingen tillstånds-aliasing).
			v.StateJSON = cloneBytes(v.StateJSON)
			sd.state.ProcessorStates[k] = v
		}
		if len(outcome.Events) > 0 {
			sd.state.LastEvents = append([]FlowEvent(nil), outcome.Events...)
		} else {
			sd.state.LastEvents = nil
		}
		sd.state.LastMessageID = messageID
		fence := sd.state.DataRevision
		sd.state.DataRevision++
		if rt.store.CommitBatch(tenant, id, fence, sd.state, outcome.History, outcome.Events) {
			rt.store.DedupMark(tenant, messageID, id)
			return outcome, nil
		}
		// CAS-konflikt: inget från försöket blev synligt (CommitBatch är
		// atomär). Backoff före omtag så att tävlande skribenter sprids;
		// första omtaget sker direkt (oftast transient), sedan växande.
		if attempt+1 < maxBatchAttempts {
			time.Sleep(time.Duration(attempt*attempt)*time.Millisecond +
				time.Duration(rand.IntN(1000))*time.Microsecond)
		}
		fresh, ok := rt.store.LoadThing(tenant, id)
		if !ok {
			return nil, fmt.Errorf("%w: thing %q deleted concurrently", things.ErrConflict, id)
		}
		freshCfg, err := decodeConfig(fresh.ConfigJSON)
		if err != nil {
			return nil, fmt.Errorf("runtime: thing %q has undecodable config: %w", id, err)
		}
		sd.state = fresh
		sd.config = freshCfg
	}
	return nil, fmt.Errorf("%w: thing %q busy, retry delivery", things.ErrConflict, id)
}

type storedDecoded struct {
	state  StoredThingState
	config *catalog.EffectiveConfig
}

func decodeConfig(raw []byte) (*catalog.EffectiveConfig, error) {
	var cfg catalog.EffectiveConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// DeleteThing tar bort en sak. Med aktiva barn blockeras borttagningen
// (ingen kaskad, B17); sakens egna relationslänkar kopplas loss och
// värdehistorik bevaras i storet.
func (rt *Runtime) DeleteThing(tenant, thingID string) error {
	if tenant == "" || thingID == "" {
		return fmt.Errorf("%w: tenant and thing ID must be provided", things.ErrValidation)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.store.LoadThing(tenant, thingID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	if children := rt.store.Children(tenant, thingID); len(children) > 0 {
		return fmt.Errorf("%w: thing %q has %d active children", things.ErrConflict, thingID, len(children))
	}
	rt.store.DetachChild(tenant, thingID)
	rt.store.DeleteThingState(tenant, thingID)
	return nil
}

// MoveThing flyttar ett barns partOf-förälder atomärt under låset.
// Barnets ID, bindningar, konfiguration och historik påverkas inte.
func (rt *Runtime) MoveThing(tenant, childID, newParentID string) error {
	if tenant == "" || childID == "" || newParentID == "" {
		return fmt.Errorf("%w: tenant, child and parent must be provided", things.ErrValidation)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.store.LoadThing(tenant, childID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, childID)
	}
	if _, ok := rt.store.LoadThing(tenant, newParentID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, newParentID)
	}
	return rt.store.MoveChild(tenant, childID, newParentID)
}

// StableEventID är det stabila deduplicerings-ID:t per (sak, meddelande,
// egenskap): återleverans ger samma ID så att konsumenter deduplicerar.
func StableEventID(tenant, thingID, messageID, property string) string {
	sanitize := func(s string) string {
		return strings.ReplaceAll(s, "/", "~")
	}
	return "v1/" + sanitize(tenant) + "/" + sanitize(thingID) + "/" + sanitize(messageID) + "/" + sanitize(property)
}

// buildThingValuesEvent mappar en batchs FlowEvents till ett versionerat
// kontraktsevent. Sources (ström-ID:n) slås ihop kommaseparerat: full
// proveniens (antal tillgängliga/förväntade per ingång) stannar i
// historik/outbox-lagren; kontraktsutökning är K1-arbete.
func buildThingValuesEvent(tenant, thingID, thingType, thingTypeVersion string, units map[string]string, events []FlowEvent, revision int64, publishedAt time.Time) pkgtypes.ThingValuesEventV1 {
	values := make([]pkgtypes.ThingValueV1, 0, len(events))
	var messageID string
	for _, e := range events {
		if messageID == "" {
			messageID = e.MessageID
		}
		v := e.Value
		values = append(values, pkgtypes.ThingValueV1{
			Schema:         pkgtypes.SchemaV1,
			ThingID:        thingID,
			Tenant:         tenant,
			PropertyID:     e.PropertyID,
			Value:          &v,
			Quality:        e.Quality,
			Unit:           units[e.PropertyID],
			Source:         strings.Join(e.Sources, ","),
			MessageID:      e.MessageID,
			ObservedAt:     e.ObservedAt,
			ReceivedAt:     e.ReceivedAt,
			ComputedAt:     publishedAt,
			PublishedAt:    publishedAt,
			ConfigRevision: revision,
			EventID:        StableEventID(tenant, thingID, e.MessageID, e.PropertyID),
		})
	}
	return pkgtypes.ThingValuesEventV1{
		Schema: pkgtypes.SchemaV1,
		EventID: "v1/" + strings.ReplaceAll(tenant, "/", "~") + "/" +
			strings.ReplaceAll(thingID, "/", "~") + "/" + strings.ReplaceAll(messageID, "/", "~"),
		Tenant:           tenant,
		ThingID:          thingID,
		ThingType:        thingType,
		ThingTypeVersion: thingTypeVersion,
		ConfigRevision:   revision,
		PublishedAt:      publishedAt,
		Values:           values,
	}
}

// DrainAndPublish levererar väntande händelser för en sak: bygger ett
// kontraktsevent per meddelande-ID (stabilt EventID), publicerar, och
// rensar endast de ID:n som levererats. Fel behåller kön för retry.
// Revision stämplas per batchgrupp från lagrade händelser (låst revision vid
// beräkning), inte från aktuell config (som kan ha uppdaterats efter batch).
func (rt *Runtime) DrainAndPublish(ctx context.Context, pub EventPublisher, tenant, thingID string, now time.Time) (int, error) {
	if tenant == "" || thingID == "" {
		return 0, fmt.Errorf("%w: tenant and thing ID must be provided", things.ErrValidation)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rt.mu.Lock()
	st, ok := rt.store.LoadThing(tenant, thingID)
	pending := rt.store.PendingEvents(tenant, thingID)
	rt.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	if len(pending) == 0 {
		return 0, nil
	}
	cfg, err := decodeConfig(st.ConfigJSON)
	if err != nil {
		return 0, fmt.Errorf("runtime: thing %q has undecodable config: %w", thingID, err)
	}
	units := make(map[string]string, len(cfg.Properties))
	for id, prop := range cfg.Properties {
		units[id] = prop.Unit
	}
	groups := [][]FlowEvent{}
	index := make(map[string]int)
	for _, e := range pending {
		if i, ok := index[e.MessageID]; ok {
			groups[i] = append(groups[i], e)
			continue
		}
		index[e.MessageID] = len(groups)
		groups = append(groups, []FlowEvent{e})
	}
	published := 0
	for _, group := range groups {
		rev := st.Revision
		if len(group) > 0 {
			rev = group[0].ConfigRevision
		}
		ev := buildThingValuesEvent(tenant, thingID, cfg.TemplateID, cfg.TemplateVersion, units, group, rev, now)
		for _, v := range ev.Values {
			if err := v.Validate(); err != nil {
				return published, fmt.Errorf("runtime: invalid value event: %w", err)
			}
		}
		if err := pub.PublishThingValues(ctx, ev); err != nil {
			return published, err
		}
		published++
		// Rensa inkrementellt per lyckad grupp så att partiellt lyckad
		// dränering inte levererar samma grupp dubbelt vid retry (L7).
		var ids []string
		for _, e := range group {
			ids = append(ids, StableEventID(tenant, thingID, e.MessageID, e.PropertyID))
		}
		rt.mu.Lock()
		rt.store.ClearPendingIDs(tenant, thingID, ids)
		rt.mu.Unlock()
	}
	return published, nil
}

// DrainAll publicerar väntande händelser för alla saker i alla tenants
// (uppstartsdränering efter omstart). Returnerar antal levererade batchar.
func (rt *Runtime) DrainAll(ctx context.Context, pub EventPublisher, now time.Time) (int, error) {
	type target struct{ tenant, thing string }
	var targets []target
	rt.mu.Lock()
	for _, tenant := range rt.store.ListTenants() {
		for _, id := range rt.store.ListThingIDs(tenant) {
			if len(rt.store.PendingEvents(tenant, id)) > 0 {
				targets = append(targets, target{tenant, id})
			}
		}
	}
	rt.mu.Unlock()
	total := 0
	for _, t := range targets {
		n, err := rt.DrainAndPublish(ctx, pub, t.tenant, t.thing, now)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
