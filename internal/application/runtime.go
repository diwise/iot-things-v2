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
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracer bär kedjan vidare: HandleBatch anropas med remote-context från
// brokerkonsumtionen (messaging extraherar trace-headers) och publicering
// (DrainAndPublish/DrainRaw) injicerar samma context, så mätvärde ->
// beräkning -> värdesevent hänger ihop i collectorn.
var runtimeTracer = otel.Tracer("iot-things-v2/runtime")

// traceContextFrom fångar transportens trace-context för beständig lagring
// i outbox separat från domänpayloaden (TV-07). Tom map vid spårlös context.
func traceContextFrom(ctx context.Context) map[string]string {
	carrier := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagationMapCarrier(carrier))
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// parentSpanContext återställer lagrad trace-context till en remote
// SpanContext för länkning vid senare leverans (omstart). False vid saknad
// eller ogiltig carrier.
func parentSpanContext(tc map[string]string) (trace.SpanContext, bool) {
	if len(tc) == 0 {
		return trace.SpanContext{}, false
	}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagationMapCarrier(tc))
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return trace.SpanContext{}, false
	}
	return sc, true
}

type propagationMapCarrier map[string]string

func (c propagationMapCarrier) Get(key string) string { return c[key] }
func (c propagationMapCarrier) Set(key, value string) { c[key] = value }
func (c propagationMapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// traceLinks samlar unika giltiga föräldra-SpanContexts från en värdesgrupp
// för länkning vid publicering (TV-07, omstart).
func traceLinks(group []FlowEvent) []trace.Link {
	seen := make(map[string]struct{})
	var out []trace.Link
	for _, e := range group {
		sc, ok := parentSpanContext(e.TraceContext)
		if !ok {
			continue
		}
		key := sc.TraceID().String() + "/" + sc.SpanID().String()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trace.Link{SpanContext: sc})
	}
	return out
}

// traceLinkForRaw länkar ett rått lifecycle-/relationsevent till dess ursprung.
func traceLinkForRaw(re RawOutboxEvent) []trace.Link {
	sc, ok := parentSpanContext(re.TraceContext)
	if !ok {
		return nil
	}
	return []trace.Link{{SpanContext: sc}}
}

// StoredThingState är det persisterade tillståndet per sak. Revision är
// konfigurationsversionen (API-ytan, If-Match); DataRevision är
// skrivstaketet för CAS (bumpas vid varje blob-skrivning, även batchar
// utan configändring) så att samtidiga skribenter upptäcks.
type StoredThingState struct {
	// ConfigJSON är den effektiva konfigurationen som läsbar JSON
	// (json.RawMessage bäddas in utan base64).
	ConfigJSON      json.RawMessage            `json:"config"`
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
	// Alla metoder tar ctx först (TV-07): tracing och cancellation
	// propagerar genom lagringsgränssnitten i stället för att kapas av
	// context.Background(). Minnesimplementationen ignorerar ctx (ingen
	// I/O), PostgreSQL härleder query-timeout från den.
	LoadThing(ctx context.Context, tenant, thingID string) (StoredThingState, bool)
	// LoadThingErr är felklassificerande variant av LoadThing (TV-05):
	// saknad rad ger things.ErrNotFound, SQL-fel och korrupt blob ger
	// andra fel. Används där feltyp styr svaret (relationsupplösning):
	// endast ErrNotFound får bli target:null.
	LoadThingErr(ctx context.Context, tenant, thingID string) (StoredThingState, error)
	// InsertThing lagrar ny sak; false om den redan finns.
	InsertThing(ctx context.Context, tenant, thingID string, st StoredThingState) bool
	// SaveThingCAS lagrar endast om lagrat skrivstaket är expected
	// (DataRevision, ej konfigurationsrevisionen).
	SaveThingCAS(ctx context.Context, tenant, thingID string, expected int64, st StoredThingState) bool
	// CommitBatch persisterar en omgång atomärt: blob-CAS på expected
	// staket + idempotent historik + idempotent pending + dedupmarkering
	// i samma atomära enhet. False vid staketkonflikt (inget skrivs,
	// inget markeras); anroparen läser om och försöker igen.
	// Markeringen i samma enhet stänger fönstret mellan commit och mark:
	// en krasch lämnar aldrig state utan dedup (dubbeltillämpning av
	// sum/delta vid återleverans) eller dedup utan state (tappad omgång).
	CommitBatch(ctx context.Context, tenant, thingID, messageID string, expected int64, st StoredThingState, hist []HistoryWrite, pending []FlowEvent) bool
	DeleteThingState(ctx context.Context, tenant, thingID string) error
	// PurgeThingHistory raderar sakens värdehistorik, väntande outbox samt
	// dedup/sedda ID:n (K6 GDPR-radering). Fungerar för levande och
	// borttagna saker; idempotent. Returnerar antal raderade historikrader.
	// Fel propageras åtskilt från "0 rader" (TV-01).
	PurgeThingHistory(ctx context.Context, tenant, thingID string) (int64, error)
	// PurgeHistoryOlderThan raderar historikrader äldre än cutoff i alla
	// tenants (K4 retention). Returnerar antal raderade rader; fel
	// propageras åtskilt från "0 rader" (TV-01).
	PurgeHistoryOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
	ListThingIDs(ctx context.Context, tenant string) []string
	ListTenants(ctx context.Context) []string
	AppendHistory(ctx context.Context, tenant, thingID string, h HistoryWrite) bool
	// History läser egenskapshistorik äldst först. Noll-tid betyder ingen
	// gräns; limit > 0 kapar, offset > 0 hoppar över de äldsta raderna.
	History(ctx context.Context, tenant, thingID, prop string, from, to time.Time, limit, offset int) []HistoryWrite
	PendingEvents(ctx context.Context, tenant, thingID string) []FlowEvent
	StorePending(ctx context.Context, tenant, thingID string, events []FlowEvent)
	ClearPendingIDs(ctx context.Context, tenant, thingID string, ids []string)
	// Raw outbox for thing lifecycle and relation events (other topics
	// than property values, same durability). IDs are namespaced per
	// topic so values and lifecycle never collide.
	StoreRawEvents(ctx context.Context, tenant, thingID string, events []RawOutboxEvent)
	PendingRawEvents(ctx context.Context, tenant, thingID string) []RawOutboxEvent
	ClearRawIDs(ctx context.Context, tenant, thingID string, ids []string)
	// ListRawTargets lists tenant/things with pending non-values events,
	// including deleted things (their values are gone but lifecycle
	// events must still drain). Used by restart drain.
	ListRawTargets(ctx context.Context) []RawTarget
	DedupCheck(ctx context.Context, tenant, messageID, thingID string) bool
	DedupMark(ctx context.Context, tenant, messageID, thingID string)
	Children(ctx context.Context, tenant, parentID string) []string
	ParentOf(ctx context.Context, tenant, childID string) (string, bool)
	AttachChild(ctx context.Context, tenant, childID, parentID string) error
	MoveChild(ctx context.Context, tenant, childID, newParentID string) error
	DetachChild(ctx context.Context, tenant, childID string)
	// SaveThingWithRelations persists blob (insert when create, CAS on
	// expected data revision otherwise), validates+applies partOf
	// relation ops and stores raw outbox events — atomically, fenced on
	// revision (TV-03). Relation validation errors and ErrConflict pass
	// through with the same values as the granular methods.
	SaveThingWithRelations(ctx context.Context, tenant, thingID string, create bool, expected int64, st StoredThingState, relOps []RelationOp, raw []RawOutboxEvent) error
	// DeleteThingWithRelations checks no active children, detaches,
	// deletes state and stores raw outbox events — atomically (TV-03).
	// Missing blob gives ErrNotFound, children give ErrConflict.
	DeleteThingWithRelations(ctx context.Context, tenant, thingID string, raw []RawOutboxEvent) error
}

// RawOutboxEvent is one durable non-values outbox entry (lifecycle or
// relation change): topic routes, ID dedups, Payload is the event JSON.
// TraceContext bär transportens W3C trace-context separat från
// domänpayloaden (TV-07): vid omstart återställs/länkas den vid leverans
// så att kedjan till ursprungsobservationen bevaras. Tom vid händelser
// utan incoming trace (t.ex. API-registrering utan spans).
type RawOutboxEvent struct {
	ID           string
	Topic        string
	Payload      []byte
	ObservedAt   time.Time
	Revision     int64
	TraceContext map[string]string
}

// RawTarget identifies a thing with pending raw outbox events.
type RawTarget struct {
	Tenant  string
	ThingID string
}

// EventPublisher levererar konvergerade utbox-batchar. Produktions-
// implementation: TopicPublisher nedan (messaging-golang). Tester använder
// en fake.
type EventPublisher interface {
	PublishThingValues(ctx context.Context, ev pkgtypes.ThingValuesEventV1) error
	PublishMessage(ctx context.Context, msg messaging.TopicMessage) error
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

// RelationOp is one hierarchy mutation applied atomically with a blob
// save. Only partOf is persisted (other named slots stay views derived
// at read); attach/move carry the parent, detach clears it.
type RelationOp struct {
	Op     string // "attach", "move" or "detach"
	Target string // parent for attach/move; "" for detach
}

// RegisterThing kompilerar obj via cat och lagrar konfiguration + revision.
// Aktuellt tillstånd, historik och dedupmarkörer bevaras vid omregistrering.
// Gammal revision vid uppdatering nekas med ErrConflict (optimistisk
// låsning, B19): anroparen ska läsa aktuell revision och försöka igen.
// Vid lyckad uppdatering inkrementeras revisionen (If-Match-semantik);
// nyskapad sak får den begärda revisionen (kräver revision > 0).
func (rt *Runtime) RegisterThing(ctx context.Context, tenant string, cat *catalog.Catalog, obj catalog.ObjectSpec, revision int64) (*catalog.EffectiveConfig, error) {
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
	st, exists := rt.store.LoadThing(ctx, tenant, obj.ThingID)
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
	// Relationsplanering (endast läsning här): validera mål och beräkna
	// planerade länkändringar. Lagringen återvaliderar atomärt före
	// skrivning, så samtidiga ändringar inte kan smita emellan (TV-03).
	var relOps []RelationOp
	hasPartOf := false
	for _, rel := range cfg.Relations {
		// Hierarkimedlemskap (partOf) persisteras som barn->förälder;
		// övriga namngivna platser är vyer som härleds vid läsning.
		if rel.Name != "partOf" {
			continue
		}
		hasPartOf = true
		if _, ok := rt.store.LoadThing(ctx, tenant, rel.TargetThingID); !ok {
			return nil, fmt.Errorf("%w: relation target %q", things.ErrNotFound, rel.TargetThingID)
		}
		if cur, ok := rt.store.ParentOf(ctx, tenant, obj.ThingID); ok {
			if cur == rel.TargetThingID {
				continue
			}
			relOps = append(relOps, RelationOp{Op: "move", Target: rel.TargetThingID})
			continue
		}
		relOps = append(relOps, RelationOp{Op: "attach", Target: rel.TargetThingID})
	}
	// detachedFrom bär den borttagna förälderns ID till removed-händelsen
	// (ToID = sista kända mål enligt kontraktet). Op:et i sig förblir
	// mållöst — lagringen tar bort utgående länk oavsett mål.
	var detachedFrom string
	if !hasPartOf {
		// Borttagen partOf i ny config ska koppla loss (lämna ej kvar gammal länk).
		if old, ok := rt.store.ParentOf(ctx, tenant, obj.ThingID); ok {
			relOps = append(relOps, RelationOp{Op: "detach"})
			detachedFrom = old
		}
	}
	st.Revision = newRevision
	st.ConfigJSON = raw
	if st.Current == nil {
		st.Current = make(map[string]CurrentValue)
	}
	// K6: borttagna mätpunkter lämnar inga spöken i blobben: aktuella värden
	// för egenskaper utanför nya configen samt processortillstånd för
	// borttagna receptutgångar rensas vid omregistrering (historik bevaras).
	// Återinförd egenskap börjar fräscht i stället för att återuppliva
	// gammalt tillstånd.
	if exists {
		pruneRemovedState(cfg, &st)
	}
	// Livscykel- och relationshändelser byggs här men persisteras i samma
	// atomära lagringsoperation som blob + länkar (P10/P11, TV-03):
	// konsumenter utan API-åtkomst hålls synkade inklusive borttag.
	now := time.Now().UTC()
	kind := pkgtypes.ThingCreated
	if exists {
		kind = pkgtypes.ThingUpdated
	}
	rawEvents := []RawOutboxEvent{buildLifecycleEvent(tenant, cfg, kind, newRevision, now)}
	for _, op := range relOps {
		target := op.Target
		if op.Op == "detach" {
			target = detachedFrom
		}
		var parentCfg *catalog.EffectiveConfig
		if target != "" {
			var err error
			parentCfg, err = rt.loadParentCfg(ctx, tenant, target)
			if err != nil {
				return nil, err
			}
		}
		rawEvents = append(rawEvents, buildRelationEvent(tenant, cfg, parentCfg, obj.ThingID, target, op.Op == "detach", newRevision, now))
	}
	// Ett atomärt anrop: blob-CAS/insert + relationsvalidering med
	// länkändringar + outbox. Misslyckas något (cykel, saknat mål,
	// samtidig skrivning) ändras ingenting.
	if err := rt.store.SaveThingWithRelations(ctx, tenant, obj.ThingID, !exists, fence, st, relOps, rawEvents); err != nil {
		if errors.Is(err, things.ErrConflict) {
			if !exists {
				return nil, fmt.Errorf("%w: thing %q created concurrently",
					things.ErrConflict, obj.ThingID)
			}
			return nil, fmt.Errorf("%w: thing %q changed concurrently",
				things.ErrConflict, obj.ThingID)
		}
		return nil, err
	}
	return cfg, nil
}

// pruneRemovedState rensar aktuella värden och processortillstånd vars
// egenskap/utgång inte längre finns i den kompilerade konfigurationen
// (K6 livscykel för borttagna mätpunkter). Historik påverkas inte.
func pruneRemovedState(cfg *catalog.EffectiveConfig, st *StoredThingState) {
	for id := range st.Current {
		if _, ok := cfg.Properties[id]; !ok {
			delete(st.Current, id)
		}
	}
	outputs := make(map[string]struct{}, len(cfg.Recipes))
	for _, r := range cfg.Recipes {
		for _, o := range r.Outputs {
			outputs[o] = struct{}{}
		}
	}
	for key := range st.ProcessorStates {
		if _, ok := outputs[key]; !ok {
			delete(st.ProcessorStates, key)
		}
	}
}

// EncodeEventSegment gör ett ID-segment brokersäkert för event-ID:n.
// Kodningen är injektiv: först escapes "~" till "~~", sedan "/" till "~".
// Olika (tenant, sak, meddelande, egenskap)-tupler ger därför alltid olika
// ID:n — även mellan tenants som "a/b" och "a~b". Segment utan "~" kodas
// byte-identiskt med den äldre "/"->"~"-regeln, så lagrade/publicerade
// ID:n utan tilde påverkas inte av bytet (TV-04).
func EncodeEventSegment(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~~"), "/", "~")
}

// LegacyEncodeEventSegment är kodningen före injektiviteten (enbart
// "/"->"~"). Den kan kollidera mellan t.ex. tenant "a/b" och "a~b" och
// får inte användas för nya ID:n — endast för rensning av redan lagrade
// ID:n (se deleteThingTransient/Forget).
func LegacyEncodeEventSegment(s string) string {
	return strings.ReplaceAll(s, "/", "~")
}

// sanitizeEventID gör ID-segment broker-säkra (samma regel som StableEventID).
func sanitizeEventID(s string) string {
	return EncodeEventSegment(s)
}

// describeThing bygger sakdeskriptorn för events från effektiv config.
func describeThing(cfg *catalog.EffectiveConfig) *pkgtypes.ThingDescriptorV1 {
	// K1: explicit AlternativeName/SubType vinner; metadata är fallback
	// för saker materialiserade före fälten infördes.
	alt := cfg.AlternativeName
	if alt == "" && cfg.Metadata != nil {
		alt = cfg.Metadata["alternativeName"]
	}
	sub := cfg.SubType
	if sub == "" && cfg.Metadata != nil {
		sub = cfg.Metadata["subType"]
	}
	desc := &pkgtypes.ThingDescriptorV1{
		ThingID:         cfg.ThingID,
		Name:            cfg.Name,
		AlternativeName: alt,
		SubType:         sub,
		Category:        cfg.Category,
		Metadata:        cfg.Metadata,
		VariantID:       cfg.VariantID,
		VariantVersion:  cfg.VariantVersion,
	}
	if cfg.Location != nil {
		desc.Location = &pkgtypes.GeoLocationV1{
			Type:        cfg.Location.Type,
			Coordinates: append([]byte(nil), cfg.Location.Coordinates...),
		}
	}
	return desc
}

// buildLifecycleEvent bygger en livscykelhändelse utan sidoeffekter.
// Anroparen persisterar den atomärt med mutationen. Payloaden marshalas
// direkt så att senare mutationer aldrig läcker in. ID:t bär kind: create
// och delete på samma revision (ingen uppdatering emellan) är olika
// händelser och får inte dedupliceras mot varandra.
func buildLifecycleEvent(tenant string, cfg *catalog.EffectiveConfig, kind pkgtypes.ThingLifecycleKind, revision int64, at time.Time) RawOutboxEvent {
	ev := pkgtypes.ThingLifecycleV1{
		Schema:           pkgtypes.SchemaV1,
		EventID:          "v1/" + sanitizeEventID(tenant) + "/" + sanitizeEventID(cfg.ThingID) + "/lifecycle/" + string(kind) + "/" + strconv.FormatInt(revision, 10),
		Tenant:           tenant,
		ThingID:          cfg.ThingID,
		Kind:             kind,
		ThingType:        cfg.TemplateID,
		ThingTypeVersion: cfg.TemplateVersion,
		ConfigRevision:   revision,
		ChangedAt:        at,
		Thing:            describeThing(cfg),
	}
	body, _ := json.Marshal(ev)
	return RawOutboxEvent{
		ID: ev.EventID, Topic: ev.TopicName(), Payload: body, ObservedAt: at, Revision: revision,
	}
}

// buildRelationEvent bygger en relationsändring för ett barns partOf-länk
// utan sidoeffekter. Anroparen persisterar den atomärt med mutationen.
// Relations-ID:t delas med lagringen ("partof-<barn>"). Event-ID:t bär
// åtgärd: set och removed på samma revision (t.ex. skapa-med-förälder
// följt av delete utan uppdatering emellan) är olika händelser och får
// inte dedupliceras mot varandra.
//
// fromCfg är alltid barnets effektiva config; toCfg är förälderns (nil
// ⇔ okänd föräldersida — undviks i praktiken genom att anroparen läser
// föräldern före bygget). Båda serialiseras via describeThing så att
// konsumenter kan routa och rendera utan kataloguppslag.
func buildRelationEvent(tenant string, fromCfg, toCfg *catalog.EffectiveConfig, childID, parentID string, removed bool, revision int64, at time.Time) RawOutboxEvent {
	action := "set"
	if removed {
		action = "removed"
	}
	ev := pkgtypes.RelationChangeV1{
		RelationID: "partof-" + childID,
		Type:       "partOf",
		FromID:     childID,
		ToID:       parentID,
		Removed:    removed,
		Tenant:     tenant,
		Revision:   revision,
		ChangedAt:  at,
	}
	if fromCfg != nil {
		ev.FromThingType = fromCfg.TemplateID
		ev.From = describeThing(fromCfg)
	}
	if toCfg != nil {
		ev.ToThingType = toCfg.TemplateID
		ev.To = describeThing(toCfg)
	}
	body, _ := json.Marshal(ev)
	return RawOutboxEvent{
		ID:    "v1/" + sanitizeEventID(tenant) + "/" + sanitizeEventID(childID) + "/relations/partof/" + strconv.FormatInt(revision, 10) + "/" + action,
		Topic: ev.TopicName(), Payload: body, ObservedAt: at, Revision: revision,
	}
}

// loadParentCfg läser och avkodar en förälders config för
// relationshändelsers To-descriptor. Saknad eller korrupt förälder ger fel
// (TV-05) — den får aldrig bli en tyst typlös händelse.
func (rt *Runtime) loadParentCfg(ctx context.Context, tenant, parentID string) (*catalog.EffectiveConfig, error) {
	pst, err := rt.store.LoadThingErr(ctx, tenant, parentID)
	if err != nil {
		return nil, err
	}
	return decodeConfig(pst.ConfigJSON)
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
func (rt *Runtime) HandleBatch(ctx context.Context, tenant, messageID string, obs []things.Observation, receivedAt time.Time) (outcomes []BatchOutcome, err error) {
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant must be provided", things.ErrValidation)
	}
	if messageID == "" {
		return nil, fmt.Errorf("%w: messageID must be provided", things.ErrValidation)
	}
	ctx, span := runtimeTracer.Start(ctx, "handle-batch", trace.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("messaging.message_id", messageID),
		attribute.Int("observations", len(obs)),
	))
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	states := make(map[string]*storedDecoded)
	for _, id := range rt.store.ListThingIDs(ctx, tenant) {
		st, ok := rt.store.LoadThing(ctx, tenant, id)
		if !ok {
			continue
		}
		cfg, err := decodeConfig(st.ConfigJSON)
		if err != nil {
			return nil, fmt.Errorf("runtime: thing %q has undecodable config: %w", id, err)
		}
		states[id] = &storedDecoded{state: st, config: cfg}
	}

	// Routa: sorterade saks-ID:n ger deterministisk grupperingsordning
	// (flödet sorterar ändå observationer per sak, men stabil inmatning
	// underlättar felsökning och tester).
	ids := make([]string, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	grouped := make(map[string][]things.Observation)
	order := []string{}
	for _, o := range obs {
		for _, id := range ids {
			sd := states[id]
			if len(matchingInputs(sd.config.Bindings, o.Signal)) == 0 {
				continue
			}
			if _, ok := grouped[id]; !ok {
				order = append(order, id)
			}
			grouped[id] = append(grouped[id], o)
		}
	}

	outcomes = make([]BatchOutcome, 0, len(order))
	for _, id := range order {
		sd := states[id]
		// Atomär dedup (L6/L10): check-only före beräkning; markeringen sker
		// atomärt i CommitBatch, så varken fel/omstart före commit (dedup
		// utan state) eller krasch efter commit (state utan dedup) ger
		// divergerande tillstånd.
		if rt.store.DedupCheck(ctx, tenant, messageID, id) {
			outcomes = append(outcomes, BatchOutcome{
				ThingID:   id,
				Outcome:   &FlowOutcome{Updated: map[string]CurrentValue{}, Events: append([]FlowEvent(nil), sd.state.LastEvents...)},
				Duplicate: true,
			})
			continue
		}
		outcome, perr := rt.persistBatch(ctx, tenant, messageID, id, sd, grouped[id], receivedAt)
		if perr != nil {
			err = perr
			return nil, err
		}
		outcomes = append(outcomes, BatchOutcome{ThingID: id, Outcome: outcome})
	}
	span.SetAttributes(attribute.Int("things", len(outcomes)))
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
func (rt *Runtime) persistBatch(ctx context.Context, tenant, messageID, id string, sd *storedDecoded, obs []things.Observation, receivedAt time.Time) (outcome *FlowOutcome, err error) {
	ctx, span := runtimeTracer.Start(ctx, "evaluate-thing", trace.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("thing.id", id),
		attribute.String("messaging.message_id", messageID),
		attribute.Int("observations", len(obs)),
	))
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	for attempt := range maxBatchAttempts {
		evalOut, evalErr := EvaluateBatchFlow(sd.config, sd.state.Current, sd.state.ProcessorStates, obs, receivedAt, messageID, sd.state.Revision)
		if evalErr != nil {
			err = fmt.Errorf("runtime: thing %q: %w", id, evalErr)
			return nil, err
		}
		outcome = evalOut
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
		// Spara transportens trace-context separat från domänpayload i
		// outbox (TV-07): omstart dränerar med länk till ursprunget.
		if tc := traceContextFrom(ctx); tc != nil {
			for i := range outcome.Events {
				outcome.Events[i].TraceContext = tc
			}
		}
		if rt.store.CommitBatch(ctx, tenant, id, messageID, fence, sd.state, outcome.History, outcome.Events) {
			span.SetAttributes(
				attribute.Int("events", len(outcome.Events)),
				attribute.Int("history", len(outcome.History)),
			)
			return outcome, nil
		}
		// CAS-konflikt: inget från försöket blev synligt (CommitBatch är
		// atomär). Backoff före omtag så att tävlande skribenter sprids;
		// första omtaget sker direkt (oftast transient), sedan växande.
		if attempt+1 < maxBatchAttempts {
			time.Sleep(time.Duration(attempt*attempt)*time.Millisecond +
				time.Duration(rand.IntN(1000))*time.Microsecond)
		}
		fresh, ok := rt.store.LoadThing(ctx, tenant, id)
		if !ok {
			err = fmt.Errorf("%w: thing %q deleted concurrently", things.ErrConflict, id)
			return nil, err
		}
		if rt.store.DedupCheck(ctx, tenant, messageID, id) {
			// En samtidig skribent slutförde samma batch under vårt omtag:
			// spela upp lagrade händelser i stället för att räkna om.
			// Omberäkning är inte konvergent för tillståndsbärande
			// operatorer (räknare/summor/deltan dubbelräknar).
			outcome = &FlowOutcome{
				Updated: map[string]CurrentValue{},
				Events:  append([]FlowEvent(nil), fresh.LastEvents...),
			}
			return outcome, nil
		}
		freshCfg, ferr := decodeConfig(fresh.ConfigJSON)
		if ferr != nil {
			err = fmt.Errorf("runtime: thing %q has undecodable config: %w", id, ferr)
			return nil, err
		}
		sd.state = fresh
		sd.config = freshCfg
	}
	err = fmt.Errorf("%w: thing %q busy, retry delivery", things.ErrConflict, id)
	return nil, err
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
// värdehistorik bevaras i storet. Livscykel (deleted) och borttagen
// partOf-länk köas för konsumenter före lagringens rensning (som tömmer
// väntande händelser för saken).
func (rt *Runtime) DeleteThing(ctx context.Context, tenant, thingID string) error {
	if tenant == "" || thingID == "" {
		return fmt.Errorf("%w: tenant and thing ID must be provided", things.ErrValidation)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st, ok := rt.store.LoadThing(ctx, tenant, thingID)
	if !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	if children := rt.store.Children(ctx, tenant, thingID); len(children) > 0 {
		return fmt.Errorf("%w: thing %q has %d active children", things.ErrConflict, thingID, len(children))
	}
	// K6 livscykel vid borttagning: blob + aktuella värden försvinner,
	// värdehistorik bevaras (B6), väntande outbox samt dedup/sedda ID:n
	// glöms, utgående relationer tas bort. Inkommande icke-partOf-referenser
	// från andra saker behålls som historiska referenser (läsningar
	// tolererar saknade mål); barn blockerar i stället för kaskad.
	// Explicit GDPR-radering av historik: PurgeThingHistory.
	parent, hasParent := rt.store.ParentOf(ctx, tenant, thingID)
	cfg, err := decodeConfig(st.ConfigJSON)
	if err != nil {
		return fmt.Errorf("runtime: thing %q has undecodable config: %w", thingID, err)
	}
	// Ett atomärt anrop: barnkontroll (igen, mot samtidig attach),
	// detach, state-delete och events. Misslyckas något är ingenting
	// ändrat — till skillnad från tidigare uppdelad sekvens (TV-03).
	now := time.Now().UTC()
	raw := []RawOutboxEvent{buildLifecycleEvent(tenant, cfg, pkgtypes.ThingDeleted, st.Revision, now)}
	if hasParent {
		parentCfg, err := rt.loadParentCfg(ctx, tenant, parent)
		if err != nil {
			return err
		}
		raw = append(raw, buildRelationEvent(tenant, cfg, parentCfg, thingID, parent, true, st.Revision, now))
	}
	return rt.store.DeleteThingWithRelations(ctx, tenant, thingID, raw)
}

// MoveThing flyttar ett barns partOf-förälder atomärt under låset.
// Barnets ID, bindningar, konfiguration och historik påverkas inte.
func (rt *Runtime) MoveThing(ctx context.Context, tenant, childID, newParentID string) error {
	if tenant == "" || childID == "" || newParentID == "" {
		return fmt.Errorf("%w: tenant, child and parent must be provided", things.ErrValidation)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.store.LoadThing(ctx, tenant, childID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, childID)
	}
	if _, ok := rt.store.LoadThing(ctx, tenant, newParentID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, newParentID)
	}
	return rt.store.MoveChild(ctx, tenant, childID, newParentID)
}

// StableEventID är det stabila deduplicerings-ID:t per (sak, meddelande,
// egenskap): återleverans ger samma ID så att konsumenter deduplicerar.
func StableEventID(tenant, thingID, messageID, property string) string {
	return "v1/" + EncodeEventSegment(tenant) + "/" + EncodeEventSegment(thingID) + "/" + EncodeEventSegment(messageID) + "/" + EncodeEventSegment(property)
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
		EventID: "v1/" + EncodeEventSegment(tenant) + "/" +
			EncodeEventSegment(thingID) + "/" + EncodeEventSegment(messageID),
		Tenant:           tenant,
		ThingID:          thingID,
		ThingType:        thingType,
		ThingTypeVersion: thingTypeVersion,
		ConfigRevision:   revision,
		PublishedAt:      publishedAt,
		Values:           values,
	}
}

// DrainAndPublish levererar väntande händelser för en sak: först råa
// livscykel-/relationshändelser (registret före avläsningarna), sedan ett
// kontraktsevent per meddelande-ID (stabilt EventID), och rensar endast de
// ID:n som levererats. Fel behåller kön för retry.
// Revision stämplas per batchgrupp från lagrade händelser (låst revision vid
// beräkning), inte från aktuell config (som kan ha uppdaterats efter batch).
func (rt *Runtime) DrainAndPublish(ctx context.Context, pub EventPublisher, tenant, thingID string, now time.Time) (published int, err error) {
	if tenant == "" || thingID == "" {
		return 0, fmt.Errorf("%w: tenant and thing ID must be provided", things.ErrValidation)
	}
	ctx, span := runtimeTracer.Start(ctx, "drain-publish", trace.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("thing.id", thingID),
	))
	defer func() {
		span.SetAttributes(attribute.Int("published", published))
		tracing.RecordAnyErrorAndEndSpan(err, span)
	}()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, derr := rt.DrainRaw(ctx, pub, tenant, thingID); derr != nil {
		err = derr
		return 0, err
	}
	rt.mu.Lock()
	st, ok := rt.store.LoadThing(ctx, tenant, thingID)
	pending := rt.store.PendingEvents(ctx, tenant, thingID)
	rt.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	if len(pending) == 0 {
		return 0, nil
	}
	cfg, cerr := decodeConfig(st.ConfigJSON)
	if cerr != nil {
		err = fmt.Errorf("runtime: thing %q has undecodable config: %w", thingID, cerr)
		return 0, err
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
	published = 0
	for _, group := range groups {
		rev := st.Revision
		if len(group) > 0 {
			rev = group[0].ConfigRevision
		}
		ev := buildThingValuesEvent(tenant, thingID, cfg.TemplateID, cfg.TemplateVersion, units, group, rev, now)
		ev.Thing = describeThing(cfg)
		for _, v := range ev.Values {
			if verr := v.Validate(); verr != nil {
				err = fmt.Errorf("runtime: invalid value event: %w", verr)
				return published, err
			}
		}
		// Länka till ursprungsobservationen (TV-07): outbox bevarar
		// trace-context över omstart; dränering publicerar med länk så att
		// kedjan mätvärde→beräkning→event håller även efter omstart.
		publish := func(pctx context.Context) error {
			return pub.PublishThingValues(pctx, ev)
		}
		if links := traceLinks(group); len(links) > 0 {
			pctx, pspan := runtimeTracer.Start(ctx, "publish-values",
				trace.WithLinks(links...),
				trace.WithAttributes(attribute.String("messaging.message_id", group[0].MessageID)))
			perr := publish(pctx)
			tracing.RecordAnyErrorAndEndSpan(perr, pspan)
			if perr != nil {
				err = perr
				return published, err
			}
		} else if perr := publish(ctx); perr != nil {
			err = perr
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
		rt.store.ClearPendingIDs(ctx, tenant, thingID, ids)
		rt.mu.Unlock()
	}
	return published, nil
}

// DrainAll publicerar väntande händelser för alla saker i alla tenants
// (uppstartsdränering efter omstart). Returnerar antal levererade batchar.
func (rt *Runtime) DrainAll(ctx context.Context, pub EventPublisher, now time.Time) (total int, err error) {
	ctx, span := runtimeTracer.Start(ctx, "drain-all")
	defer func() {
		span.SetAttributes(attribute.Int("published", total))
		tracing.RecordAnyErrorAndEndSpan(err, span)
	}()
	type target struct{ tenant, thing string }
	var targets []target
	rt.mu.Lock()
	for _, tenant := range rt.store.ListTenants(ctx) {
		for _, id := range rt.store.ListThingIDs(ctx, tenant) {
			if len(rt.store.PendingEvents(ctx, tenant, id)) > 0 {
				targets = append(targets, target{tenant, id})
			}
		}
	}
	rt.mu.Unlock()
	total = 0
	for _, t := range targets {
		n, derr := rt.DrainAndPublish(ctx, pub, t.tenant, t.thing, now)
		if derr != nil {
			err = derr
			return total, err
		}
		total += n
	}
	// Råa händelser för borttagna saker syns inte i saklistningen;
	// svep dem separat så att deletes levereras efter omstart.
	rt.mu.Lock()
	rawTargets := rt.store.ListRawTargets(ctx)
	rt.mu.Unlock()
	for _, t := range rawTargets {
		if _, rerr := rt.DrainRaw(ctx, pub, t.Tenant, t.ThingID); rerr != nil {
			err = rerr
			return total, err
		}
	}
	return total, nil
}

// DrainRaw levererar väntande livscykel-/relationshändelser för en sak.
// Till skillnad från DrainAndPublish kräver den ingen lagrad sakblob:
// borttagna saker dräneras likadant. Returnerar antal levererade händelser.
func (rt *Runtime) DrainRaw(ctx context.Context, pub EventPublisher, tenant, thingID string) (delivered int, err error) {
	if tenant == "" || thingID == "" {
		return 0, fmt.Errorf("%w: tenant and thing ID must be provided", things.ErrValidation)
	}
	ctx, span := runtimeTracer.Start(ctx, "drain-raw", trace.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("thing.id", thingID),
	))
	defer func() {
		span.SetAttributes(attribute.Int("delivered", delivered))
		tracing.RecordAnyErrorAndEndSpan(err, span)
	}()
	rt.mu.Lock()
	pending := rt.store.PendingRawEvents(ctx, tenant, thingID)
	rt.mu.Unlock()
	delivered = 0
	for _, re := range pending {
		var msg messaging.TopicMessage
		switch re.Topic {
		case pkgtypes.TopicLifecycleChanges:
			var ev pkgtypes.ThingLifecycleV1
			if uerr := json.Unmarshal(re.Payload, &ev); uerr != nil {
				err = fmt.Errorf("runtime: invalid lifecycle event %q: %w", re.ID, uerr)
				return delivered, err
			}
			if verr := ev.Validate(); verr != nil {
				err = fmt.Errorf("runtime: invalid lifecycle event %q: %w", re.ID, verr)
				return delivered, err
			}
			msg = ev
		case pkgtypes.TopicRelationChanges:
			var ev pkgtypes.RelationChangeV1
			if uerr := json.Unmarshal(re.Payload, &ev); uerr != nil {
				err = fmt.Errorf("runtime: invalid relation event %q: %w", re.ID, uerr)
				return delivered, err
			}
			msg = ev
		default:
			// Värdehändelser dräneras via DrainAndPublish; okända topics
			// lämnas för framtida konsumenter utan att blockera kön.
			continue
		}
		// Länka till ursprung vid omstart (TV-07); utan lagrad context
		// publiceras med dräneringscontexten som tidigare.
		publish := func(pctx context.Context) error {
			return pub.PublishMessage(pctx, msg)
		}
		if links := traceLinkForRaw(re); len(links) > 0 {
			pctx, pspan := runtimeTracer.Start(ctx, "publish-raw",
				trace.WithLinks(links...),
				trace.WithAttributes(attribute.String("outbox.id", re.ID)))
			perr := publish(pctx)
			tracing.RecordAnyErrorAndEndSpan(perr, pspan)
			if perr != nil {
				err = perr
				return delivered, err
			}
		} else if perr := publish(ctx); perr != nil {
			err = perr
			return delivered, err
		}
		delivered++
		rt.mu.Lock()
		rt.store.ClearRawIDs(ctx, tenant, thingID, []string{re.ID})
		rt.mu.Unlock()
	}
	return delivered, nil
}
