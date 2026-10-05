// RuntimeAdapter implementerar application.RuntimeStore över minneslagren.
// All state serialiseras till JSON-blobar i MemThingStore så att samma
// metoduppsättning senare kan mappas mot PostgreSQL-rader utan att
// applikationslagret ändras (en Runtime-batch = en transaktion).
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// targetTemplateLocked läser målets verkliga malltyp i samma tenant.
// Anroparen håller a.S.mu (via SaveThingWithRelations).
func (a *RuntimeAdapter) targetTemplateLocked(tenant, thingID string) (string, error) {
	raw, _, ok := a.S.Things.Get(tenant, thingID)
	if !ok {
		return "", fmt.Errorf("%w: relation target %q", things.ErrNotFound, thingID)
	}
	var st application.StoredThingState
	if err := json.Unmarshal(raw, &st); err != nil {
		return "", fmt.Errorf("relation target %q has undecodable data: %w", thingID, err)
	}
	var cfg catalog.EffectiveConfig
	if err := json.Unmarshal(st.ConfigJSON, &cfg); err != nil {
		return "", fmt.Errorf("relation target %q has undecodable config: %w", thingID, err)
	}
	return cfg.TemplateID, nil
}

// copyTraceContext kopierar W3C trace-carrier (TV-07) för outbox-isolering.
func copyTraceContext(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)
	return cp
}

// RuntimeAdapter är trådsäker via de underliggande lagrens egna lås plus
// Runtime-låset för batch-atomicitet.
type RuntimeAdapter struct {
	S *Storage
}

// NewRuntimeAdapter lindar ett Storage (skapat via New) som RuntimeStore.
func NewRuntimeAdapter(s *Storage) *RuntimeAdapter {
	return &RuntimeAdapter{S: s}
}

func (a *RuntimeAdapter) LoadThing(ctx context.Context, tenant, thingID string) (application.StoredThingState, bool) {
	st, err := a.LoadThingErr(ctx, tenant, thingID)
	if err != nil {
		return application.StoredThingState{}, false
	}
	return st, true
}

// LoadThingErr är felklassificerande variant av LoadThing (TV-05).
func (a *RuntimeAdapter) LoadThingErr(ctx context.Context, tenant, thingID string) (application.StoredThingState, error) {
	raw, dataRev, ok := a.S.Things.Get(tenant, thingID)
	if !ok {
		return application.StoredThingState{}, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	var st application.StoredThingState
	if err := json.Unmarshal(raw, &st); err != nil {
		return application.StoredThingState{}, fmt.Errorf("thing %q has undecodable data: %w", thingID, err)
	}
	// Kolumnen är skrivstaketet (auktoritativ); konfigurationsrevisionen
	// kommer ur JSON-blobben.
	st.DataRevision = dataRev
	return st, nil
}

func (a *RuntimeAdapter) InsertThing(ctx context.Context, tenant, thingID string, st application.StoredThingState) bool {
	raw, _ := json.Marshal(st)
	return a.S.Things.Insert(ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: raw})
}

func (a *RuntimeAdapter) SaveThingCAS(ctx context.Context, tenant, thingID string, expected int64, st application.StoredThingState) bool {
	raw, _ := json.Marshal(st)
	return a.S.Things.CompareAndSwap(tenant, thingID, expected, ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: raw})
}

// CommitBatch persisterar blob-CAS + historik + pending + dedupmarkering
// atomärt under Storage-låset: vid staketkonflikt blir inget synligt och
// inget markerat. Historik/outbox är själva idempotenta, så omtag efter
// konflikt konvergerar.
func (a *RuntimeAdapter) CommitBatch(ctx context.Context, tenant, thingID, messageID string, expected int64, st application.StoredThingState, hist []application.HistoryWrite, pending []application.FlowEvent) bool {
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	raw, _ := json.Marshal(st)
	if !a.S.Things.CompareAndSwap(tenant, thingID, expected, ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: raw}) {
		return false
	}
	for _, h := range hist {
		a.S.History.AppendIdempotent(HistoryRecord{
			Tenant:     tenant,
			ThingID:    thingID,
			PropertyID: h.PropertyID,
			ObservedAt: h.ObservedAt,
			ReceivedAt: h.ReceivedAt,
			Value:      h.Value,
			Quality:    h.Quality,
			Unit:       h.Unit,
			Source:     h.Source,
			MessageID:  h.MessageID,
			Late:       h.Late,
		})
	}
	for _, e := range pending {
		payload, _ := json.Marshal(e)
		a.S.Outbox.Append(OutboxEvent{
			ID:           application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID),
			Tenant:       tenant,
			ThingID:      thingID,
			PropertyID:   e.PropertyID,
			ObservedAt:   e.ObservedAt,
			Revision:     e.ConfigRevision,
			Payload:      payload,
			Topic:        pkgtypes.TopicThingValues,
			TraceContext: copyTraceContext(e.TraceContext),
		})
	}
	a.S.Dedup.Mark(tenant, messageID, thingID)
	return true
}

func (a *RuntimeAdapter) DeleteThingState(ctx context.Context, tenant, thingID string) error {
	a.S.Things.Delete(tenant, thingID)
	// Värdehistorik bevaras (B6/H8), men föräldralös outbox och dedup för
	// saken rensas så att dränering inte missar (ListTenants utgår från
	// Things) och nyskapad sak med samma ID inte blockeras av gammal livscykel.
	// Minnesoperationer felar inte; nil bevarar gränssnittets felkontrakt.
	a.S.Outbox.Forget(tenant, thingID)
	a.S.Dedup.ForgetThing(tenant, thingID)
	return nil
}

// SaveThingWithRelations persisterar blob (insert vid create, CAS på
// expected annars), validerar+applicerar partOf-länköperationer och lagrar
// råa outbox-events — atomärt under Storage-låset (TV-03). Samma felvärden
// som de granulära metoderna: ErrConflict vid fence-krock, ErrNotFound vid
// saknat relationsmål, övriga relationsfel oförändrade.
func (a *RuntimeAdapter) SaveThingWithRelations(ctx context.Context, tenant, thingID string, create bool, expected int64, st application.StoredThingState, relOps []application.RelationOp, raw []application.RawOutboxEvent) error {
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	// Fence först (read-only): under S.mu kan ingen annan S.mu-hållare
	// (CommitBatch/Save/Delete) ändras emellan. Granulära MoveChild
	// serialiseras också via S.mu (se nedan), så fencet håller.
	if create {
		if _, _, ok := a.S.Things.Get(tenant, thingID); ok {
			return things.ErrConflict
		}
	} else {
		_, rev, ok := a.S.Things.Get(tenant, thingID)
		if !ok || rev != expected {
			return things.ErrConflict
		}
	}
	// Relations-ögonblicksbild för rollback: relOps kan fallera efter att
	// en tidigare op muterat (t.ex. två partOf-refar där den andra nekas).
	// Utan snapshot skulle länkar ändras utan blob. Snapshot under
	// Relations-låset (samma paket, ingen datarace).
	a.S.Relations.mu.Lock()
	byIDSnap := make(map[string]RelationRecord, len(a.S.Relations.byID))
	maps.Copy(byIDSnap, a.S.Relations.byID)
	childParentSnap := make(map[string]string, len(a.S.Relations.childParent))
	maps.Copy(childParentSnap, a.S.Relations.childParent)
	a.S.Relations.mu.Unlock()
	restoreRelations := func() {
		a.S.Relations.mu.Lock()
		a.S.Relations.byID = byIDSnap
		a.S.Relations.childParent = childParentSnap
		a.S.Relations.mu.Unlock()
	}
	for _, op := range relOps {
		var err error
		name := op.Name
		if name == "" {
			name = "partOf"
		}
		switch op.Op {
		case "attach":
			err = a.attachChildLocked(tenant, thingID, op.Target, op.DeclaredTargetType)
		case "move":
			err = a.moveChildLocked(tenant, thingID, op.Target, op.DeclaredTargetType)
		case "detach":
			// PartOf-detach tar endast partOf (ej generella platser).
			a.S.Relations.RemovePartOf(tenant, thingID)
		case "addLink":
			err = a.addLinkLocked(tenant, thingID, name, op.Target, op.DeclaredTargetType, op.Max)
		case "removeLink":
			a.S.Relations.RemoveLink(tenant, name, thingID, op.Target)
		default:
			err = fmt.Errorf("relation: unknown op %q", op.Op)
		}
		if err != nil {
			restoreRelations()
			return err
		}
	}
	// Blob efter relationslyckande: vid fence-krock återställs länkarna
	// så att inget ändras (atomärt). Insert/CAS låser Things internt;
	// ordningen S.mu -> Things.mu är samma som CommitBatch.
	rawBlob, _ := json.Marshal(st)
	if create {
		if !a.S.Things.Insert(ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: rawBlob}) {
			restoreRelations()
			return things.ErrConflict
		}
	} else {
		if !a.S.Things.CompareAndSwap(tenant, thingID, expected, ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: rawBlob}) {
			restoreRelations()
			return things.ErrConflict
		}
	}
	for _, e := range raw {
		if e.ID == "" {
			continue
		}
		a.S.Outbox.Append(OutboxEvent{
			ID:           e.ID,
			Tenant:       tenant,
			ThingID:      thingID,
			ObservedAt:   e.ObservedAt,
			Revision:     e.Revision,
			Payload:      e.Payload,
			Topic:        e.Topic,
			TraceContext: copyTraceContext(e.TraceContext),
		})
	}
	return nil
}

// attachChildLocked validerar mål (existens + verklig typ i samma tenant) +
// länkar partOf (samma regler och felmeddelanden som AttachChild).
// Anroparen håller a.S.mu.
func (a *RuntimeAdapter) attachChildLocked(tenant, childID, parentID, declaredType string) error {
	realType, err := a.targetTemplateLocked(tenant, parentID)
	if err != nil {
		return err
	}
	if declaredType != "" && declaredType != realType {
		return fmt.Errorf("%w: relation %q target %q has type %q, declared %q",
			things.ErrValidation, "partOf", parentID, realType, declaredType)
	}
	if cur, ok := a.S.Relations.Parent(tenant, childID); ok && cur.ToID == parentID {
		return nil // Redan kopplad: idempotent.
	}
	return a.S.Relations.Add(RelationRecord{
		ID:     "partof-" + childID,
		Tenant: tenant,
		Type:   "partOf",
		FromID: childID,
		ToID:   parentID,
	})
}

// moveChildLocked validerar mål (existens + verklig typ) + flyttar partOf-länk
// (samma regler och felmeddelanden som MoveChild). Anroparen håller a.S.mu.
func (a *RuntimeAdapter) moveChildLocked(tenant, childID, newParentID, declaredType string) error {
	realType, err := a.targetTemplateLocked(tenant, newParentID)
	if err != nil {
		return err
	}
	if declaredType != "" && declaredType != realType {
		return fmt.Errorf("%w: relation %q target %q has type %q, declared %q",
			things.ErrValidation, "partOf", newParentID, realType, declaredType)
	}
	return a.S.Relations.Move(tenant, childID, newParentID)
}

// addLinkLocked persisterar en generell namngiven plats (set-semantik,
// idempotent upsert, max i tx, ingen cykelgrind). Anroparen håller a.S.mu.
func (a *RuntimeAdapter) addLinkLocked(tenant, fromID, name, toID, declaredType string, max int) error {
	if fromID == "" || name == "" || toID == "" {
		return fmt.Errorf("relation: missing from/name/to for addLink")
	}
	if fromID == toID {
		return fmt.Errorf("relation: self-reference denied (%q)", fromID)
	}
	realType, err := a.targetTemplateLocked(tenant, toID)
	if err != nil {
		return err
	}
	if declaredType != "" && declaredType != realType {
		return fmt.Errorf("%w: relation %q target %q has type %q, declared %q",
			things.ErrValidation, name, toID, realType, declaredType)
	}
	if a.S.Relations.HasLink(tenant, name, fromID, toID) {
		return nil // Idempotent: samma länk redan lagrad.
	}
	if max > 0 && a.S.Relations.CountLinks(tenant, fromID, name)+1 > max {
		return fmt.Errorf("%w: relation %q exceeds max %d", things.ErrValidation, name, max)
	}
	return a.S.Relations.Add(RelationRecord{
		ID:     "rel-" + name + "--" + fromID + "--" + toID,
		Tenant: tenant,
		Type:   name,
		FromID: fromID,
		ToID:   toID,
	})
}

// DeleteThingWithRelations kontrollerar barnlöst tillstånd, kopplar loss,
// raderar state och lagrar råa outbox-events — atomärt under Storage-låset
// (TV-03). Saknad blob ger ErrNotFound, aktiva barn ErrConflict.
func (a *RuntimeAdapter) DeleteThingWithRelations(ctx context.Context, tenant, thingID string, raw []application.RawOutboxEvent) error {
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	if _, _, ok := a.S.Things.Get(tenant, thingID); !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	if children := a.S.Relations.Children(tenant, thingID); len(children) > 0 {
		return fmt.Errorf("%w: thing %q has %d active children", things.ErrConflict, thingID, len(children))
	}
	a.S.Relations.RemoveOutgoing(tenant, thingID)
	a.S.Things.Delete(tenant, thingID)
	a.S.Outbox.Forget(tenant, thingID)
	a.S.Dedup.ForgetThing(tenant, thingID)
	for _, e := range raw {
		if e.ID == "" {
			continue
		}
		a.S.Outbox.Append(OutboxEvent{
			ID:           e.ID,
			Tenant:       tenant,
			ThingID:      thingID,
			ObservedAt:   e.ObservedAt,
			Revision:     e.Revision,
			Payload:      e.Payload,
			Topic:        e.Topic,
			TraceContext: copyTraceContext(e.TraceContext),
		})
	}
	return nil
}

// PurgeThingHistory raderar sakens värdehistorik och väntande värdesevent
// (K6 GDPR-radering). Livscykel-/relationshändelser, dedup-markörer och
// sedda ID:n lämnas orörda: levande saker behåller sitt leveransskydd så
// att återleverans inte omberäknas (TV-02). Fungerar för levande och
// borttagna saker; idempotent. Returnerar antal raderade historikrader.
func (a *RuntimeAdapter) PurgeThingHistory(ctx context.Context, tenant, thingID string) (int64, error) {
	n := int64(a.S.History.PurgeThing(tenant, thingID))
	a.S.Outbox.ForgetValues(tenant, thingID)
	return n, nil
}

// PurgeHistoryOlderThan raderar historikrader äldre än cutoff i alla tenants
// (K4 retention). Väntande outbox, dedup/sedda ID:n, blobbar, katalog och
// livscykel-/relationshändelser påverkas inte. Returnerar antal raderade rader.
func (a *RuntimeAdapter) PurgeHistoryOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return int64(a.S.History.PurgeOlderThan(cutoff)), nil
}

func (a *RuntimeAdapter) ListThingIDs(ctx context.Context, tenant string) []string {
	recs := a.S.Things.ListByTenant(tenant)
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.ID)
	}
	return out
}

func (a *RuntimeAdapter) ListTenants(ctx context.Context) []string {
	return a.S.Things.Tenants()
}

func (a *RuntimeAdapter) AppendHistory(ctx context.Context, tenant, thingID string, h application.HistoryWrite) bool {
	return a.S.History.AppendIdempotent(HistoryRecord{
		Tenant:     tenant,
		ThingID:    thingID,
		PropertyID: h.PropertyID,
		ObservedAt: h.ObservedAt,
		ReceivedAt: h.ReceivedAt,
		Value:      h.Value,
		Quality:    h.Quality,
		Unit:       h.Unit,
		Source:     h.Source,
		MessageID:  h.MessageID,
		Late:       h.Late,
	})
}

func (a *RuntimeAdapter) History(ctx context.Context, tenant, thingID, prop string, from, to time.Time, limit, offset int) []application.HistoryWrite {
	recs := a.S.History.List(tenant, thingID, prop, from, to, limit, offset)
	out := make([]application.HistoryWrite, 0, len(recs))
	for _, r := range recs {
		out = append(out, application.HistoryWrite{
			PropertyID: r.PropertyID,
			Value:      r.Value,
			Quality:    r.Quality,
			Unit:       r.Unit,
			ObservedAt: r.ObservedAt,
			ReceivedAt: r.ReceivedAt,
			Source:     r.Source,
			MessageID:  r.MessageID,
			Late:       r.Late,
		})
	}
	return out
}

func (a *RuntimeAdapter) PendingEvents(ctx context.Context, tenant, thingID string) []application.FlowEvent {
	evs := a.S.Outbox.PeekFor(tenant, thingID)
	out := make([]application.FlowEvent, 0, len(evs))
	for _, ev := range evs {
		// Råa livscykel-/relationsrader delar tabellen; värdesdränering
		// hoppar över dem (de dräneras via PendingRawEvents).
		if ev.Topic != "" && ev.Topic != pkgtypes.TopicThingValues {
			continue
		}
		var fe application.FlowEvent
		if err := json.Unmarshal(ev.Payload, &fe); err != nil {
			continue
		}
		fe.TraceContext = copyTraceContext(ev.TraceContext)
		out = append(out, fe)
	}
	return out
}

func (a *RuntimeAdapter) StorePending(ctx context.Context, tenant, thingID string, events []application.FlowEvent) {
	for _, e := range events {
		raw, _ := json.Marshal(e)
		a.S.Outbox.Append(OutboxEvent{
			ID:           application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID),
			Tenant:       tenant,
			ThingID:      thingID,
			PropertyID:   e.PropertyID,
			ObservedAt:   e.ObservedAt,
			Revision:     e.ConfigRevision,
			Payload:      raw,
			Topic:        pkgtypes.TopicThingValues,
			TraceContext: copyTraceContext(e.TraceContext),
		})
	}
}

func (a *RuntimeAdapter) ClearPendingIDs(ctx context.Context, tenant, thingID string, ids []string) {
	a.S.Outbox.ClearIDs(tenant, thingID, ids)
}

// StoreRawEvents persists lifecycle/relation events with ID dedup.
func (a *RuntimeAdapter) StoreRawEvents(ctx context.Context, tenant, thingID string, events []application.RawOutboxEvent) {
	for _, e := range events {
		a.S.Outbox.Append(OutboxEvent{
			ID:           e.ID,
			Tenant:       tenant,
			ThingID:      thingID,
			ObservedAt:   e.ObservedAt,
			Revision:     e.Revision,
			Payload:      e.Payload,
			Topic:        e.Topic,
			TraceContext: copyTraceContext(e.TraceContext),
		})
	}
}

// PendingRawEvents returns pending non-values outbox entries (lifecycle,
// relations) with their topics.
func (a *RuntimeAdapter) PendingRawEvents(ctx context.Context, tenant, thingID string) []application.RawOutboxEvent {
	evs := a.S.Outbox.PeekFor(tenant, thingID)
	var out []application.RawOutboxEvent
	for _, ev := range evs {
		if ev.Topic == "" || ev.Topic == pkgtypes.TopicThingValues {
			continue
		}
		out = append(out, application.RawOutboxEvent{
			ID:           ev.ID,
			Topic:        ev.Topic,
			Payload:      append([]byte(nil), ev.Payload...),
			ObservedAt:   ev.ObservedAt,
			Revision:     ev.Revision,
			TraceContext: copyTraceContext(ev.TraceContext),
		})
	}
	return out
}

// ClearRawIDs removes delivered raw entries by ID.
func (a *RuntimeAdapter) ClearRawIDs(ctx context.Context, tenant, thingID string, ids []string) {
	a.S.Outbox.ClearIDs(tenant, thingID, ids)
}

// ListRawTargets lists tenant/things with pending non-values entries,
// including deleted things (values are gone with the blob, lifecycle
// events still have to drain).
func (a *RuntimeAdapter) ListRawTargets(ctx context.Context) []application.RawTarget {
	recs := a.S.Outbox.PendingRawTargets()
	out := make([]application.RawTarget, 0, len(recs))
	for _, r := range recs {
		out = append(out, application.RawTarget{Tenant: r[0], ThingID: r[1]})
	}
	return out
}

func (a *RuntimeAdapter) DedupHit(tenant, messageID, thingID string) bool {
	return a.S.Dedup.SeenOrMark(tenant, messageID, thingID)
}

// DedupCheck är check-only (ingen markering) för atomär dedup i Runtime:
// markera först efter lyckad persist via DedupMark.
func (a *RuntimeAdapter) DedupCheck(ctx context.Context, tenant, messageID, thingID string) bool {
	return a.S.Dedup.Has(tenant, messageID, thingID)
}

// DedupMark markerar (tenant, messageID, thingID) som behandlad.
func (a *RuntimeAdapter) DedupMark(ctx context.Context, tenant, messageID, thingID string) {
	a.S.Dedup.Mark(tenant, messageID, thingID)
}

func (a *RuntimeAdapter) Children(ctx context.Context, tenant, parentID string) []string {
	recs := a.S.Relations.Children(tenant, parentID)
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.FromID)
	}
	sort.Strings(out)
	return out
}

func (a *RuntimeAdapter) ParentOf(ctx context.Context, tenant, childID string) (string, bool) {
	rec, ok := a.S.Relations.Parent(tenant, childID)
	if !ok {
		return "", false
	}
	return rec.ToID, true
}

func (a *RuntimeAdapter) AttachChild(ctx context.Context, tenant, childID, parentID string) error {
	// Serialisera mot Save/Delete (TV-03): länkändringar får inte smita
	// emellan blob-fence och relationsapply.
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	if cur, ok := a.S.Relations.Parent(tenant, childID); ok && cur.ToID == parentID {
		return nil // Redan kopplad: idempotent.
	}
	return a.S.Relations.Add(RelationRecord{
		ID:     "partof-" + childID,
		Tenant: tenant,
		Type:   "partOf",
		FromID: childID,
		ToID:   parentID,
	})
}

func (a *RuntimeAdapter) MoveChild(ctx context.Context, tenant, childID, newParentID string) error {
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	return a.S.Relations.Move(tenant, childID, newParentID)
}

func (a *RuntimeAdapter) DetachChild(ctx context.Context, tenant, childID string) {
	a.S.mu.Lock()
	defer a.S.mu.Unlock()
	a.S.Relations.RemoveOutgoing(tenant, childID)
}
