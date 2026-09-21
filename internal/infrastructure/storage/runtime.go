// RuntimeAdapter implementerar application.RuntimeStore över minneslagren.
// All state serialiseras till JSON-blobar i MemThingStore så att samma
// metoduppsättning senare kan mappas mot PostgreSQL-rader utan att
// applikationslagret ändras (en Runtime-batch = en transaktion).
package storage

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

// RuntimeAdapter är trådsäker via de underliggande lagrens egna lås plus
// Runtime-låset för batch-atomicitet.
type RuntimeAdapter struct {
	S *Storage
}

// NewRuntimeAdapter lindar ett Storage (skapat via New) som RuntimeStore.
func NewRuntimeAdapter(s *Storage) *RuntimeAdapter {
	return &RuntimeAdapter{S: s}
}

func (a *RuntimeAdapter) LoadThing(tenant, thingID string) (application.StoredThingState, bool) {
	raw, dataRev, ok := a.S.Things.Get(tenant, thingID)
	if !ok {
		return application.StoredThingState{}, false
	}
	var st application.StoredThingState
	if err := json.Unmarshal(raw, &st); err != nil {
		return application.StoredThingState{}, false
	}
	// Kolumnen är skrivstaketet (auktoritativ); konfigurationsrevisionen
	// kommer ur JSON-blobben.
	st.DataRevision = dataRev
	return st, true
}

func (a *RuntimeAdapter) InsertThing(tenant, thingID string, st application.StoredThingState) bool {
	raw, _ := json.Marshal(st)
	return a.S.Things.Insert(ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: raw})
}

func (a *RuntimeAdapter) SaveThingCAS(tenant, thingID string, expected int64, st application.StoredThingState) bool {
	raw, _ := json.Marshal(st)
	return a.S.Things.CompareAndSwap(tenant, thingID, expected, ThingRecord{ID: thingID, Tenant: tenant, Revision: st.DataRevision, Data: raw})
}

// CommitBatch persisterar blob-CAS + historik + pending + dedupmarkering
// atomärt under Storage-låset: vid staketkonflikt blir inget synligt och
// inget markerat. Historik/outbox är själva idempotenta, så omtag efter
// konflikt konvergerar.
func (a *RuntimeAdapter) CommitBatch(tenant, thingID, messageID string, expected int64, st application.StoredThingState, hist []application.HistoryWrite, pending []application.FlowEvent) bool {
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
			Source:     h.Source,
			MessageID:  h.MessageID,
			Late:       h.Late,
		})
	}
	for _, e := range pending {
		payload, _ := json.Marshal(e)
		a.S.Outbox.Append(OutboxEvent{
			ID:         application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID),
			Tenant:     tenant,
			ThingID:    thingID,
			PropertyID: e.PropertyID,
			ObservedAt: e.ObservedAt,
			Revision:   e.ConfigRevision,
			Payload:    payload,
			Topic:      pkgtypes.TopicThingValues,
		})
	}
	a.S.Dedup.Mark(tenant, messageID, thingID)
	return true
}

func (a *RuntimeAdapter) DeleteThingState(tenant, thingID string) error {
	a.S.Things.Delete(tenant, thingID)
	// Värdehistorik bevaras (B6/H8), men föräldralös outbox och dedup för
	// saken rensas så att dränering inte missar (ListTenants utgår från
	// Things) och nyskapad sak med samma ID inte blockeras av gammal livscykel.
	// Minnesoperationer felar inte; nil bevarar gränssnittets felkontrakt.
	a.S.Outbox.Forget(tenant, thingID)
	a.S.Dedup.ForgetThing(tenant, thingID)
	return nil
}

// PurgeThingHistory raderar sakens värdehistorik, väntande outbox samt
// dedup/sedda ID:n (K6 GDPR-radering). Fungerar för levande och borttagna
// saker; idempotent. Returnerar antal raderade historikrader.
func (a *RuntimeAdapter) PurgeThingHistory(tenant, thingID string) (int64, error) {
	n := int64(a.S.History.PurgeThing(tenant, thingID))
	a.S.Outbox.Forget(tenant, thingID)
	a.S.Dedup.ForgetThing(tenant, thingID)
	return n, nil
}

// PurgeHistoryOlderThan raderar historikrader äldre än cutoff i alla tenants
// (K4 retention). Väntande outbox, dedup/sedda ID:n, blobbar, katalog och
// livscykel-/relationshändelser påverkas inte. Returnerar antal raderade rader.
func (a *RuntimeAdapter) PurgeHistoryOlderThan(cutoff time.Time) (int64, error) {
	return int64(a.S.History.PurgeOlderThan(cutoff)), nil
}

func (a *RuntimeAdapter) ListThingIDs(tenant string) []string {
	recs := a.S.Things.ListByTenant(tenant)
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.ID)
	}
	return out
}

func (a *RuntimeAdapter) ListTenants() []string {
	return a.S.Things.Tenants()
}

func (a *RuntimeAdapter) AppendHistory(tenant, thingID string, h application.HistoryWrite) bool {
	return a.S.History.AppendIdempotent(HistoryRecord{
		Tenant:     tenant,
		ThingID:    thingID,
		PropertyID: h.PropertyID,
		ObservedAt: h.ObservedAt,
		ReceivedAt: h.ReceivedAt,
		Value:      h.Value,
		Quality:    h.Quality,
		Source:     h.Source,
		MessageID:  h.MessageID,
		Late:       h.Late,
	})
}

func (a *RuntimeAdapter) History(tenant, thingID, prop string, from, to time.Time, limit, offset int) []application.HistoryWrite {
	recs := a.S.History.List(tenant, thingID, prop, from, to, limit, offset)
	out := make([]application.HistoryWrite, 0, len(recs))
	for _, r := range recs {
		out = append(out, application.HistoryWrite{
			PropertyID: r.PropertyID,
			Value:      r.Value,
			Quality:    r.Quality,
			ObservedAt: r.ObservedAt,
			ReceivedAt: r.ReceivedAt,
			Source:     r.Source,
			MessageID:  r.MessageID,
			Late:       r.Late,
		})
	}
	return out
}

func (a *RuntimeAdapter) PendingEvents(tenant, thingID string) []application.FlowEvent {
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
		out = append(out, fe)
	}
	return out
}

func (a *RuntimeAdapter) StorePending(tenant, thingID string, events []application.FlowEvent) {
	for _, e := range events {
		raw, _ := json.Marshal(e)
		a.S.Outbox.Append(OutboxEvent{
			ID:         application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID),
			Tenant:     tenant,
			ThingID:    thingID,
			PropertyID: e.PropertyID,
			ObservedAt: e.ObservedAt,
			Revision:   e.ConfigRevision,
			Payload:    raw,
			Topic:      pkgtypes.TopicThingValues,
		})
	}
}

func (a *RuntimeAdapter) ClearPendingIDs(tenant, thingID string, ids []string) {
	a.S.Outbox.ClearIDs(tenant, thingID, ids)
}

// StoreRawEvents persists lifecycle/relation events with ID dedup.
func (a *RuntimeAdapter) StoreRawEvents(tenant, thingID string, events []application.RawOutboxEvent) {
	for _, e := range events {
		a.S.Outbox.Append(OutboxEvent{
			ID:         e.ID,
			Tenant:     tenant,
			ThingID:    thingID,
			ObservedAt: e.ObservedAt,
			Revision:   e.Revision,
			Payload:    e.Payload,
			Topic:      e.Topic,
		})
	}
}

// PendingRawEvents returns pending non-values outbox entries (lifecycle,
// relations) with their topics.
func (a *RuntimeAdapter) PendingRawEvents(tenant, thingID string) []application.RawOutboxEvent {
	evs := a.S.Outbox.PeekFor(tenant, thingID)
	var out []application.RawOutboxEvent
	for _, ev := range evs {
		if ev.Topic == "" || ev.Topic == pkgtypes.TopicThingValues {
			continue
		}
		out = append(out, application.RawOutboxEvent{
			ID:         ev.ID,
			Topic:      ev.Topic,
			Payload:    append([]byte(nil), ev.Payload...),
			ObservedAt: ev.ObservedAt,
			Revision:   ev.Revision,
		})
	}
	return out
}

// ClearRawIDs removes delivered raw entries by ID.
func (a *RuntimeAdapter) ClearRawIDs(tenant, thingID string, ids []string) {
	a.S.Outbox.ClearIDs(tenant, thingID, ids)
}

// ListRawTargets lists tenant/things with pending non-values entries,
// including deleted things (values are gone with the blob, lifecycle
// events still have to drain).
func (a *RuntimeAdapter) ListRawTargets() []application.RawTarget {
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
func (a *RuntimeAdapter) DedupCheck(tenant, messageID, thingID string) bool {
	return a.S.Dedup.Has(tenant, messageID, thingID)
}

// DedupMark markerar (tenant, messageID, thingID) som behandlad.
func (a *RuntimeAdapter) DedupMark(tenant, messageID, thingID string) {
	a.S.Dedup.Mark(tenant, messageID, thingID)
}

func (a *RuntimeAdapter) Children(tenant, parentID string) []string {
	recs := a.S.Relations.Children(tenant, parentID)
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.FromID)
	}
	sort.Strings(out)
	return out
}

func (a *RuntimeAdapter) ParentOf(tenant, childID string) (string, bool) {
	rec, ok := a.S.Relations.Parent(tenant, childID)
	if !ok {
		return "", false
	}
	return rec.ToID, true
}

func (a *RuntimeAdapter) AttachChild(tenant, childID, parentID string) error {
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

func (a *RuntimeAdapter) MoveChild(tenant, childID, newParentID string) error {
	return a.S.Relations.Move(tenant, childID, newParentID)
}

func (a *RuntimeAdapter) DetachChild(tenant, childID string) {
	a.S.Relations.RemoveOutgoing(tenant, childID)
}
