// RuntimeAdapter implementerar application.RuntimeStore över minneslagren.
// All state serialiseras till JSON-blobar i MemThingStore så att samma
// metoduppsättning senare kan mappas mot PostgreSQL-rader utan att
// applikationslagret ändras (en Runtime-batch = en transaktion).
package storage

import (
	"encoding/json"
	"sort"

	"github.com/diwise/iot-things-v2/internal/application"
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
	raw, rev, ok := a.S.Things.Get(tenant, thingID)
	if !ok {
		return application.StoredThingState{}, false
	}
	var st application.StoredThingState
	if err := json.Unmarshal(raw, &st); err != nil {
		return application.StoredThingState{}, false
	}
	st.Revision = rev
	return st, true
}

func (a *RuntimeAdapter) SaveThing(tenant, thingID string, st application.StoredThingState) {
	raw, _ := json.Marshal(st)
	a.S.Things.Upsert(ThingRecord{ID: thingID, Tenant: tenant, Revision: st.Revision, Data: raw})
	// MemThingStore.Upsert behåller Revision som den är; verifiera inte här
	// (optimistisk låsning sker i Runtime.RegisterThing före anrop).
}

func (a *RuntimeAdapter) DeleteThingState(tenant, thingID string) {
	a.S.Things.Delete(tenant, thingID)
	// Värdehistorik bevaras (B6/H8), men föräldralös outbox och dedup för
	// saken rensas så att dränering inte missar (ListTenants utgår från
	// Things) och nyskapad sak med samma ID inte blockeras av gammal livscykel.
	a.S.Outbox.Forget(tenant, thingID)
	a.S.Dedup.ForgetThing(tenant, thingID)
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
	})
}

func (a *RuntimeAdapter) PendingEvents(tenant, thingID string) []application.FlowEvent {
	evs := a.S.Outbox.PeekFor(tenant, thingID)
	out := make([]application.FlowEvent, 0, len(evs))
	for _, ev := range evs {
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
		})
	}
}

func (a *RuntimeAdapter) ClearPendingIDs(tenant, thingID string, ids []string) {
	a.S.Outbox.ClearIDs(tenant, thingID, ids)
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
