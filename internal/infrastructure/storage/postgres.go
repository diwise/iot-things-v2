package storage

import (
	"context"
	"embed"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// queryTimeout gränsar varje enskild databasoperation. Tvärsnittsatomicitet
// bygger på idempotens + CAS (se Runtime.persistBatch), inte på långa
// transaktioner, så korta timeouter är korrekta här.
const queryTimeout = 5 * time.Second

// PgStore implementerar application.RuntimeStore mot PostgreSQL
// (Timescale-hypertabell för historik när tillägget finns, annars vanliga
// tabeller; se migrations/001_init.sql). Varje metod är atomär i sig;
// protokollets garantier kommer från idempotenta skrivningar +
// revisions-CAS + deterministisk omberäkning, verifierat av samma
// protokolltester som minnesbackendens adapter.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPostgres ansluter, verifierar nåbarhet (Ping) och applicerar
// migrationer idempotent under ett advisory-lås (säkert vid samtidig
// uppstart av flera instanser).
func NewPostgres(ctx context.Context, cfg Config) (*PgStore, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnStr())
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(timeoutCtx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &PgStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close stänger poolen.
func (s *PgStore) Close() {
	s.pool.Close()
}

func (s *PgStore) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// Serialisera samtidiga migreringar mellan instanser.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(8104519270735101)`); err != nil {
		return err
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock(8104519270735101)`)
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("storage: migration %s: %w", name, err)
		}
	}
	return nil
}

func (s *PgStore) ctx() (context.Context, context.CancelFunc) {
	return s.ctxWith(context.Background())
}

// ctxWith härleder query-timeout från anroparens context (TV-07): tracing
// och cancellation propagerar genom lagringsgränssnitten i stället för att
// kapas av context.Background().
func (s *PgStore) ctxWith(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, queryTimeout)
}

func (s *PgStore) LoadThing(ctx context.Context, tenant, thingID string) (application.StoredThingState, bool) {
	st, err := s.LoadThingErr(ctx, tenant, thingID)
	if err != nil {
		return application.StoredThingState{}, false
	}
	return st, true
}

// LoadThingErr är felklassificerande variant av LoadThing (TV-05): saknad
// rad ger things.ErrNotFound, SQL-fel och korrupt blob ger andra fel så att
// anroparen kan skilja saknat mål (target:null) från trasig lagring.
func (s *PgStore) LoadThingErr(ctx context.Context, tenant, thingID string) (application.StoredThingState, error) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	var dataRev int64
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT revision, data FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID).Scan(&dataRev, &raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return application.StoredThingState{}, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
		}
		return application.StoredThingState{}, err
	}
	var st application.StoredThingState
	if err := json.Unmarshal(raw, &st); err != nil {
		return application.StoredThingState{}, fmt.Errorf("thing %q has undecodable data: %w", thingID, err)
	}
	// Kolumnen är skrivstaketet (auktoritativ); konfigurationsrevisionen
	// kommer ur JSON-blobben (config_revision-kolumnen är en frågebar kopia).
	st.DataRevision = dataRev
	return st, nil
}

func (s *PgStore) InsertThing(ctx context.Context, tenant, thingID string, st application.StoredThingState) bool {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	raw, _ := json.Marshal(st)
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO things_v2(tenant, id, revision, config_revision, data) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
		tenant, thingID, st.DataRevision, st.Revision, raw)
	return err == nil && tag.RowsAffected() == 1
}

func (s *PgStore) SaveThingCAS(ctx context.Context, tenant, thingID string, expected int64, st application.StoredThingState) bool {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	raw, _ := json.Marshal(st)
	tag, err := s.pool.Exec(ctx,
		`UPDATE things_v2 SET revision=$1, config_revision=$2, data=$3, updated_at=now() WHERE tenant=$4 AND id=$5 AND revision=$6`,
		st.DataRevision, st.Revision, raw, tenant, thingID, expected)
	return err == nil && tag.RowsAffected() == 1
}

func (s *PgStore) DeleteThingState(ctx context.Context, tenant, thingID string) error {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Värdehistorik bevaras (B6/H8). Väntande outbox förkastas och deras
	// sedda ID:n glöms (samma semantik som minnets Forget: nyskapad sak med
	// samma ID ska kunna behandla samma messageID på nytt); dedup för
	// saken glöms av samma skäl.
	if err := s.deleteThingTransient(ctx, tx, tenant, thingID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// seenKey versionerar outbox_seen-nycklar så att tvetydiga legacy-rader
// (lagrade med den äldre "/"->"~"-kodningen) aldrig kan ge falsk dedup
// för nya skrivningar. outbox_v2-raderna behåller wire-EventID:t; endast
// seen-tabellen nycklas om. Disjunkta per konstruktion: legacy-rader
// börjar alltid med "v1/", nya nycklar alltid med "s2:".
func seenKey(id string) string {
	return "s2:" + id
}

// encodeTrace serialiserar W3C trace-carrier för outbox (TV-07, separat
// från domänpayload). Tom map ger ” (NULL-tolerant kolumn med default ”).
func encodeTrace(tc map[string]string) string {
	if len(tc) == 0 {
		return ""
	}
	raw, err := json.Marshal(tc)
	if err != nil {
		return ""
	}
	return string(raw)
}

// decodeTrace återställer lagrad trace-carrier. Tom/ogiltig ger nil
// (spårlös händelse, ingen länk).
func decodeTrace(s string) map[string]string {
	if s == "" {
		return nil
	}
	var tc map[string]string
	if err := json.Unmarshal([]byte(s), &tc); err != nil || len(tc) == 0 {
		return nil
	}
	return tc
}

// deleteThingTransient rensar väntande outbox samt sedda ID:n och dedup för
// saken. Delad av DeleteThingState (historik bevaras, B6) och
// PurgeThingHistory (historik raderas, K6 GDPR).
func (s *PgStore) deleteThingTransient(ctx context.Context, tx pgx.Tx, tenant, thingID string) error {
	// Endast värderader (inkl. legacy-rader utan topic): livscykel- och
	// relationsrader måste överleva delete så att konsumenter ser full
	// livscykel även efter omstart (TV-03; samma semantik som fakeStore
	// och Minnes-ForgetValues).
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND (topic=$3 OR topic='')`,
		tenant, thingID, pkgtypes.TopicThingValues); err != nil {
		return err
	}
	// Stabila outbox-ID:n har formen v1/<tenant>/<thing>/<msg>/<prop> (se
	// StableEventID): glöm levererade + väntande ID:n för saken.
	// starts_with undviker LIKE-wildcards (%_\) i tenant/thing-ID:n som
	// annars skulle radera andra sakars seen-rader (LIKE 'a_b/%' matchar
	// även 'aXb/...').
	//
	// outbox_seen_v2 nycklas globalt på ID:t (ingen tenantkolumn), så
	// seen-nycklarna versioneras med "s2:"-prefix: nya skrivningar kan
	// aldrig matcha tvetydiga legacy-rader och orsaka cross-tenant
	// falsk dedup. Legacy-prefixet rensar redan lagrade ID:n; det är
	// bara exakt när tenant+sak saknar "~" (då är det bevisbart samma
	// tenant — annars skulle det kunna matcha nya ID:n för en
	// kolliderande tenant som "a/b" vs "a~b").
	prefix := seenKey("v1/" + application.EncodeEventSegment(tenant) + "/" + application.EncodeEventSegment(thingID) + "/")
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_seen_v2 WHERE starts_with(id, $1)`, prefix); err != nil {
		return err
	}
	if !strings.ContainsAny(tenant+thingID, "~") {
		legacy := "v1/" + application.LegacyEncodeEventSegment(tenant) + "/" + application.LegacyEncodeEventSegment(thingID) + "/"
		if _, err := tx.Exec(ctx, `DELETE FROM outbox_seen_v2 WHERE starts_with(id, $1)`, legacy); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dedup_v2 WHERE tenant=$1 AND thing_id=$2`, tenant, thingID); err != nil {
		return err
	}
	return nil
}

// PurgeThingHistory raderar sakens värdehistorik och väntande
// värdesevent (K6 GDPR-radering). Livscykel-/relationshändelser,
// dedup-markörer och sedda ID:n lämnas orörda: levande saker behåller
// sitt leveransskydd så att återleverans inte omberäknas (TV-02).
// Fungerar för levande och borttagna saker; idempotent. Returnerar antal
// raderade historikrader, fel åtskilt från 0 (TV-01).
// Sakblobb och katalog påverkas inte: fullständig radering = DELETE sak
// + DELETE historik. Punkt-i-tid: samtidiga batchar kan tillföra nyare
// rader efter raderingen.
func (s *PgStore) PurgeThingHistory(ctx context.Context, tenant, thingID string) (int64, error) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `DELETE FROM history_v2 WHERE tenant=$1 AND thing_id=$2`, tenant, thingID)
	if err != nil {
		return 0, err
	}
	// Endast värderader (inkl. legacy-rader utan topic, som
	// värdesdräneringen också konsumerar); livscykel/relationer stannar
	// så att konsumenter inte tappar registerhändelser.
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND (topic=$3 OR topic='')`,
		tenant, thingID, pkgtypes.TopicThingValues); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// PurgeHistoryOlderThan raderar historikrader äldre än cutoff i alla tenants
// (K4 retention). Väntande outbox, dedup/sedda ID:n, blobbar, katalog och
// livscykel-/relationshändelser påverkas inte. På Timescale-hypertabell
// gäller chunk-exkludering; på vanliga tabeller används indexet från
// migration 006. Returnerar antal raderade rader.
func (s *PgStore) PurgeHistoryOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM history_v2 WHERE observed_at < $1`, cutoff)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *PgStore) ListThingIDs(ctx context.Context, tenant string) []string {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id FROM things_v2 WHERE tenant=$1 ORDER BY id`, tenant)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return out
		}
		out = append(out, id)
	}
	return out
}

func (s *PgStore) ListTenants(ctx context.Context) []string {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT tenant FROM things_v2 ORDER BY tenant`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return out
		}
		out = append(out, tenant)
	}
	return out
}

func (s *PgStore) AppendHistory(ctx context.Context, tenant, thingID string, h application.HistoryWrite) bool {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	// Normalisera till mikrosekund (samma som minnet) så att dedupnyckeln
	// blir identisk mellan backendarna; databasen avrundar ändå.
	h.ObservedAt = canonicalObserved(h.ObservedAt)
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO history_v2(tenant, thing_id, property_id, observed_at, received_at, value, quality, source, message_id, late)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`,
		tenant, thingID, h.PropertyID, h.ObservedAt, orNow(h.ReceivedAt),
		nullableFloat(h.Value), h.Quality, h.Source, h.MessageID, h.Late)
	return err == nil && tag.RowsAffected() == 1
}

func (s *PgStore) History(ctx context.Context, tenant, thingID, prop string, from, to time.Time, limit, offset int) []application.HistoryWrite {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	query := `SELECT property_id, observed_at, received_at, value, quality, source, message_id, late
		FROM history_v2 WHERE tenant=$1 AND thing_id=$2`
	args := []any{tenant, thingID}
	if prop != "" {
		query += fmt.Sprintf(` AND property_id=$%d`, len(args)+1)
		args = append(args, prop)
	}
	if !from.IsZero() {
		query += fmt.Sprintf(` AND observed_at >= $%d`, len(args)+1)
		args = append(args, from)
	}
	if !to.IsZero() {
		query += fmt.Sprintf(` AND observed_at <= $%d`, len(args)+1)
		args = append(args, to)
	}
	query += ` ORDER BY observed_at, received_at, source`
	if limit > 0 {
		query += fmt.Sprintf(` LIMIT %d`, limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(` OFFSET %d`, offset)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []application.HistoryWrite
	for rows.Next() {
		var h application.HistoryWrite
		var value *float64
		if err := rows.Scan(&h.PropertyID, &h.ObservedAt, &h.ReceivedAt, &value, &h.Quality, &h.Source, &h.MessageID, &h.Late); err != nil {
			return out
		}
		h.Value = value
		out = append(out, h)
	}
	return out
}

func (s *PgStore) PendingEvents(ctx context.Context, tenant, thingID string) []application.FlowEvent {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT payload, trace_context FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND topic=$3 ORDER BY observed_at, property_id, id`,
		tenant, thingID, pkgtypes.TopicThingValues)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []application.FlowEvent
	for rows.Next() {
		var raw []byte
		var tcStr string
		if err := rows.Scan(&raw, &tcStr); err != nil {
			return out
		}
		var fe application.FlowEvent
		if err := json.Unmarshal(raw, &fe); err != nil {
			continue
		}
		fe.TraceContext = decodeTrace(tcStr)
		out = append(out, fe)
	}
	return out
}

func (s *PgStore) StorePending(ctx context.Context, tenant, thingID string, events []application.FlowEvent) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	for _, e := range events {
		raw, _ := json.Marshal(e)
		id := application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID)
		// Atomär seen+pending: exakt en instans vinner kapplöpningen; crash
		// mellan seen och pending är omöjlig (en sats).
		// Seen-nyckeln versioneras (seenKey): tvetydiga legacy-rader från
		// före injektiv kodning kan aldrig ge falsk dedup för nya
		// skrivningar; outbox-radens id är fortsatt wire-EventID:t.
		_, _ = s.pool.Exec(ctx, `
			WITH s AS (
				INSERT INTO outbox_seen_v2(id) VALUES($1) ON CONFLICT DO NOTHING RETURNING id
			)
			INSERT INTO outbox_v2(id, tenant, thing_id, property_id, observed_at, revision, payload, topic, trace_context)
			SELECT $2,$3,$4,$5,$6,$7,$8,$9,$10 WHERE EXISTS (SELECT 1 FROM s)`,
			seenKey(id), id, tenant, thingID, e.PropertyID, e.ObservedAt, e.ConfigRevision, raw, pkgtypes.TopicThingValues, encodeTrace(e.TraceContext))
	}
}

func (s *PgStore) ClearPendingIDs(ctx context.Context, tenant, thingID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND id = ANY($3)`,
		tenant, thingID, ids)
}

// StoreRawEvents persists lifecycle/relation events with ID dedup (same
// seen-table as values; IDs are namespaced per topic).
func (s *PgStore) StoreRawEvents(ctx context.Context, tenant, thingID string, events []application.RawOutboxEvent) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	for _, e := range events {
		if e.ID == "" {
			continue
		}
		_, _ = s.pool.Exec(ctx, `
			WITH s AS (
				INSERT INTO outbox_seen_v2(id) VALUES($1) ON CONFLICT DO NOTHING RETURNING id
			)
			INSERT INTO outbox_v2(id, tenant, thing_id, observed_at, revision, payload, topic, trace_context)
			SELECT $2,$3,$4,$5,$6,$7,$8,$9 WHERE EXISTS (SELECT 1 FROM s)`,
			seenKey(e.ID), e.ID, tenant, thingID, e.ObservedAt, e.Revision, e.Payload, e.Topic, encodeTrace(e.TraceContext))
	}
}

// PendingRawEvents returns pending non-values outbox entries with topics.
func (s *PgStore) PendingRawEvents(ctx context.Context, tenant, thingID string) []application.RawOutboxEvent {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT id, topic, payload, observed_at, revision, trace_context FROM outbox_v2
		 WHERE tenant=$1 AND thing_id=$2 AND topic <> $3 ORDER BY observed_at, id`,
		tenant, thingID, pkgtypes.TopicThingValues)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []application.RawOutboxEvent
	for rows.Next() {
		var e application.RawOutboxEvent
		var tcStr string
		if err := rows.Scan(&e.ID, &e.Topic, &e.Payload, &e.ObservedAt, &e.Revision, &tcStr); err != nil {
			return out
		}
		e.TraceContext = decodeTrace(tcStr)
		out = append(out, e)
	}
	return out
}

// ClearRawIDs removes delivered raw entries by ID.
func (s *PgStore) ClearRawIDs(ctx context.Context, tenant, thingID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND id = ANY($3)`,
		tenant, thingID, ids)
}

// ListRawTargets lists tenant/things with pending non-values entries,
// including deleted things (their values are gone, lifecycle events
// still have to drain).
func (s *PgStore) ListRawTargets(ctx context.Context) []application.RawTarget {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT tenant, thing_id FROM outbox_v2 WHERE topic <> $1 ORDER BY tenant, thing_id`,
		pkgtypes.TopicThingValues)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []application.RawTarget
	for rows.Next() {
		var t application.RawTarget
		if err := rows.Scan(&t.Tenant, &t.ThingID); err != nil {
			return out
		}
		out = append(out, t)
	}
	return out
}

// CommitBatch persisterar blob-CAS + historik + pending i en transaktion:
// vid staketkonflikt (0 rader) rollbackas allt och false returneras.
// Historik/outbox-skrivningar är själva idempotenta, så omtag efter
// konflikt konvergerar.
func (s *PgStore) CommitBatch(ctx context.Context, tenant, thingID, messageID string, expected int64, st application.StoredThingState, hist []application.HistoryWrite, pending []application.FlowEvent) bool {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	raw, _ := json.Marshal(st)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`UPDATE things_v2 SET revision=$1, config_revision=$2, data=$3, updated_at=now() WHERE tenant=$4 AND id=$5 AND revision=$6`,
		st.DataRevision, st.Revision, raw, tenant, thingID, expected)
	if err != nil || tag.RowsAffected() != 1 {
		return false
	}
	for _, h := range hist {
		h.ObservedAt = canonicalObserved(h.ObservedAt)
		if _, err := tx.Exec(ctx,
			`INSERT INTO history_v2(tenant, thing_id, property_id, observed_at, received_at, value, quality, source, message_id, late)
			 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`,
			tenant, thingID, h.PropertyID, h.ObservedAt, orNow(h.ReceivedAt),
			nullableFloat(h.Value), h.Quality, h.Source, h.MessageID, h.Late); err != nil {
			return false
		}
	}
	for _, e := range pending {
		payload, _ := json.Marshal(e)
		id := application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID)
		if _, err := tx.Exec(ctx, `
			WITH s AS (
				INSERT INTO outbox_seen_v2(id) VALUES($1) ON CONFLICT DO NOTHING RETURNING id
			)
			INSERT INTO outbox_v2(id, tenant, thing_id, property_id, observed_at, revision, payload, trace_context)
			SELECT $2,$3,$4,$5,$6,$7,$8,$9 WHERE EXISTS (SELECT 1 FROM s)`,
			seenKey(id), id, tenant, thingID, e.PropertyID, e.ObservedAt, e.ConfigRevision, payload, encodeTrace(e.TraceContext)); err != nil {
			return false
		}
	}
	if messageID != "" {
		if _, err := tx.Exec(ctx,
			`INSERT INTO dedup_v2(tenant, message_id, thing_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
			tenant, messageID, thingID); err != nil {
			return false
		}
	}
	return tx.Commit(ctx) == nil
}

func (s *PgStore) DedupCheck(ctx context.Context, tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM dedup_v2 WHERE tenant=$1 AND message_id=$2 AND thing_id=$3`,
		tenant, messageID, thingID).Scan(&one)
	return err == nil
}

func (s *PgStore) DedupMark(ctx context.Context, tenant, messageID, thingID string) {
	if messageID == "" {
		return
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO dedup_v2(tenant, message_id, thing_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		tenant, messageID, thingID)
}

func (s *PgStore) Children(ctx context.Context, tenant, parentID string) []string {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT from_id FROM relations_v2 WHERE tenant=$1 AND to_id=$2 ORDER BY from_id`,
		tenant, parentID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return out
		}
		out = append(out, id)
	}
	return out
}

func (s *PgStore) ParentOf(ctx context.Context, tenant, childID string) (string, bool) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	var parent string
	err := s.pool.QueryRow(ctx,
		`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
		tenant, childID).Scan(&parent)
	return parent, err == nil
}

func (s *PgStore) AttachChild(ctx context.Context, tenant, childID, parentID string) error {
	if childID == parentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	if cur, ok := s.ParentOf(ctx, tenant, childID); ok {
		if cur == parentID {
			return nil
		}
		return fmt.Errorf("relation: child %q already has partOf parent %q", childID, cur)
	}
	if s.partOfCreatesCycle(ctx, tenant, childID, parentID) {
		return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", childID, parentID)
	}
	// ON CONFLICT DO NOTHING maskerar kapplöpning (samtidig attach med
	// olika föräldrar delar id "partof-<barn>"): 0 rader betyder att en
	// annan skribent vann — läs om och avgör idempotens/konflikt i stället
	// för att felaktigt returnera nil.
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO relations_v2(tenant, id, type, from_id, to_id) VALUES($1,$2,'partOf',$3,$4) ON CONFLICT DO NOTHING`,
		tenant, "partof-"+childID, childID, parentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if cur, ok := s.ParentOf(ctx, tenant, childID); ok {
			if cur == parentID {
				return nil
			}
			return fmt.Errorf("relation: child %q already has partOf parent %q", childID, cur)
		}
		// Rad försvann mellan INSERT och omläsning (samtidig detach):
		// försök en gång till utan DO NOTHING för tydligt fel.
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO relations_v2(tenant, id, type, from_id, to_id) VALUES($1,$2,'partOf',$3,$4)`,
			tenant, "partof-"+childID, childID, parentID); err != nil {
			return err
		}
	}
	return nil
}

func (s *PgStore) MoveChild(ctx context.Context, tenant, childID, newParentID string) error {
	if childID == "" || newParentID == "" {
		return fmt.Errorf("relation: missing child/new parent")
	}
	if childID == newParentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	var cur string
	err := s.pool.QueryRow(ctx,
		`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
		tenant, childID).Scan(&cur)
	if err != nil {
		return fmt.Errorf("relation: no partOf parent for child %q", childID)
	}
	if cur != newParentID && s.partOfCreatesCycle(ctx, tenant, childID, newParentID) {
		return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", childID, newParentID)
	}
	_, err = s.pool.Exec(ctx,
		`UPDATE relations_v2 SET to_id=$1 WHERE tenant=$2 AND from_id=$3 AND type='partOf'`,
		newParentID, tenant, childID)
	return err
}

func (s *PgStore) DetachChild(ctx context.Context, tenant, childID string) {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM relations_v2 WHERE tenant=$1 AND from_id=$2`, tenant, childID)
}

// rowQuerier abstraherar pool och transaktion för valideringsläsningar som
// måste se egna osparade skrivningar (TV-03).
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// partOfCreatesCycle följer föräldrakedjan från newParentID (rekursiv CTE):
// når den childID skulle kanten skapa en cykel.
func (s *PgStore) partOfCreatesCycle(ctx context.Context, tenant, childID, newParentID string) bool {
	return partOfCreatesCycleQ(ctx, s.pool, tenant, childID, newParentID)
}

func partOfCreatesCycleQ(ctx context.Context, q rowQuerier, tenant, childID, newParentID string) bool {
	var exists bool
	err := q.QueryRow(ctx, `
		WITH RECURSIVE chain(cur) AS (
			SELECT $3::text
			UNION
			SELECT r.to_id FROM relations_v2 r
			JOIN chain c ON r.tenant=$1 AND r.from_id=c.cur AND r.type='partOf'
		)
		SELECT EXISTS(SELECT 1 FROM chain WHERE cur=$2)`,
		tenant, childID, newParentID).Scan(&exists)
	return err == nil && exists
}

// thingExistsQ rapporterar om saken finns (för relationsvalidering i tx).
func thingExistsQ(ctx context.Context, q rowQuerier, tenant, thingID string) bool {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT true FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID).Scan(&exists); err != nil {
		return false
	}
	return exists
}

// attachChildTx är AttachChild-logiken i transaktion (samma fel och
// idempotens, men validering mot tx-snapshot).
func attachChildTx(ctx context.Context, tx pgx.Tx, tenant, childID, parentID string) error {
	if childID == parentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	if !thingExistsQ(ctx, tx, tenant, parentID) {
		return fmt.Errorf("%w: relation target %q", things.ErrNotFound, parentID)
	}
	var cur string
	err := tx.QueryRow(ctx,
		`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
		tenant, childID).Scan(&cur)
	if err == nil {
		if cur == parentID {
			return nil
		}
		return fmt.Errorf("relation: child %q already has partOf parent %q", childID, cur)
	}
	if partOfCreatesCycleQ(ctx, tx, tenant, childID, parentID) {
		return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", childID, parentID)
	}
	tag, err := tx.Exec(ctx,
		`INSERT INTO relations_v2(tenant, id, type, from_id, to_id) VALUES($1,$2,'partOf',$3,$4) ON CONFLICT DO NOTHING`,
		tenant, "partof-"+childID, childID, parentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Kapplöpning inom transaktionen kan inte inträffa (samma tx ser
		// egna skrivningar); 0 rader betyder att raden redan fanns.
		var cur2 string
		if err := tx.QueryRow(ctx,
			`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
			tenant, childID).Scan(&cur2); err != nil {
			return err
		}
		if cur2 == parentID {
			return nil
		}
		return fmt.Errorf("relation: child %q already has partOf parent %q", childID, cur2)
	}
	return nil
}

// moveChildTx är MoveChild-logiken i transaktion.
func moveChildTx(ctx context.Context, tx pgx.Tx, tenant, childID, newParentID string) error {
	if childID == "" || newParentID == "" {
		return fmt.Errorf("relation: missing child/new parent")
	}
	if childID == newParentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	if !thingExistsQ(ctx, tx, tenant, newParentID) {
		return fmt.Errorf("%w: relation target %q", things.ErrNotFound, newParentID)
	}
	var cur string
	err := tx.QueryRow(ctx,
		`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
		tenant, childID).Scan(&cur)
	if err != nil {
		return fmt.Errorf("relation: no partOf parent for child %q", childID)
	}
	if cur != newParentID && partOfCreatesCycleQ(ctx, tx, tenant, childID, newParentID) {
		return fmt.Errorf("relation: partOf cycle denied for child %q parent %q", childID, newParentID)
	}
	_, err = tx.Exec(ctx,
		`UPDATE relations_v2 SET to_id=$1 WHERE tenant=$2 AND from_id=$3 AND type='partOf'`,
		newParentID, tenant, childID)
	return err
}

// detachChildTx tar bort utgående relationer (alla typer, som RemoveOutgoing).
func detachChildTx(ctx context.Context, tx pgx.Tx, tenant, childID string) error {
	_, err := tx.Exec(ctx,
		`DELETE FROM relations_v2 WHERE tenant=$1 AND from_id=$2`, tenant, childID)
	return err
}

// storeRawEventsTx är StoreRawEvents i transaktion (samma CTE-dedup).
func storeRawEventsTx(ctx context.Context, tx pgx.Tx, tenant, thingID string, events []application.RawOutboxEvent) error {
	for _, e := range events {
		if e.ID == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			WITH s AS (
				INSERT INTO outbox_seen_v2(id) VALUES($1) ON CONFLICT DO NOTHING RETURNING id
			)
			INSERT INTO outbox_v2(id, tenant, thing_id, observed_at, revision, payload, topic, trace_context)
			SELECT $2,$3,$4,$5,$6,$7,$8,$9 WHERE EXISTS (SELECT 1 FROM s)`,
			seenKey(e.ID), e.ID, tenant, thingID, e.ObservedAt, e.Revision, e.Payload, e.Topic, encodeTrace(e.TraceContext)); err != nil {
			return err
		}
	}
	return nil
}

// lockTenant serialiserar mutations-transaktioner per tenant mellan
// instanser (TV-03): blob-fence skyddar enskilda rader, men
// relationsinvarianter (en förälder, acyklicitet) behöver ömsesidig
// uteslutning. Xact-scoped: släpps automatiskt vid commit/rollback.
func lockTenant(ctx context.Context, tx pgx.Tx, tenant string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, tenant)
	return err
}

// SaveThingWithRelations persisterar blob (insert vid create, CAS på
// expected annars), validerar+applicerar partOf-länköperationer och lagrar
// råa outbox-events — atomärt med revisionsfence (TV-03). Feltyper matchar
// de granulära metoderna: ErrConflict vid fence-krock, ErrNotFound vid
// saknat relationsmål, övriga relationsfel oförändrade.
func (s *PgStore) SaveThingWithRelations(ctx context.Context, tenant, thingID string, create bool, expected int64, st application.StoredThingState, relOps []application.RelationOp, raw []application.RawOutboxEvent) error {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockTenant(ctx, tx, tenant); err != nil {
		return err
	}
	blob, _ := json.Marshal(st)
	if create {
		tag, err := tx.Exec(ctx,
			`INSERT INTO things_v2(tenant, id, revision, config_revision, data) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
			tenant, thingID, st.DataRevision, st.Revision, blob)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return things.ErrConflict
		}
		// Ingen epok-rensning här: delete lämnar livscykel-/relationsrader
		// för dränering (TV-03) och rensar redan värderader; nya
		// livscykel-ID:n bär kind (created vs deleted) och kolliderar
		// aldrig med föregående epok. Rensning skulle tappa delete-event
		// före dränering.
	} else {
		tag, err := tx.Exec(ctx,
			`UPDATE things_v2 SET revision=$1, config_revision=$2, data=$3, updated_at=now() WHERE tenant=$4 AND id=$5 AND revision=$6`,
			st.DataRevision, st.Revision, blob, tenant, thingID, expected)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return things.ErrConflict
		}
	}
	for _, op := range relOps {
		var err error
		switch op.Op {
		case "attach":
			err = attachChildTx(ctx, tx, tenant, thingID, op.Target)
		case "move":
			err = moveChildTx(ctx, tx, tenant, thingID, op.Target)
		case "detach":
			err = detachChildTx(ctx, tx, tenant, thingID)
		default:
			err = fmt.Errorf("relation: unknown op %q", op.Op)
		}
		if err != nil {
			return err
		}
	}
	if err := storeRawEventsTx(ctx, tx, tenant, thingID, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeleteThingWithRelations kontrollerar barnlöst tillstånd, kopplar loss,
// raderar state och lagrar råa outbox-events — atomärt (TV-03).
// Saknad blob ger ErrNotFound, aktiva barn ErrConflict.
func (s *PgStore) DeleteThingWithRelations(ctx context.Context, tenant, thingID string, raw []application.RawOutboxEvent) error {
	ctx, cancel := s.ctxWith(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockTenant(ctx, tx, tenant); err != nil {
		return err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT true FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID).Scan(&exists); err != nil {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	var nChildren int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM relations_v2 WHERE tenant=$1 AND to_id=$2`, tenant, thingID).Scan(&nChildren); err != nil {
		return err
	}
	if nChildren > 0 {
		return fmt.Errorf("%w: thing %q has %d active children", things.ErrConflict, thingID, nChildren)
	}
	if err := detachChildTx(ctx, tx, tenant, thingID); err != nil {
		return err
	}
	// Samma state-rensning som DeleteThingState (B6: historik bevaras).
	if err := s.deleteThingTransient(ctx, tx, tenant, thingID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID); err != nil {
		return err
	}
	if err := storeRawEventsTx(ctx, tx, tenant, thingID, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func orNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}

func nullableFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

// QueryThing implements application.ThingsReader (legacy facade): thing
// blob + write fence. Primärvägen är RuntimeStore-metoderna ovan.
func (s *PgStore) QueryThing(tenant, id string) ([]byte, int64, bool) {
	st, ok := s.LoadThing(context.Background(), tenant, id)
	if !ok {
		return nil, 0, false
	}
	raw, _ := json.Marshal(st)
	return raw, st.DataRevision, true
}

// PutThing implements application.ThingsWriter (legacy facade, upsert).
func (s *PgStore) PutThing(tenant, id string, data []byte, revision int64) int64 {
	var st application.StoredThingState
	_ = json.Unmarshal(data, &st)
	st.DataRevision = revision
	raw, _ := json.Marshal(st)
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO things_v2(tenant, id, revision, config_revision, data) VALUES($1,$2,$3,$4,$5)
		 ON CONFLICT(tenant, id) DO UPDATE SET revision=EXCLUDED.revision, config_revision=EXCLUDED.config_revision, data=EXCLUDED.data, updated_at=now()`,
		tenant, id, revision, st.Revision, raw)
	return revision
}

// SaveTemplate persists one published template version. An already stored
// version reports ErrConflict (immutable); parallel publishers converge via
// ON CONFLICT DO NOTHING.
func (s *PgStore) SaveTemplate(spec catalog.TemplateSpec) error {
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO templates_v2(id, version, spec) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		spec.Template.ID, spec.Template.Version, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: template %s/%s already published",
			things.ErrConflict, spec.Template.ID, spec.Template.Version)
	}
	return nil
}

// SaveVariant persists one published variant version (same semantics as
// SaveTemplate).
func (s *PgStore) SaveVariant(spec catalog.VariantSpec) error {
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO variants_v2(id, version, spec) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		spec.Variant.ID, spec.Variant.Version, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: variant %s/%s already published",
			things.ErrConflict, spec.Variant.ID, spec.Variant.Version)
	}
	return nil
}

// LoadTemplates returns all persisted template versions for catalog restore.
func (s *PgStore) LoadTemplates() []catalog.TemplateSpec {
	ctx, cancel := s.ctx()
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT spec FROM templates_v2 ORDER BY id, version`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []catalog.TemplateSpec
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return out
		}
		var spec catalog.TemplateSpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			continue
		}
		out = append(out, spec)
	}
	return out
}

// LoadVariants returns all persisted variant versions for catalog restore.
func (s *PgStore) LoadVariants() []catalog.VariantSpec {
	ctx, cancel := s.ctx()
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT spec FROM variants_v2 ORDER BY id, version`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []catalog.VariantSpec
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return out
		}
		var spec catalog.VariantSpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			continue
		}
		out = append(out, spec)
	}
	return out
}
