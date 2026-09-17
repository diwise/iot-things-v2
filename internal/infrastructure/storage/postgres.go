package storage

import (
	"context"
	"embed"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
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
	return context.WithTimeout(context.Background(), queryTimeout)
}

func (s *PgStore) LoadThing(tenant, thingID string) (application.StoredThingState, bool) {
	ctx, cancel := s.ctx()
	defer cancel()
	var dataRev int64
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT revision, data FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID).Scan(&dataRev, &raw)
	if err != nil {
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

func (s *PgStore) InsertThing(tenant, thingID string, st application.StoredThingState) bool {
	ctx, cancel := s.ctx()
	defer cancel()
	raw, _ := json.Marshal(st)
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO things_v2(tenant, id, revision, data) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		tenant, thingID, st.DataRevision, raw)
	return err == nil && tag.RowsAffected() == 1
}

func (s *PgStore) SaveThingCAS(tenant, thingID string, expected int64, st application.StoredThingState) bool {
	ctx, cancel := s.ctx()
	defer cancel()
	raw, _ := json.Marshal(st)
	tag, err := s.pool.Exec(ctx,
		`UPDATE things_v2 SET revision=$1, data=$2, updated_at=now() WHERE tenant=$3 AND id=$4 AND revision=$5`,
		st.DataRevision, raw, tenant, thingID, expected)
	return err == nil && tag.RowsAffected() == 1
}

func (s *PgStore) DeleteThingState(tenant, thingID string) {
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	// Värdehistorik bevaras (B6/H8). Väntande outbox förkastas och deras
	// sedda ID:n glöms (samma semantik som minnets Forget: nyskapad sak med
	// samma ID ska kunna behandla samma messageID på nytt); dedup för
	// saken glöms av samma skäl.
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2`, tenant, thingID); err != nil {
		return
	}
	// Stabila outbox-ID:n har formen v1/<tenant>/<thing>/<msg>/<prop> med
	// "/" -> "~" (se StableEventID): glöm levererade + väntande ID:n för saken.
	// starts_with undviker LIKE-wildcards (%_\) i tenant/thing-ID:n som
	// annars skulle radera andra sakars seen-rader (LIKE 'a_b/%' matchar
	// även 'aXb/...').
	sanitize := func(v string) string {
		out := make([]byte, 0, len(v))
		for i := 0; i < len(v); i++ {
			if v[i] == '/' {
				out = append(out, '~')
			} else {
				out = append(out, v[i])
			}
		}
		return string(out)
	}
	prefix := "v1/" + sanitize(tenant) + "/" + sanitize(thingID) + "/"
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_seen_v2 WHERE starts_with(id, $1)`, prefix); err != nil {
		return
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dedup_v2 WHERE tenant=$1 AND thing_id=$2`, tenant, thingID); err != nil {
		return
	}
	if _, err := tx.Exec(ctx, `DELETE FROM things_v2 WHERE tenant=$1 AND id=$2`, tenant, thingID); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

func (s *PgStore) ListThingIDs(tenant string) []string {
	ctx, cancel := s.ctx()
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

func (s *PgStore) ListTenants() []string {
	ctx, cancel := s.ctx()
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

func (s *PgStore) AppendHistory(tenant, thingID string, h application.HistoryWrite) bool {
	ctx, cancel := s.ctx()
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

func (s *PgStore) History(tenant, thingID, prop string, from, to time.Time, limit, offset int) []application.HistoryWrite {
	ctx, cancel := s.ctx()
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

func (s *PgStore) PendingEvents(tenant, thingID string) []application.FlowEvent {
	ctx, cancel := s.ctx()
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT payload FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 ORDER BY observed_at, property_id, id`,
		tenant, thingID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []application.FlowEvent
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return out
		}
		var fe application.FlowEvent
		if err := json.Unmarshal(raw, &fe); err != nil {
			continue
		}
		out = append(out, fe)
	}
	return out
}

func (s *PgStore) StorePending(tenant, thingID string, events []application.FlowEvent) {
	ctx, cancel := s.ctx()
	defer cancel()
	for _, e := range events {
		raw, _ := json.Marshal(e)
		id := application.StableEventID(tenant, thingID, e.MessageID, e.PropertyID)
		// Atomär seen+pending: exakt en instans vinner kapplöpningen; crash
		// mellan seen och pending är omöjlig (en sats).
		_, _ = s.pool.Exec(ctx, `
			WITH s AS (
				INSERT INTO outbox_seen_v2(id) VALUES($1) ON CONFLICT DO NOTHING RETURNING id
			)
			INSERT INTO outbox_v2(id, tenant, thing_id, property_id, observed_at, revision, payload)
			SELECT $1,$2,$3,$4,$5,$6,$7 WHERE EXISTS (SELECT 1 FROM s)`,
			id, tenant, thingID, e.PropertyID, e.ObservedAt, e.ConfigRevision, raw)
	}
}

func (s *PgStore) ClearPendingIDs(tenant, thingID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM outbox_v2 WHERE tenant=$1 AND thing_id=$2 AND id = ANY($3)`,
		tenant, thingID, ids)
}

// CommitBatch persisterar blob-CAS + historik + pending i en transaktion:
// vid staketkonflikt (0 rader) rollbackas allt och false returneras.
// Historik/outbox-skrivningar är själva idempotenta, så omtag efter
// konflikt konvergerar.
func (s *PgStore) CommitBatch(tenant, thingID string, expected int64, st application.StoredThingState, hist []application.HistoryWrite, pending []application.FlowEvent) bool {
	ctx, cancel := s.ctx()
	defer cancel()
	raw, _ := json.Marshal(st)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`UPDATE things_v2 SET revision=$1, data=$2, updated_at=now() WHERE tenant=$3 AND id=$4 AND revision=$5`,
		st.DataRevision, raw, tenant, thingID, expected)
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
			INSERT INTO outbox_v2(id, tenant, thing_id, property_id, observed_at, revision, payload)
			SELECT $1,$2,$3,$4,$5,$6,$7 WHERE EXISTS (SELECT 1 FROM s)`,
			id, tenant, thingID, e.PropertyID, e.ObservedAt, e.ConfigRevision, payload); err != nil {
			return false
		}
	}
	return tx.Commit(ctx) == nil
}

func (s *PgStore) DedupCheck(tenant, messageID, thingID string) bool {
	if messageID == "" {
		return false
	}
	ctx, cancel := s.ctx()
	defer cancel()
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM dedup_v2 WHERE tenant=$1 AND message_id=$2 AND thing_id=$3`,
		tenant, messageID, thingID).Scan(&one)
	return err == nil
}

func (s *PgStore) DedupMark(tenant, messageID, thingID string) {
	if messageID == "" {
		return
	}
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO dedup_v2(tenant, message_id, thing_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		tenant, messageID, thingID)
}

func (s *PgStore) Children(tenant, parentID string) []string {
	ctx, cancel := s.ctx()
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

func (s *PgStore) ParentOf(tenant, childID string) (string, bool) {
	ctx, cancel := s.ctx()
	defer cancel()
	var parent string
	err := s.pool.QueryRow(ctx,
		`SELECT to_id FROM relations_v2 WHERE tenant=$1 AND from_id=$2 AND type='partOf'`,
		tenant, childID).Scan(&parent)
	return parent, err == nil
}

func (s *PgStore) AttachChild(tenant, childID, parentID string) error {
	if childID == parentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	ctx, cancel := s.ctx()
	defer cancel()
	if cur, ok := s.ParentOf(tenant, childID); ok {
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
		if cur, ok := s.ParentOf(tenant, childID); ok {
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

func (s *PgStore) MoveChild(tenant, childID, newParentID string) error {
	if childID == "" || newParentID == "" {
		return fmt.Errorf("relation: missing child/new parent")
	}
	if childID == newParentID {
		return fmt.Errorf("relation: self-reference denied (%q)", childID)
	}
	ctx, cancel := s.ctx()
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

func (s *PgStore) DetachChild(tenant, childID string) {
	ctx, cancel := s.ctx()
	defer cancel()
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM relations_v2 WHERE tenant=$1 AND from_id=$2`, tenant, childID)
}

// partOfCreatesCycle följer föräldrakedjan från newParentID (rekursiv CTE):
// når den childID skulle kanten skapa en cykel.
func (s *PgStore) partOfCreatesCycle(ctx context.Context, tenant, childID, newParentID string) bool {
	var exists bool
	err := s.pool.QueryRow(ctx, `
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
	st, ok := s.LoadThing(tenant, id)
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
		`INSERT INTO things_v2(tenant, id, revision, data) VALUES($1,$2,$3,$4)
		 ON CONFLICT(tenant, id) DO UPDATE SET revision=EXCLUDED.revision, data=EXCLUDED.data, updated_at=now()`,
		tenant, id, revision, raw)
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
