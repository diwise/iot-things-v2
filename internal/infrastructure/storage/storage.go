package storage

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage mirrors the iot-things storage wiring: one handle passed as both
// reader and writer to the application layer. The v1 implementation is
// memory-backed (see memory.go); the PostgreSQL/TimescaleDB backend is the
// next step (K4). The pgxpool import is real: New validates the connection
// string with pgxpool.ParseConfig so misconfiguration fails fast at startup,
// same failure domain as iot-things storage.New.
type Storage struct {
	Things    *MemThingStore
	History   *MemHistoryStore
	Relations *MemRelationStore
	Outbox    *MemOutbox
	Dedup     *MemDedup

	// mu serialiserar flerlagers-commitar (CommitBatch) mellan adaptrar
	// över samma Storage. Underliggande lager har egna lås i konsistent
	// ordning (Storage.mu -> lagermutex); lagermetoder tar aldrig Storage.mu.
	mu      sync.Mutex
	connStr string
}

func New(_ context.Context, cfg Config) (*Storage, error) {
	connStr := cfg.ConnStr()
	if _, err := pgxpool.ParseConfig(connStr); err != nil {
		return nil, err
	}
	return &Storage{
		Things:    NewMemThingStore(),
		History:   NewMemHistoryStore(),
		Relations: NewMemRelationStore(),
		Outbox:    NewMemOutbox(),
		Dedup:     NewMemDedup(),
		connStr:   connStr,
	}, nil
}

// Close releases resources. Memory-backed: no-op, kept for lifecycle parity
// with iot-things (ownedResources closes storage after messenger).
func (s *Storage) Close() {}

// QueryThing implements application.ThingsReader against the memory thing
// store. PostgreSQL lookup (tenant-filtered SELECT with soft-delete) is the
// next step; the method set is already the seam cmd wires into the app.
func (s *Storage) QueryThing(tenant, id string) ([]byte, int64, bool) {
	return s.Things.Get(tenant, id)
}

// PutThing implements application.ThingsWriter against the memory thing
// store and returns the stored revision.
func (s *Storage) PutThing(tenant, id string, data []byte, revision int64) int64 {
	cp := append([]byte(nil), data...)
	s.Things.Upsert(ThingRecord{ID: id, Tenant: tenant, Revision: revision, Data: cp})
	if _, rev, ok := s.Things.Get(tenant, id); ok {
		return rev
	}
	return revision
}
