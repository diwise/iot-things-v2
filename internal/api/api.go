package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// TODO(auth): X-Tenant is a scaffold stand-in. Replace with authenticated
// tenant from OIDC claims + OPA authorization before production use.

// ErrConflict is returned when an optimistic-concurrency revision does not match.
var ErrConflict = errors.New("revision conflict")

// ErrNotFound is returned when a thing does not exist in the requesting tenant.
var ErrNotFound = errors.New("thing not found")

// ThingDTO is the v1 scaffold representation of a thing.
type ThingDTO struct {
	ID              string         `json:"id"`
	Tenant          string         `json:"tenant"`
	Name            string         `json:"name,omitempty"`
	TemplateID      string         `json:"templateId,omitempty"`
	TemplateVersion string         `json:"templateVersion,omitempty"`
	VariantID       string         `json:"variantId,omitempty"`
	VariantVersion  string         `json:"variantVersion,omitempty"`
	Revision        int64          `json:"revision"`
	Properties      map[string]any `json:"properties,omitempty"`
	Bindings        []any          `json:"bindings,omitempty"`
}

// Server is an in-memory thing API without external dependencies.
type Server struct {
	mu        sync.RWMutex
	things    map[string]ThingDTO
	history   map[string][]map[string]any
	relations []map[string]any
}

// NewServer returns an empty Server.
func NewServer() *Server {
	return &Server{
		things:  make(map[string]ThingDTO),
		history: make(map[string][]map[string]any),
	}
}

func thingKey(tenant, id string) string {
	return tenant + "/" + id
}

// FormatETag renders a revision as an HTTP ETag value.
func FormatETag(revision int64) string {
	return fmt.Sprintf(`"rev-%d"`, revision)
}

// ParseETag parses "rev-<n>" with or without quotes (and an optional W/ prefix).
func ParseETag(v string) (int64, error) {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "W/")
	s = strings.Trim(s, `"`)
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "rev-") {
		return 0, fmt.Errorf("invalid ETag %q: want rev-<n>", v)
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(s, "rev-"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid ETag %q: %w", v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid ETag %q: negative revision", v)
	}
	return n, nil
}

// UpsertThing creates revision 1 or increments the stored revision.
// It returns the stored revision.
func (s *Server) UpsertThing(dto ThingDTO) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := thingKey(dto.Tenant, dto.ID)
	if cur, ok := s.things[key]; ok {
		dto.Revision = cur.Revision + 1
	} else {
		dto.Revision = 1
	}
	s.things[key] = dto
	return dto.Revision
}

// GetThing returns the thing plus its ETag. The boolean is false when the
// thing does not exist in the given tenant (never leak cross-tenant data).
func (s *Server) GetThing(tenant, id string) (ThingDTO, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dto, ok := s.things[thingKey(tenant, id)]
	if !ok {
		return ThingDTO{}, "", false
	}
	return dto, FormatETag(dto.Revision), true
}

// UpdateThingWithMatch replaces a thing only when ifMatch equals the stored
// revision. It returns ErrNotFound for unknown things and ErrConflict on a
// stale revision.
func (s *Server) UpdateThingWithMatch(tenant, id string, dto ThingDTO, ifMatch int64) (ThingDTO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := thingKey(tenant, id)
	cur, ok := s.things[key]
	if !ok {
		return ThingDTO{}, ErrNotFound
	}
	if cur.Revision != ifMatch {
		return ThingDTO{}, ErrConflict
	}
	dto.ID = id
	dto.Tenant = tenant
	dto.Revision = cur.Revision + 1
	s.things[key] = dto
	return dto, nil
}

// AddRelation stores a relation entry. Expected keys: "type", "fromID"/"fromId",
// "toID"/"toId", "tenant". It enforces B17 for the in-memory scaffold:
// non-empty tenant/endpoints, denied self-reference and at most one active
// "partOf" parent per (tenant, child). Same child->same parent is idempotent
// (nil without storing a duplicate). Cross-tenant reads stay isolated via
// ListChildren/GetThing tenant filtering; callers must pass the owning tenant.
func (s *Server) AddRelation(rel map[string]any) error {
	typ := relString(rel, "type")
	fromID := relString(rel, "fromID", "fromId")
	toID := relString(rel, "toID", "toId")
	tenant := relString(rel, "tenant")
	if typ == "" {
		return fmt.Errorf("relation: missing type")
	}
	if tenant == "" {
		return fmt.Errorf("relation: missing tenant")
	}
	if fromID == "" || toID == "" {
		return fmt.Errorf("relation: missing fromID/toID")
	}
	if fromID == toID {
		return fmt.Errorf("relation: self-reference denied (%q)", fromID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if typ == "partOf" {
		for _, r := range s.relations {
			if relString(r, "tenant") != tenant {
				continue
			}
			if relString(r, "type") != "partOf" {
				continue
			}
			if relString(r, "fromID", "fromId") != fromID {
				continue
			}
			if relString(r, "toID", "toId") == toID {
				return nil
			}
			return fmt.Errorf("relation: child %q already has partOf parent %q", fromID, relString(r, "toID", "toId"))
		}
	}
	cp := make(map[string]any, len(rel))
	for k, v := range rel {
		cp[k] = v
	}
	s.relations = append(s.relations, cp)
	return nil
}

func relString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if str, ok := v.(string); ok {
				return str
			}
		}
	}
	return ""
}

// ListChildren returns things in tenant that are partOf parentID, i.e. have a
// stored relation with type "partOf" and ToID == parentID.
func (s *Server) ListChildren(tenant, parentID string) []ThingDTO {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []ThingDTO{}
	for _, rel := range s.relations {
		if relString(rel, "tenant") != tenant {
			continue
		}
		if relString(rel, "type") != "partOf" {
			continue
		}
		if relString(rel, "toID", "toId") != parentID {
			continue
		}
		fromID := relString(rel, "fromID", "fromId")
		if fromID == "" {
			continue
		}
		if dto, ok := s.things[thingKey(tenant, fromID)]; ok {
			out = append(out, dto)
		}
	}
	return out
}

// AppendHistory appends a history entry for a thing.
func (s *Server) AppendHistory(tenant, id string, entry map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := thingKey(tenant, id)
	s.history[key] = append(s.history[key], entry)
}

// ListHistory returns history entries, optionally filtered by the "property"
// key, keeping insertion order. limit > 0 returns the most recent limit
// entries; limit <= 0 returns all matches.
func (s *Server) ListHistory(tenant, id, property string, limit int) []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := s.history[thingKey(tenant, id)]
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		if property != "" {
			if p, _ := e["property"].(string); p != property {
				continue
			}
		}
		out = append(out, e)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// Exists reports whether a thing exists in the given tenant.
func (s *Server) Exists(tenant, id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.things[thingKey(tenant, id)]
	return ok
}

// NewMux wires the HTTP routes for the in-memory API.
func NewMux(s *Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant")
		if tenant == "" {
			writeError(w, http.StatusBadRequest, "missing X-Tenant header")
			return
		}
		id := r.PathValue("id")
		dto, etag, ok := s.GetThing(tenant, id)
		if !ok {
			writeError(w, http.StatusNotFound, "thing not found")
			return
		}
		w.Header().Set("ETag", etag)
		writeJSON(w, http.StatusOK, dto)
	})
	mux.HandleFunc("PUT /api/v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant")
		if tenant == "" {
			writeError(w, http.StatusBadRequest, "missing X-Tenant header")
			return
		}
		id := r.PathValue("id")
		var dto ThingDTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if dto.Tenant != "" && dto.Tenant != tenant {
			writeError(w, http.StatusBadRequest, "tenant mismatch")
			return
		}
		raw := r.Header.Get("If-Match")
		if raw == "" {
			writeError(w, http.StatusBadRequest, "missing If-Match header")
			return
		}
		rev, err := ParseETag(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid If-Match header")
			return
		}
		updated, err := s.UpdateThingWithMatch(tenant, id, dto, rev)
		switch {
		case errors.Is(err, ErrNotFound):
			writeError(w, http.StatusNotFound, "thing not found")
			return
		case errors.Is(err, ErrConflict):
			writeError(w, http.StatusConflict, "revision conflict")
			return
		case err != nil:
			writeError(w, http.StatusInternalServerError, "update failed")
			return
		}
		w.Header().Set("ETag", FormatETag(updated.Revision))
		writeJSON(w, http.StatusOK, updated)
	})
	mux.HandleFunc("GET /api/v1/things/{id}/history", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant")
		if tenant == "" {
			writeError(w, http.StatusBadRequest, "missing X-Tenant header")
			return
		}
		id := r.PathValue("id")
		if !s.Exists(tenant, id) {
			writeError(w, http.StatusNotFound, "thing not found")
			return
		}
		limit := 0
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 0 {
				writeError(w, http.StatusBadRequest, "invalid limit")
				return
			}
			limit = n
		}
		property := r.URL.Query().Get("property")
		entries := s.ListHistory(tenant, id, property, limit)
		if entries == nil {
			entries = []map[string]any{}
		}
		writeJSON(w, http.StatusOK, entries)
	})
	mux.HandleFunc("GET /api/v1/things/{id}/children", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant")
		if tenant == "" {
			writeError(w, http.StatusBadRequest, "missing X-Tenant header")
			return
		}
		id := r.PathValue("id")
		if !s.Exists(tenant, id) {
			writeError(w, http.StatusNotFound, "thing not found")
			return
		}
		children := s.ListChildren(tenant, id)
		if children == nil {
			children = []ThingDTO{}
		}
		writeJSON(w, http.StatusOK, children)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}
