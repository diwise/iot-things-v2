package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testServer() *Server {
	s := NewServer()
	s.UpsertThing(ThingDTO{ID: "thing-1", Tenant: "t1", Name: "Thing 1"})
	s.UpsertThing(ThingDTO{ID: "thing-1", Tenant: "t2", Name: "Other tenant"})
	return s
}

func doRequest(t *testing.T, mux http.Handler, method, target, tenant, ifMatch string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, target, rdr)
	if tenant != "" {
		req.Header.Set("X-Tenant", tenant)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// P4: PUT with correct If-Match succeeds and bumps the revision.
func TestPutWithCorrectIfMatch(t *testing.T) {
	s := testServer()
	mux := NewMux(s)

	rec := doRequest(t, mux, http.MethodGet, "/api/v1/things/thing-1", "t1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag header")
	}

	var cur ThingDTO
	if err := json.NewDecoder(rec.Body).Decode(&cur); err != nil {
		t.Fatal(err)
	}

	next := cur
	next.Name = "Renamed"
	rec = doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", etag, next)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var updated ThingDTO
	if err := json.NewDecoder(rec.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	if updated.Revision != cur.Revision+1 {
		t.Fatalf("revision = %d, want %d", updated.Revision, cur.Revision+1)
	}
	if got := rec.Header().Get("ETag"); got != FormatETag(updated.Revision) {
		t.Fatalf("ETag = %q, want %q", got, FormatETag(updated.Revision))
	}
}

// P8: PUT with a stale revision returns 409.
func TestPutWithStaleRevisionConflict(t *testing.T) {
	s := testServer()
	mux := NewMux(s)

	body := ThingDTO{ID: "thing-1", Tenant: "t1", Name: "Stale"}
	rec := doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", `"rev-999"`, body)
	if rec.Code != http.StatusConflict {
		t.Fatalf("PUT status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
	}
}

// A11: cross-tenant access returns 404 and never leaks the other tenant's data.
func TestCrossTenantIsolation(t *testing.T) {
	s := NewServer()
	s.UpsertThing(ThingDTO{ID: "secret", Tenant: "t1", Name: "Secret"})
	mux := NewMux(s)

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{"get", http.MethodGet, "/api/v1/things/secret"},
		{"children", http.MethodGet, "/api/v1/things/secret/children"},
		{"history", http.MethodGet, "/api/v1/things/secret/history"},
	} {
		rec := doRequest(t, mux, tc.method, tc.target, "t2", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", tc.name, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "Secret") {
			t.Fatalf("%s: response leaks cross-tenant data: %s", tc.name, rec.Body.String())
		}
	}
}

// A25: ETag format is "rev-<n>" (quoted).
func TestETagFormat(t *testing.T) {
	s := testServer()
	mux := NewMux(s)

	rec := doRequest(t, mux, http.MethodGet, "/api/v1/things/thing-1", "t1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if !strings.HasPrefix(etag, `"rev-`) || !strings.HasSuffix(etag, `"`) {
		t.Fatalf("ETag = %q, want quoted rev-<n>", etag)
	}
	rev, err := ParseETag(etag)
	if err != nil {
		t.Fatalf("ParseETag(%q) failed: %v", etag, err)
	}
	var dto ThingDTO
	if err := json.NewDecoder(rec.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if rev != dto.Revision {
		t.Fatalf("ETag revision = %d, body revision = %d", rev, dto.Revision)
	}
	if want := fmt.Sprintf(`"rev-%d"`, dto.Revision); etag != want {
		t.Fatalf("ETag = %q, want %q", etag, want)
	}
}

// Children are filtered per tenant: only partOf relations in the requesting
// tenant whose ToID is the parent are returned.
func TestChildrenFilteredPerTenant(t *testing.T) {
	s := NewServer()
	s.UpsertThing(ThingDTO{ID: "parent", Tenant: "t1", Name: "Parent"})
	s.UpsertThing(ThingDTO{ID: "child-a", Tenant: "t1", Name: "A"})
	s.UpsertThing(ThingDTO{ID: "child-b", Tenant: "t1", Name: "B"})
	s.UpsertThing(ThingDTO{ID: "parent", Tenant: "t2", Name: "Parent t2"})
	s.UpsertThing(ThingDTO{ID: "child-x", Tenant: "t2", Name: "X"})
	s.AddRelation(map[string]any{"type": "partOf", "fromID": "child-a", "toID": "parent", "tenant": "t1"})
	s.AddRelation(map[string]any{"type": "partOf", "fromID": "child-b", "toID": "parent", "tenant": "t1"})
	s.AddRelation(map[string]any{"type": "locatedIn", "fromID": "child-b", "toID": "parent", "tenant": "t1"})
	s.AddRelation(map[string]any{"type": "partOf", "fromID": "child-x", "toID": "parent", "tenant": "t2"})
	mux := NewMux(s)

	rec := doRequest(t, mux, http.MethodGet, "/api/v1/things/parent/children", "t1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET children status = %d, want 200", rec.Code)
	}
	var children []ThingDTO
	if err := json.NewDecoder(rec.Body).Decode(&children); err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 {
		t.Fatalf("children = %d, want 2 (got %+v)", len(children), children)
	}
	for _, c := range children {
		if c.Tenant != "t1" {
			t.Fatalf("child tenant = %q, want t1", c.Tenant)
		}
		if c.ID != "child-a" && c.ID != "child-b" {
			t.Fatalf("unexpected child %q", c.ID)
		}
	}
}

func TestPutTenantMismatch(t *testing.T) {
	s := testServer()
	mux := NewMux(s)

	body := ThingDTO{ID: "thing-1", Tenant: "other", Name: "Hijack"}
	rec := doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", `"rev-1"`, body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT status = %d, want 400", rec.Code)
	}
}

func TestHistoryLimitAndPropertyFilter(t *testing.T) {
	s := NewServer()
	s.UpsertThing(ThingDTO{ID: "thing-1", Tenant: "t1"})
	s.AppendHistory("t1", "thing-1", map[string]any{"property": "temp", "v": 1})
	s.AppendHistory("t1", "thing-1", map[string]any{"property": "hum", "v": 2})
	s.AppendHistory("t1", "thing-1", map[string]any{"property": "temp", "v": 3})
	mux := NewMux(s)

	rec := doRequest(t, mux, http.MethodGet, "/api/v1/things/thing-1/history?property=temp&limit=1", "t1", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET history status = %d, want 200", rec.Code)
	}
	var entries []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0]["v"] != float64(3) {
		t.Fatalf("entry = %v, want most recent temp reading", entries[0])
	}
}

func TestHealthz(t *testing.T) {
	s := NewServer()
	mux := NewMux(s)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("healthz body = %v, want status ok", body)
	}
}

func TestMissingTenantHeaderIs400(t *testing.T) {
	s := testServer()
	mux := NewMux(s)
	for _, target := range []string{
		"/api/v1/things/thing-1",
		"/api/v1/things/thing-1/history",
		"/api/v1/things/thing-1/children",
	} {
		rec := doRequest(t, mux, http.MethodGet, target, "", "", nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s utan X-Tenant: status = %d, want 400", target, rec.Code)
		}
	}
}

func TestPutRequiresValidIfMatch(t *testing.T) {
	s := testServer()
	mux := NewMux(s)
	body := ThingDTO{ID: "thing-1", Tenant: "t1", Name: "X"}
	rec := doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", "", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT utan If-Match: status = %d, want 400", rec.Code)
	}
	rec = doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", `"bogus"`, body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT med ogiltig If-Match: status = %d, want 400", rec.Code)
	}
}

func TestAddRelationValidation(t *testing.T) {
	s := NewServer()
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "x", "toID": "x", "tenant": "t1"}); err == nil {
		t.Fatal("självreferens ska nekas")
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "child", "toID": "p1", "tenant": "t1"}); err != nil {
		t.Fatalf("första partOf ska gå: %v", err)
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "child", "toID": "p1", "tenant": "t1"}); err != nil {
		t.Fatalf("samma kant ska vara idempotent: %v", err)
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "child", "toID": "p2", "tenant": "t1"}); err == nil {
		t.Fatal("andra partOf-förälder ska nekas")
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "child", "toID": "p2", "tenant": "t2"}); err != nil {
		t.Fatalf("samma barn i annan tenant ska gå: %v", err)
	}
	if err := s.AddRelation(map[string]any{"type": "", "fromID": "a", "toID": "b", "tenant": "t1"}); err == nil {
		t.Fatal("saknad typ ska nekas")
	}
}

// Regression B19: PUT mot okänt thing ska ge 404 (aldrig 409/läckage).
func TestPutMissingThingIs404(t *testing.T) {
	s := NewServer()
	mux := NewMux(s)
	body := ThingDTO{ID: "missing", Tenant: "t1", Name: "X"}
	rec := doRequest(t, mux, http.MethodPut, "/api/v1/things/missing", "t1", `"rev-1"`, body)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("PUT missing: status = %d, want 404 (body %s)", rec.Code, rec.Body.String())
	}
}

// Regression: path-ID mot body-ID ska valideras (400 vid mismatch),
// samma strikthet som tenant-mismatch och iot-things updateHandler.
func TestPutIDMismatchIs400(t *testing.T) {
	s := testServer()
	mux := NewMux(s)
	body := ThingDTO{ID: "other-id", Tenant: "t1", Name: "Hijack"}
	rec := doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", `"rev-1"`, body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT id-mismatch: status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	// Tomt body-ID fylls från path och ska accepteras.
	body = ThingDTO{Tenant: "t1", Name: "NoID"}
	rec = doRequest(t, mux, http.MethodPut, "/api/v1/things/thing-1", "t1", `"rev-1"`, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT utan ID: status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
}

// Regression ETag: svag prefix W/, utan citat och negativa/ogiltiga värden.
func TestParseETagVariants(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{`"rev-1"`, 1},
		{`rev-1`, 1},
		{`W/"rev-2"`, 2},
		{`W/rev-3`, 3},
		{`  "rev-4"  `, 4},
	} {
		got, err := ParseETag(tc.in)
		if err != nil {
			t.Fatalf("ParseETag(%q) fel: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseETag(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{``, `"bogus"`, `"rev--1"`, `"rev-x"`, `"rev-1-extra"`, `W/`, `""`} {
		if _, err := ParseETag(bad); err == nil {
			t.Fatalf("ParseETag(%q) = nil, want fel", bad)
		}
	}
	if _, err := ParseETag(`"rev--5"`); err == nil {
		t.Fatal("negativ revision ska nekas")
	}
	// Format/Parse roundtrip ska bevara revision.
	for _, rev := range []int64{1, 2, 99} {
		back, err := ParseETag(FormatETag(rev))
		if err != nil || back != rev {
			t.Fatalf("roundtrip rev %d = (%d, %v)", rev, back, err)
		}
	}
}

// Regression B17: partOf-cykler ska nekas även i API-scaffolden,
// samma skydd som MemRelationStore (a->c->b->a).
func TestAddRelationCycleDenied(t *testing.T) {
	s := NewServer()
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "b", "toID": "a", "tenant": "t1"}); err != nil {
		t.Fatalf("b->a ska gå: %v", err)
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "c", "toID": "b", "tenant": "t1"}); err != nil {
		t.Fatalf("c->b ska gå: %v", err)
	}
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "a", "toID": "c", "tenant": "t1"}); err == nil {
		t.Fatal("partOf-cykel a->c->b->a ska nekas")
	}
	// Samma kant i annan tenant ska inte påverkas (isolering).
	if err := s.AddRelation(map[string]any{"type": "partOf", "fromID": "a", "toID": "c", "tenant": "t2"}); err != nil {
		t.Fatalf("samma kant i annan tenant ska gå: %v", err)
	}
	// Icke-partOf-cykler är tillåtna (begränsningen gäller partOf-hierarki).
	s2 := NewServer()
	if err := s2.AddRelation(map[string]any{"type": "locatedIn", "fromID": "a", "toID": "b", "tenant": "t1"}); err != nil {
		t.Fatalf("locatedIn a->b ska gå: %v", err)
	}
	if err := s2.AddRelation(map[string]any{"type": "locatedIn", "fromID": "b", "toID": "a", "tenant": "t1"}); err != nil {
		t.Fatalf("locatedIn b->a ska gå (ej partOf-cykel): %v", err)
	}
}
