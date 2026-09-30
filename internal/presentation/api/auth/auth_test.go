package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/matryer/is"
)

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

// THINGS-002: errors wrapped with %w must preserve the chain, so
// errors.Is/As keep working across the presentation layer.
func TestNewAuthenticatorReadErrorWrapsCause(t *testing.T) {
	is := is.New(t)

	_, err := NewAuthenticator(context.Background(), errReader{})
	is.True(err != nil)
	is.Equal(err.Error(), "unable to read authz policies: read failed")
	is.True(errors.Unwrap(err) != nil)
	is.Equal(errors.Unwrap(err).Error(), "read failed")
}

const legacyPolicy = `package example.authz
default allow := false
allow := {"tenants": ["t1", "t2"]} if { input.token == "good" }
`

const accessObjectPolicy = `package example.authz
default allow := false
allow := {"tenants": ["t1", "t2"], "access": {
	"t1": ["things.read", "things.create"],
	"t2": ["things.read"],
}} if { input.token == "good" }
`

const denyPolicy = `package example.authz
default allow := false
`

func doAuth(ent Enticator, scopes []Scope, token string) (int, context.Context) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/things", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	h := ent.RequireAccess(scopes...)
	var ctx context.Context
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx = r.Context()
		next.ServeHTTP(w, r)
	})
	h(wrapped).ServeHTTP(rr, req)
	return rr.Code, ctx
}

// Legacy-modellen: giltig token ger begärt scope i policyns tenants.
func TestRequireAccessLegacyTenants(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(legacyPolicy), WithAccessObjectAuthorization(false))
	is.NoErr(err)

	code, ctx := doAuth(ent, []Scope{"things.read"}, "good")
	is.Equal(code, http.StatusTeapot)
	got := GetTenantsWithAllowedScopes(ctx, "things.read")
	sort.Strings(got)
	is.Equal(got, []string{"t1", "t2"})

	// Scope som policyn inte känner begränsar inget i legacy: begärt scope
	// tilldelas i alla tenants (auktorisering sker ej i policyn).
	code, ctx = doAuth(ent, []Scope{"things.delete"}, "good")
	is.Equal(code, http.StatusTeapot)
	got = GetTenantsWithAllowedScopes(ctx, "things.delete")
	sort.Strings(got)
	is.Equal(got, []string{"t1", "t2"})

	// Ogiltig token nekas; saknad Bearer nekas.
	code, _ = doAuth(ent, []Scope{"things.read"}, "bad")
	is.Equal(code, http.StatusUnauthorized)
	code, _ = doAuth(ent, []Scope{"things.read"}, "")
	is.Equal(code, http.StatusUnauthorized)
}

// Access-objekt-modellen: scope filtreras per tenant mot policyns access.
func TestRequireAccessObjectScopes(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(accessObjectPolicy))
	is.NoErr(err)

	code, ctx := doAuth(ent, []Scope{"things.read"}, "good")
	is.Equal(code, http.StatusTeapot)
	got := GetTenantsWithAllowedScopes(ctx, "things.read")
	is.Equal(len(got), 2)

	// create finns bara i t1: t2 filtreras bort.
	code, ctx = doAuth(ent, []Scope{"things.create"}, "good")
	is.Equal(code, http.StatusTeapot)
	is.Equal(GetTenantsWithAllowedScopes(ctx, "things.create"), []string{"t1"})

	// delete finns ingenstans: ingen tenant, 401.
	code, _ = doAuth(ent, []Scope{"things.delete"}, "good")
	is.Equal(code, http.StatusUnauthorized)

	code, _ = doAuth(ent, []Scope{"things.read"}, "bad")
	is.Equal(code, http.StatusUnauthorized)
}

// Ren bool-falsk policy nekar utan result-objekt.
func TestRequireAccessDenyPolicy(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(denyPolicy))
	is.NoErr(err)

	code, _ := doAuth(ent, []Scope{"things.read"}, "good")
	is.Equal(code, http.StatusUnauthorized)
}

// Access-objekt-läge mot legacy-policy (utan access-nyckel) är
// felkonfiguration och ska fela högljutt, inte tyst tillåta.
func TestRequireAccessObjectNeedsAccessPolicy(t *testing.T) {
	is := is.New(t)
	ent, err := NewAuthenticator(context.Background(), strings.NewReader(legacyPolicy), WithAccessObjectAuthorization(true))
	is.NoErr(err)

	code, _ := doAuth(ent, []Scope{"things.read"}, "good")
	is.Equal(code, http.StatusInternalServerError)
}
