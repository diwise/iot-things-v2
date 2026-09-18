package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/iot-things-v2/internal/infrastructure/storage"
	"github.com/diwise/iot-things-v2/internal/presentation/api/auth"
)

const (
	v0TempURN = "urn:oma:lwm2m:ext:3303"
	v0DistURN = "urn:oma:lwm2m:ext:3330"
)

func v0ctx(tenant string) context.Context {
	return auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		tenant: {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
	})
}

func v0setup(t *testing.T) (*handler, *application.Manager, *application.Runtime) {
	t.Helper()
	s, err := storage.New(context.Background(), storage.NewConfig("", "", "", "5432", "diwise", "disable"))
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	adapter := storage.NewRuntimeAdapter(s)
	rt := application.NewRuntime(adapter)
	mgr := application.NewManager(catalog.New(), rt, adapter)
	return &handler{mgr: mgr}, mgr, rt
}

func v0loc() *things.Location {
	return &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)}
}

func v0meta() map[string]string {
	return map[string]string{"test": "true"}
}

func v0wastebin(t *testing.T, h *handler) {
	t.Helper()
	ctx := v0ctx("t")
	tmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required: []string{"distance", "level", "fillRate"},
			Category: "wastebin",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
			{Name: "fill-rate", Operator: "fillRate", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"fillRate"},
				Params: []string{"usableHeight"}},
		},
	}
	if rr := doJSON(h.publishTemplate, ctx, "", tmpl, ""); rr.Code != http.StatusCreated {
		t.Fatalf("publish template: %d %s", rr.Code, rr.Body.String())
	}
	variant := catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 1.2},
	}}
	if rr := doJSON(h.publishVariant, ctx, "", variant, ""); rr.Code != http.StatusCreated {
		t.Fatalf("publish variant: %d %s", rr.Code, rr.Body.String())
	}
}

func v0binObject() catalog.ObjectSpec {
	return catalog.ObjectSpec{
		ThingID: "bin-42", Name: "Soptunna 42",
		Location: v0loc(), Metadata: v0meta(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "milesight:214", Object: v0DistURN, Resource: "5700", Input: "distance"},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	}
}

func doJSON(fn func(http.ResponseWriter, *http.Request), ctx context.Context, id string, body any, ifMatch string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(http.MethodPost, "/x", &buf)
	req = req.WithContext(ctx)
	if id != "" {
		req.SetPathValue("id", id)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rr := httptest.NewRecorder()
	fn(rr, req)
	return rr
}

func doGET(fn func(http.ResponseWriter, *http.Request), ctx context.Context, id, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
	req = req.WithContext(ctx)
	if id != "" {
		req.SetPathValue("id", id)
	}
	rr := httptest.NewRecorder()
	fn(rr, req)
	return rr
}

func TestV0CreateGetUpdate(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)

	rr := doJSON(h.createThing, ctx, "", v0binObject(), "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	var created application.ThingSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.Name != "Soptunna 42" {
		t.Fatalf("summary=%+v", created)
	}
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Fatal("ETag required")
	}
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusConflict {
		t.Fatalf("duplicate create=%d, want 409", rr.Code)
	}

	rr = doGET(h.getThing, ctx, "bin-42", "")
	if rr.Code != http.StatusOK || rr.Header().Get("ETag") != etag {
		t.Fatalf("get: %d etag=%q want %q", rr.Code, rr.Header().Get("ETag"), etag)
	}
	if rr := doGET(h.getThing, v0ctx("other"), "bin-42", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant get=%d, want 404", rr.Code)
	}

	// Stale revision nekas.
	obj := v0binObject()
	obj.Name = "Stale"
	if rr := doJSON(h.updateThing, ctx, "bin-42", obj, `"rev-99"`); rr.Code != http.StatusConflict {
		t.Fatalf("stale update=%d, want 409", rr.Code)
	}
	obj.Name = "Soptunna 42 ny"
	rr = doJSON(h.updateThing, ctx, "bin-42", obj, etag)
	if rr.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("ETag") == etag {
		t.Fatal("ETag must advance after update")
	}
	if rr := doJSON(h.updateThing, ctx, "bin-42", obj, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("missing If-Match=%d, want 400", rr.Code)
	}
}

func TestV0ValidationAndAuth(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)

	bad := v0binObject()
	bad.Properties["nope"] = things.Property{}
	if rr := doJSON(h.createThing, ctx, "", bad, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid create=%d, want 400", rr.Code)
	}
	if rr := doJSON(h.createThing, context.Background(), "", v0binObject(), ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d, want 401", rr.Code)
	}
	if rr := doGET(h.getThing, context.Background(), "bin-42", ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated get=%d, want 401", rr.Code)
	}
}

func TestV0BindingsPreviewHistory(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}

	rr := doJSON(h.previewThing, ctx, "bin-42", v0binObject(), "")
	if rr.Code != http.StatusOK {
		t.Fatalf("preview: %d %s", rr.Code, rr.Body.String())
	}
	var preview catalog.EffectiveConfig
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatalf("decode preview: %v", err)
	}
	if preview.ParamValues["usableHeight"] != 1.2 {
		t.Fatalf("preview=%+v", preview)
	}

	// Bindningsbyte kräver If-Match och bumpar revision.
	swapped := map[string]any{"bindings": []things.Binding{
		{DeviceID: "milesight:215", Object: v0DistURN, Resource: "5700", Input: "distance"},
	}}
	rr = doGET(h.getThing, ctx, "bin-42", "")
	etag := rr.Header().Get("ETag")
	srr := doJSON(h.swapBindings, ctx, "bin-42", swapped, etag)
	if srr.Code != http.StatusOK {
		t.Fatalf("swap: %d %s", srr.Code, srr.Body.String())
	}
	if srr.Header().Get("ETag") == etag {
		t.Fatal("ETag must advance after swap")
	}

	rr = doGET(h.getHistory, ctx, "bin-42", "property=distance&limit=10")
	if rr.Code != http.StatusOK {
		t.Fatalf("history: %d %s", rr.Code, rr.Body.String())
	}
	rr = doGET(h.getHistory, ctx, "bin-42", "limit=nope")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad limit=%d, want 400", rr.Code)
	}
}

func TestV0UpgradeMoveDelete(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	v2 := catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v2",
			Required: []string{"distance", "level", "fillRate"},
			Category: "wastebin",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v2",
				Inputs: []string{"distance"}, Outputs: []string{"level"}},
		},
	}
	if rr := doJSON(h.publishTemplate, ctx, "", v2, ""); rr.Code != http.StatusCreated {
		t.Fatalf("publish v2: %d %s", rr.Code, rr.Body.String())
	}
	ref := versionRef{TemplateID: "wastebin", TemplateVersion: "v2"}
	rr := doJSON(h.previewUpgrade, ctx, "bin-42", ref, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("preview upgrade: %d %s", rr.Code, rr.Body.String())
	}
	var preview struct {
		Diff      catalog.Diff `json:"diff"`
		Conflicts []string     `json:"conflicts"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &preview); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(preview.Conflicts) == 0 {
		t.Fatal("removed override must conflict")
	}

	rr = doGET(h.getThing, ctx, "bin-42", "")
	etag := rr.Header().Get("ETag")
	if rr := doJSON(h.upgradeThing, ctx, "bin-42", ref, etag); rr.Code != http.StatusConflict {
		t.Fatalf("blocked upgrade=%d, want 409", rr.Code)
	}
	// Force via query param kräver egen request (doJSON sätter ingen query).
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(ref)
	req := httptest.NewRequest(http.MethodPost, "/x?force=true", &buf)
	req = req.WithContext(ctx)
	req.SetPathValue("id", "bin-42")
	req.Header.Set("If-Match", etag)
	frr := httptest.NewRecorder()
	h.upgradeThing(frr, req)
	if frr.Code != http.StatusOK {
		t.Fatalf("forced upgrade: %d %s", frr.Code, frr.Body.String())
	}

	// Move + overview + delete.
	move := map[string]string{"parentId": "building-a"}
	if rr := doJSON(h.moveThing, ctx, "bin-42", move, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("move without If-Match=%d, want 400", rr.Code)
	}
	mrr := doGET(h.getThing, ctx, "bin-42", "")
	metag := mrr.Header().Get("ETag")
	if rr := doJSON(h.moveThing, ctx, "bin-42", move, metag); rr.Code != http.StatusNotFound {
		t.Fatalf("move to unknown parent=%d, want 404", rr.Code)
	}
	rr = doGET(h.getOverview, ctx, "bin-42", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("overview: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doGET(h.deleteThing, ctx, "bin-42", ""); rr.Code != http.StatusNoContent {
		t.Fatalf("delete=%d, want 204", rr.Code)
	}
	if rr := doGET(h.getThing, ctx, "bin-42", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("get after delete=%d, want 404", rr.Code)
	}
}

func TestV0CatalogAndSearch(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}

	if rr := doGET(h.listTemplates, ctx, "", ""); rr.Code != http.StatusOK {
		t.Fatalf("list templates: %d", rr.Code)
	}
	if rr := doGET(h.listTemplates, ctx, "", "category=wastebin"); rr.Code != http.StatusOK {
		t.Fatalf("filter templates: %d", rr.Code)
	} else {
		var tmpls []catalog.TemplateSpec
		if err := json.Unmarshal(rr.Body.Bytes(), &tmpls); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(tmpls) != 1 || tmpls[0].Template.ID != "wastebin" {
			t.Fatalf("category filter=%+v, want wastebin", tmpls)
		}
	}
	if rr := doGET(h.listTemplates, ctx, "", "category=room"); rr.Code != http.StatusOK {
		t.Fatalf("empty filter: %d", rr.Code)
	} else {
		var empty []catalog.TemplateSpec
		if err := json.Unmarshal(rr.Body.Bytes(), &empty); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(empty) != 0 {
			t.Fatalf("room filter=%d, want 0", len(empty))
		}
	}
	if rr := doGET(h.listVariants, ctx, "", ""); rr.Code != http.StatusOK {
		t.Fatalf("list variants: %d", rr.Code)
	}
	rr := doGET(h.searchThings, ctx, "", "template=wastebin&limit=5")
	if rr.Code != http.StatusOK {
		t.Fatalf("search: %d %s", rr.Code, rr.Body.String())
	}
	var found []application.ThingSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 || found[0].ThingID != "bin-42" {
		t.Fatalf("search=%+v", found)
	}
}

// RegisterHandlers ska acceptera den riktiga OPA-policyn (startup-paritet).
func TestV0RegisterHandlersWithRealPolicy(t *testing.T) {
	f, err := os.Open("../../../assets/config/authz.rego")
	if err != nil {
		t.Fatalf("open policy: %v", err)
	}
	defer f.Close()
	_, mgr, _ := v0setup(t)
	mux := http.NewServeMux()
	if err := RegisterHandlers(context.Background(), mux, mgr, f); err != nil {
		t.Fatalf("register: %v", err)
	}
}

// Hjälpare med exakt scope-uppsättning per tenant: avslöjar
// auktoriseringshål som v0ctx (alla scopes) döljer.
func v0ctxScopes(tenant string, scopes ...auth.Scope) context.Context {
	set := map[auth.Scope]struct{}{}
	for _, s := range scopes {
		set[s] = struct{}{}
	}
	return auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		tenant: set,
	})
}

func v0ctxReadOnly(tenant string) context.Context {
	return v0ctxScopes(tenant, ReadThings)
}

func v0ctxCreateOnly(tenant string) context.Context {
	return v0ctxScopes(tenant, ReadThings, CreateThings)
}

// Regression tenant/scope: skrivendpoints kräver sitt eget scope, inte bara läs.
// Read-only får varken skapa eller förhandsgranska (PUT-preview kräver Update).
func TestV0WriteScopeIsolation(t *testing.T) {
	h, _, _ := v0setup(t)
	full := v0ctx("t")
	v0wastebin(t, h)

	ro := v0ctxReadOnly("t")
	if rr := doJSON(h.createThing, ro, "", v0binObject(), ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("read-only create=%d, want 401", rr.Code)
	}
	if rr := doJSON(h.previewThing, ro, "bin-42", v0binObject(), ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("read-only preview=%d, want 401", rr.Code)
	}
	// Create-only (utan Update) kan skapa men inte förhandsgranska eller uppdatera.
	co := v0ctxCreateOnly("t")
	obj := v0binObject()
	obj.ThingID = "bin-co"
	if rr := doJSON(h.createThing, co, "", obj, ""); rr.Code != http.StatusCreated {
		t.Fatalf("create-only create=%d, want 201 (%s)", rr.Code, rr.Body.String())
	}
	if rr := doJSON(h.previewThing, co, "bin-co", obj, ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("create-only preview=%d, want 401 (preview kräver Update)", rr.Code)
	}
	if rr := doJSON(h.updateThing, co, "bin-co", obj, `"rev-1"`); rr.Code != http.StatusUnauthorized {
		t.Fatalf("create-only update=%d, want 401", rr.Code)
	}
	// Fulla rättigheter fungerar fortfarande.
	if rr := doJSON(h.previewThing, full, "bin-co", obj, ""); rr.Code != http.StatusOK {
		t.Fatalf("full preview=%d, want 200 (%s)", rr.Code, rr.Body.String())
	}
}

// Regression tenant-smuggling: Tenant i body ignoreras till förmån för auth-tenant.
func TestV0BodyTenantIgnored(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)

	obj := v0binObject()
	obj.Tenant = "other"
	if rr := doJSON(h.createThing, ctx, "", obj, ""); rr.Code != http.StatusCreated {
		t.Fatalf("create=%d, want 201 (%s)", rr.Code, rr.Body.String())
	}
	if rr := doGET(h.getThing, ctx, "bin-42", ""); rr.Code != http.StatusOK {
		t.Fatalf("get in auth tenant=%d, want 200", rr.Code)
	}
	if rr := doGET(h.getThing, v0ctx("other"), "bin-42", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("get in body tenant=%d, want 404", rr.Code)
	}
}

// Regression: flera auktoriserade tenants utan särskiljning ska ge 400, aldrig tyst merge.
func TestV0AmbiguousTenant(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		"t1": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
		"t2": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
	})
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous create=%d, want 400", rr.Code)
	}
	// Läsningar fanar ut: okänt överallt ger 404, inte 400.
	if rr := doGET(h.getThing, ctx, "bin-42", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("ambiguous get missing=%d, want 404", rr.Code)
	}
}

// Regression B19: flytt kräver If-Match, bumpas och syns i overview.
func TestV0MoveRequiresIfMatch(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	roomTmpl := catalog.TemplateSpec{
		Template: things.Template{
			ID: "room", Version: "v1",
			Category:     "room",
			RepeatGroups: []string{"temperature."},
			Relations: []things.RelationSpec{
				{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
			},
		},
	}
	if rr := doJSON(h.publishTemplate, ctx, "", roomTmpl, ""); rr.Code != http.StatusCreated {
		t.Fatalf("publish room: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doJSON(h.publishTemplate, ctx, "", catalog.TemplateSpec{
		Template: things.Template{ID: "building", Version: "v1", Category: "building"},
	}, ""); rr.Code != http.StatusCreated {
		t.Fatalf("publish building: %d %s", rr.Code, rr.Body.String())
	}
	mkObj := func(id, parent string) catalog.ObjectSpec {
		obj := catalog.ObjectSpec{
			ThingID: id, TemplateID: "room", TemplateVersion: "v1",
			Name: id, Location: v0loc(), Metadata: v0meta(),
			Properties: map[string]things.Property{"temperature.window": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-" + id, Object: v0TempURN, Resource: "5700", Input: "temperature.window"},
			},
		}
		if parent != "" {
			obj.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		}
		return obj
	}
	bldg := catalog.ObjectSpec{
		ThingID: "b1", TemplateID: "building", TemplateVersion: "v1",
		Name: "Byggnad 1", Location: v0loc(), Metadata: v0meta(),
		Properties: map[string]things.Property{},
	}
	if rr := doJSON(h.createThing, ctx, "", bldg, ""); rr.Code != http.StatusCreated {
		t.Fatalf("create building: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doJSON(h.createThing, ctx, "", mkObj("r1", ""), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create room: %d %s", rr.Code, rr.Body.String())
	}
	move := map[string]string{"parentId": "b1"}
	if rr := doJSON(h.moveThing, ctx, "r1", move, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("move without If-Match=%d, want 400", rr.Code)
	}
	rr := doGET(h.getThing, ctx, "r1", "")
	etag := rr.Header().Get("ETag")
	if rr := doJSON(h.moveThing, ctx, "r1", move, `"rev-99"`); rr.Code != http.StatusConflict {
		t.Fatalf("stale move=%d, want 409", rr.Code)
	}
	mrr := doJSON(h.moveThing, ctx, "r1", move, etag)
	if mrr.Code != http.StatusOK {
		t.Fatalf("move=%d, want 200 (%s)", mrr.Code, mrr.Body.String())
	}
	if mrr.Header().Get("ETag") == etag {
		t.Fatal("ETag must advance after move")
	}
	orr := doGET(h.getOverview, ctx, "b1", "")
	if orr.Code != http.StatusOK {
		t.Fatalf("overview=%d, want 200", orr.Code)
	}
	var ov application.Overview
	if err := json.Unmarshal(orr.Body.Bytes(), &ov); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if len(ov.Children) != 1 || ov.Children[0].ThingID != "r1" {
		t.Fatalf("overview=%+v, want r1 under b1", ov)
	}
}

// Regression P5: historik för okänt thing ska ge 404, inte 200 med tom lista.
func TestV0HistoryUnknownIs404(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doGET(h.getHistory, ctx, "missing", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("history missing=%d, want 404", rr.Code)
	}
	if rr := doGET(h.getOverview, ctx, "missing", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("overview missing=%d, want 404", rr.Code)
	}
	if rr := doGET(h.getOverview, v0ctx("other"), "missing", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant overview=%d, want 404", rr.Code)
	}
}

func doGET2(fn func(http.ResponseWriter, *http.Request), ctx context.Context, id, version, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
	req = req.WithContext(ctx)
	if id != "" {
		req.SetPathValue("id", id)
	}
	if version != "" {
		req.SetPathValue("version", version)
	}
	rr := httptest.NewRecorder()
	fn(rr, req)
	return rr
}

// Katalog: hämta enstaka mall/variant; okänd version ger 404.
func TestV0GetTemplateVariant(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)

	rr := doGET2(h.getTemplate, ctx, "wastebin", "v1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get template: %d %s", rr.Code, rr.Body.String())
	}
	var tmpl catalog.TemplateSpec
	if err := json.Unmarshal(rr.Body.Bytes(), &tmpl); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if tmpl.Template.ID != "wastebin" {
		t.Fatalf("template=%+v", tmpl)
	}
	if rr := doGET2(h.getTemplate, ctx, "wastebin", "v9", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown template=%d, want 404", rr.Code)
	}
	rr = doGET2(h.getVariant, ctx, "wastebin-B", "v1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get variant: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doGET2(h.getVariant, ctx, "nope", "v1", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown variant=%d, want 404", rr.Code)
	}
	if rr := doGET2(h.getTemplate, context.Background(), "wastebin", "v1", ""); rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated=%d, want 401", rr.Code)
	}
}

// Sök + historik: nya filter och offset-parametrar.
func TestV0SearchAndHistoryPaging(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}

	rr := doGET(h.searchThings, ctx, "", "name=soptunna")
	if rr.Code != http.StatusOK {
		t.Fatalf("search name: %d %s", rr.Code, rr.Body.String())
	}
	var found []application.ThingSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search name=%+v, want 1", found)
	}
	rr = doGET(h.searchThings, ctx, "", "variant=wastebin-B&templateVersion=v1")
	if rr.Code != http.StatusOK {
		t.Fatalf("search variant: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search variant=%+v, want 1", found)
	}
	rr = doGET(h.searchThings, ctx, "", "limit=1&offset=1")
	if rr.Code != http.StatusOK {
		t.Fatalf("search page: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("search offset past end=%+v, want empty", found)
	}
	if rr := doGET(h.searchThings, ctx, "", "offset=nope"); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad offset=%d, want 400", rr.Code)
	}

	rr = doGET(h.getHistory, ctx, "bin-42", "limit=5&offset=2")
	if rr.Code != http.StatusOK {
		t.Fatalf("history offset: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doGET(h.getHistory, ctx, "bin-42", "offset=nope"); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad history offset=%d, want 400", rr.Code)
	}
}

// Sök på klassificering/metadata/bbox; felaktig bbox ger 400.
func TestV0SearchLocationMetadata(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	obj := v0binObject()
	obj.Category = "wastebin"
	obj.Metadata = map[string]string{"district": "centrum"}
	obj.Location = &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)}
	if rr := doJSON(h.createThing, ctx, "", obj, ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}

	rr := doGET(h.searchThings, ctx, "", "category=wastebin")
	if rr.Code != http.StatusOK {
		t.Fatalf("search category: %d %s", rr.Code, rr.Body.String())
	}
	var found []application.ThingSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 || found[0].Location == nil {
		t.Fatalf("search category=%+v, want bin with location", found)
	}
	rr = doGET(h.searchThings, ctx, "", "meta.district=centrum")
	if rr.Code != http.StatusOK {
		t.Fatalf("search meta: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search meta=%+v, want 1", found)
	}
	rr = doGET(h.searchThings, ctx, "", "meta.district=s%C3%B6der")
	if rr.Code != http.StatusOK {
		t.Fatalf("search meta miss: %d", rr.Code)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("search meta miss=%+v, want empty", found)
	}
	rr = doGET(h.searchThings, ctx, "", "bbox=17,59,19,60")
	if rr.Code != http.StatusOK {
		t.Fatalf("search bbox: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search bbox=%+v, want 1", found)
	}
	if rr := doGET(h.searchThings, ctx, "", "bbox=17,59"); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad bbox=%d, want 400", rr.Code)
	}
	if rr := doGET(h.searchThings, ctx, "", "bbox=19,60,17,59"); rr.Code != http.StatusBadRequest {
		t.Fatalf("inverted bbox=%d, want 400", rr.Code)
	}
	rr = doGET(h.searchThings, ctx, "", "near=18.06,59.33,100")
	if rr.Code != http.StatusOK {
		t.Fatalf("search near: %d %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("search near=%+v, want 1", found)
	}
	rr = doGET(h.searchThings, ctx, "", "near=0,0,100")
	if rr.Code != http.StatusOK {
		t.Fatalf("search far near: %d", rr.Code)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("search far near=%+v, want empty", found)
	}
	if rr := doGET(h.searchThings, ctx, "", "near=17,59"); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad near=%d, want 400", rr.Code)
	}
	if rr := doGET(h.searchThings, ctx, "", "near=18.06,59.33,-5"); rr.Code != http.StatusBadRequest {
		t.Fatalf("negative radius=%d, want 400", rr.Code)
	}
}

// Träd, bortkoppling och X-Total-Count.
func TestV0TreeUnlinkTotal(t *testing.T) {
	h, _, _ := v0setup(t)
	ctx := v0ctx("t")
	v0wastebin(t, h)
	if rr := doJSON(h.createThing, ctx, "", v0binObject(), ""); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	second := v0binObject()
	second.ThingID = "bin-43"
	if rr := doJSON(h.createThing, ctx, "", second, ""); rr.Code != http.StatusCreated {
		t.Fatalf("create second: %d %s", rr.Code, rr.Body.String())
	}

	rr := doGET(h.searchThings, ctx, "", "limit=1")
	if rr.Code != http.StatusOK {
		t.Fatalf("search: %d", rr.Code)
	}
	if rr.Header().Get("X-Total-Count") != "2" {
		t.Fatalf("total=%q, want 2", rr.Header().Get("X-Total-Count"))
	}

	rr = doGET(h.getTree, ctx, "bin-42", "depth=0")
	if rr.Code != http.StatusOK {
		t.Fatalf("tree: %d %s", rr.Code, rr.Body.String())
	}
	var node application.TreeNode
	if err := json.Unmarshal(rr.Body.Bytes(), &node); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if node.Thing.ThingID != "bin-42" || len(node.Children) != 0 {
		t.Fatalf("tree=%+v, want bare bin", node)
	}
	if rr := doGET(h.getTree, ctx, "bin-42", "depth=nope"); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad depth=%d, want 400", rr.Code)
	}
	if rr := doGET(h.getTree, ctx, "missing", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown tree=%d, want 404", rr.Code)
	}

	// Bortkoppling kräver If-Match och Delete-scope.
	if rr := doJSON(h.unlinkThing, ctx, "bin-42", nil, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("unlink without If-Match=%d, want 400", rr.Code)
	}
	grr := doGET(h.getThing, ctx, "bin-42", "")
	etag := grr.Header().Get("ETag")
	if rr := doJSON(h.unlinkThing, ctx, "bin-42", nil, etag); rr.Code != http.StatusNotFound {
		t.Fatalf("unlink parentless=%d, want 404", rr.Code)
	}
	ro := v0ctxReadOnly("t")
	if rr := doJSON(h.unlinkThing, ro, "bin-42", nil, etag); rr.Code != http.StatusUnauthorized {
		t.Fatalf("read-only unlink=%d, want 401", rr.Code)
	}
}

// Fler-tenant-användare väljer tenant explicit via ?tenant=.
func TestV0MultiTenantSelector(t *testing.T) {
	h, _, _ := v0setup(t)
	v0wastebin(t, h)
	multi := auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		"t1": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
		"t2": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
	})
	obj := v0binObject()
	obj.ThingID = "bin-t1"
	if rr := doJSONTenant(h.createThing, multi, "", obj, "", "t1"); rr.Code != http.StatusCreated {
		t.Fatalf("create in t1: %d %s", rr.Code, rr.Body.String())
	}
	// Utan väljare fanas läsningen ut: träff i exakt en tenant ger 200.
	if rr := doGET(h.getThing, multi, "bin-t1", ""); rr.Code != http.StatusOK {
		t.Fatalf("fanout get=%d, want 200", rr.Code)
	}
	// Med väljare: läsning i rätt tenant, 404 i fel.
	if rr := doGET(h.getThing, multi, "bin-t1", "tenant=t1"); rr.Code != http.StatusOK {
		t.Fatalf("selected get=%d, want 200", rr.Code)
	}
	if rr := doGET(h.getThing, multi, "bin-t1", "tenant=t2"); rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant get=%d, want 404", rr.Code)
	}
	// Väljare utanför behörighet nekas.
	if rr := doGET(h.getThing, multi, "bin-t1", "tenant=other"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("foreign tenant=%d, want 401", rr.Code)
	}
	// En-tenant-användare med fel väljare nekas.
	if rr := doJSONTenant(h.createThing, v0ctx("t1"), "", obj, "", "t2"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong selector=%d, want 401", rr.Code)
	}
}

// Samma sak-ID i två tenants: läsning utan väljare ger 400, med väljare 200.
func TestV0MultiTenantAmbiguousThing(t *testing.T) {
	h, _, _ := v0setup(t)
	v0wastebin(t, h)
	multi := auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		"t1": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
		"t2": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
	})
	obj := v0binObject()
	obj.ThingID = "bin-dup"
	if rr := doJSONTenant(h.createThing, multi, "", obj, "", "t1"); rr.Code != http.StatusCreated {
		t.Fatalf("create t1: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doJSONTenant(h.createThing, multi, "", obj, "", "t2"); rr.Code != http.StatusCreated {
		t.Fatalf("create t2: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doGET(h.getThing, multi, "bin-dup", ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous thing=%d, want 400", rr.Code)
	}
	if rr := doGET(h.getThing, multi, "bin-dup", "tenant=t2"); rr.Code != http.StatusOK {
		t.Fatalf("selected dup=%d, want 200", rr.Code)
	}
	var found application.ThingSummary
	rr := doGET(h.getThing, multi, "bin-dup", "tenant=t2")
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if found.Tenant != "t2" {
		t.Fatalf("tenant=%q, want t2", found.Tenant)
	}
}

// Sökning fanar ut över tenants: träffar slås ihop sorterat, total summeras,
// paging appliceras globalt. Katalogläsning kräver bara behörighet.
func TestV0MultiTenantSearchMerge(t *testing.T) {
	h, _, _ := v0setup(t)
	v0wastebin(t, h)
	multi := auth.WithAccess(context.Background(), map[string]map[auth.Scope]struct{}{
		"t1": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
		"t2": {ReadThings: {}, CreateThings: {}, UpdateThings: {}, DeleteThings: {}},
	})
	mkBin := func(id string) catalog.ObjectSpec {
		obj := v0binObject()
		obj.ThingID = id
		return obj
	}
	for _, tc := range []struct{ id, tenant string }{
		{"bin-b", "t1"}, {"bin-a", "t1"}, {"bin-c", "t2"},
	} {
		if rr := doJSONTenant(h.createThing, multi, "", mkBin(tc.id), "", tc.tenant); rr.Code != http.StatusCreated {
			t.Fatalf("create %s/%s: %d %s", tc.tenant, tc.id, rr.Code, rr.Body.String())
		}
	}
	rr := doGET(h.searchThings, multi, "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("search: %d %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-Total-Count") != "3" {
		t.Fatalf("total=%q, want 3", rr.Header().Get("X-Total-Count"))
	}
	var found []application.ThingSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 3 || found[0].ThingID != "bin-a" || found[1].ThingID != "bin-b" || found[2].ThingID != "bin-c" {
		t.Fatalf("merged=%+v, want sorted a/b/c", found)
	}
	if found[2].Tenant != "t2" {
		t.Fatalf("tenant=%q, want t2", found[2].Tenant)
	}
	// Global paging: limit 2 ger a,b med total 3; offset 2 ger c.
	rr = doGET(h.searchThings, multi, "", "limit=2")
	if rr.Header().Get("X-Total-Count") != "3" {
		t.Fatalf("paged total=%q, want 3", rr.Header().Get("X-Total-Count"))
	}
	found = nil
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 2 || found[0].ThingID != "bin-a" || found[1].ThingID != "bin-b" {
		t.Fatalf("paged=%+v, want a/b", found)
	}
	rr = doGET(h.searchThings, multi, "", "offset=2")
	found = nil
	if err := json.Unmarshal(rr.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(found) != 1 || found[0].ThingID != "bin-c" {
		t.Fatalf("offset=%+v, want c", found)
	}
	// ?tenant= filtrerar till en tenant.
	rr = doGET(h.searchThings, multi, "", "tenant=t2")
	if rr.Header().Get("X-Total-Count") != "1" {
		t.Fatalf("filtered total=%q, want 1", rr.Header().Get("X-Total-Count"))
	}
	// Katalog läsbar med flera tenants (delad katalog, ingen fanout).
	if rr := doGET(h.listTemplates, multi, "", ""); rr.Code != http.StatusOK {
		t.Fatalf("catalog multi=%d, want 200", rr.Code)
	}
}

func doJSONTenant(fn func(http.ResponseWriter, *http.Request), ctx context.Context, id string, body any, ifMatch, tenant string) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	target := "/x"
	if tenant != "" {
		target += "?tenant=" + tenant
	}
	req := httptest.NewRequest(http.MethodPost, target, &buf)
	req = req.WithContext(ctx)
	if id != "" {
		req.SetPathValue("id", id)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rr := httptest.NewRecorder()
	fn(rr, req)
	return rr
}
