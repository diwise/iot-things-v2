package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/iot-things-v2/internal/presentation/api/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// Scopes mirror iot-things internal/presentation/api so policies and
// frontend roles transfer unchanged.
const (
	CreateThings auth.Scope = "things.create"
	ReadThings   auth.Scope = "things.read"
	UpdateThings auth.Scope = "things.update"
	DeleteThings auth.Scope = "things.delete"
)

type registerOptions struct {
	authOptions []auth.Option
}

// RegisterOption configures API registration.
type RegisterOption func(*registerOptions)

// WithAccessObjectAuthorization toggles the OPA access-object result model,
// same seam as iot-things.
func WithAccessObjectAuthorization(enabled bool) RegisterOption {
	return func(o *registerOptions) {
		o.authOptions = append(o.authOptions, auth.WithAccessObjectAuthorization(enabled))
	}
}

// Manager is the application surface the HTTP layer needs (implemented by
// *application.Manager). Narrow on purpose: presentation stays decoupled
// from the implementation (same direction as iot-things).
type Manager interface {
	PublishTemplate(ctx context.Context, spec catalog.TemplateSpec) error
	PublishVariant(ctx context.Context, spec catalog.VariantSpec) error
	ListTemplates(ctx context.Context) []catalog.TemplateSpec
	ListVariants(ctx context.Context) []catalog.VariantSpec
	GetTemplate(ctx context.Context, id, version string) (catalog.TemplateSpec, error)
	GetVariant(ctx context.Context, id, version string) (catalog.VariantSpec, error)
	PreviewObject(ctx context.Context, tenant string, obj catalog.ObjectSpec) (*catalog.EffectiveConfig, error)
	CreateObject(ctx context.Context, tenant string, obj catalog.ObjectSpec) (*catalog.EffectiveConfig, error)
	UpdateObject(ctx context.Context, tenant string, obj catalog.ObjectSpec, revision int64) (*catalog.EffectiveConfig, error)
	SwapBindings(ctx context.Context, tenant, thingID string, bindings []things.Binding, revision int64) (*catalog.EffectiveConfig, error)
	PreviewUpgrade(ctx context.Context, tenant, thingID, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion string) (catalog.Diff, []string, error)
	UpgradeObject(ctx context.Context, tenant, thingID, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion string, force bool, revision int64) (catalog.Diff, []string, error)
	DeleteObject(ctx context.Context, tenant, thingID string) error
	MoveObject(ctx context.Context, tenant, childID, newParentID string, revision int64) error
	UnlinkObject(ctx context.Context, tenant, childID string, revision int64) error
	History(ctx context.Context, tenant, thingID, prop string, from, to time.Time, limit, offset int) ([]application.HistoryWrite, error)
	GetObject(ctx context.Context, tenant, thingID string) (application.ThingSummary, error)
	SearchObjects(ctx context.Context, tenant string, filter application.SearchFilter) []application.ThingSummary
	Overview(ctx context.Context, tenant, thingID string) (application.Overview, error)
	Subtree(ctx context.Context, tenant, thingID string, depth int) (application.TreeNode, error)
}

// RegisterHandlers wires the public management API onto mux with the same
// structure as iot-things: router with /api/v1 prefix, OPA auth groups per
// scope (one group per HTTP method and route: the router registers one
// synthetic subtree pattern per group, so the same method in two groups
// panics the mux).
func RegisterHandlers(ctx context.Context, mux *http.ServeMux, mgr Manager, policies io.Reader, opts ...RegisterOption) error {
	const apiPrefix string = "/api/v1"

	log := logging.GetFromContext(ctx)

	registerOpts := registerOptions{}
	for _, apply := range opts {
		apply(&registerOpts)
	}

	authz, err := auth.NewAuthenticator(ctx, policies, registerOpts.authOptions...)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	h := &handler{log: log, mgr: mgr}

	r := router.New(mux, router.WithPrefix(apiPrefix))

	r.Route("catalog", func(r router.ServeMux) {
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(ReadThings))
			r.Get("templates", h.listTemplates)
			r.Get("templates/{id}/{version}", h.getTemplate)
			r.Get("variants", h.listVariants)
			r.Get("variants/{id}/{version}", h.getVariant)
		})
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(CreateThings))
			r.Post("templates", h.publishTemplate)
			r.Post("variants", h.publishVariant)
		})
	})

	r.Route("things", func(r router.ServeMux) {
		// En grupp per HTTP-metod och route: routern registrerar en
		// syntetisk subtree-pattern per (metod, grupp), så samma metod
		// i två grupper med olika scope krockar (Go-mux panik). Därför
		// är {id}-aktioner PUT (idempotenta avsikter; förhandsvisningar
		// är safe+idempotenta, muterande kräver If-Match) och delar
		// UpdateThings-scope: preview-före-skapande kräver därmed
		// uppdateringsrätt (testat i TestV0WriteScopeIsolation).
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(ReadThings))
			r.Get("", h.searchThings)
			r.Get("{id}", h.getThing)
			r.Get("{id}/history", h.getHistory)
			r.Get("{id}/overview", h.getOverview)
			r.Get("{id}/tree", h.getTree)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(CreateThings))
			r.Post("", h.createThing)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(UpdateThings))
			r.Put("{id}", h.updateThing)
			r.Put("{id}/bindings", h.swapBindings)
			r.Put("{id}/preview", h.previewThing)
			r.Put("{id}/move", h.moveThing)
			r.Put("{id}/upgrade/preview", h.previewUpgrade)
			r.Put("{id}/upgrade", h.upgradeThing)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(DeleteThings))
			r.Delete("{id}", h.deleteThing)
			r.Delete("{id}/parent", h.unlinkThing)
		})
	})

	return nil
}

type handler struct {
	log *slog.Logger
	mgr Manager
}

// tenant resolves the authorized tenant for the request. A user with
// several tenants selects one explicitly via ?tenant=; without a selector
// an ambiguous request fails instead of merging tenants silently or
// writing to the wrong one. Reads never fan out across tenants.
// scope är endpointets eget kravscope (läs/skapa/uppdatera/ta bort):
// tenanten måste vara auktoriserad för just den åtgärden, inte bara läs.
func tenant(r *http.Request, scope auth.Scope) (string, error) {
	tenants := auth.GetTenantsWithAllowedScopes(r.Context(), scope)
	if len(tenants) == 0 {
		return "", errUnauthorized
	}
	selected := r.URL.Query().Get("tenant")
	if selected != "" {
		for _, t := range tenants {
			if t == selected {
				return t, nil
			}
		}
		return "", errUnauthorized
	}
	if len(tenants) != 1 {
		return "", errAmbiguousTenant
	}
	return tenants[0], nil
}

var (
	errUnauthorized    = errors.New("unauthorized")
	errAmbiguousTenant = errors.New("ambiguous tenant")
)

func writeAPIError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errUnauthorized):
		writeError(w, http.StatusUnauthorized, "unauthorized")
	case errors.Is(err, errAmbiguousTenant):
		writeError(w, http.StatusBadRequest, "ambiguous tenant (select one via ?tenant=)")
	case errors.Is(err, things.ErrValidation):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, things.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, things.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func decodeBody[T any](r *http.Request) (T, error) {
	var v T
	if r.Body == nil {
		return v, fmt.Errorf("%w: empty body", things.ErrValidation)
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("%w: invalid JSON body: %v", things.ErrValidation, err)
	}
	return v, nil
}

func (h *handler) publishTemplate(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, CreateThings); err != nil {
		writeAPIError(w, err)
		return
	}
	spec, err := decodeBody[catalog.TemplateSpec](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := h.mgr.PublishTemplate(r.Context(), spec); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) listTemplates(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, ReadThings); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.mgr.ListTemplates(r.Context()))
}

func (h *handler) publishVariant(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, CreateThings); err != nil {
		writeAPIError(w, err)
		return
	}
	spec, err := decodeBody[catalog.VariantSpec](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := h.mgr.PublishVariant(r.Context(), spec); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) listVariants(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, ReadThings); err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.mgr.ListVariants(r.Context()))
}

func (h *handler) getTemplate(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, ReadThings); err != nil {
		writeAPIError(w, err)
		return
	}
	spec, err := h.mgr.GetTemplate(r.Context(), r.PathValue("id"), r.PathValue("version"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (h *handler) getVariant(w http.ResponseWriter, r *http.Request) {
	if _, err := tenant(r, ReadThings); err != nil {
		writeAPIError(w, err)
		return
	}
	spec, err := h.mgr.GetVariant(r.Context(), r.PathValue("id"), r.PathValue("version"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (h *handler) createThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, CreateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	obj, err := decodeBody[catalog.ObjectSpec](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	cfg, err := h.mgr.CreateObject(r.Context(), t, obj)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, obj.ThingID)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	_ = cfg
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusCreated, sum)
}

func (h *handler) searchThings(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, ReadThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	q := r.URL.Query()
	limit, err := queryLimit(r, "limit")
	if err != nil {
		writeAPIError(w, err)
		return
	}
	offset, err := queryLimit(r, "offset")
	if err != nil {
		writeAPIError(w, err)
		return
	}
	filter := application.SearchFilter{
		TemplateID:      q.Get("template"),
		TemplateVersion: q.Get("templateVersion"),
		VariantID:       q.Get("variant"),
		NameContains:    q.Get("name"),
		Category:        q.Get("category"),
		Limit:           limit,
		Offset:          offset,
	}
	for key, vals := range q {
		name, ok := strings.CutPrefix(key, "meta.")
		if !ok || name == "" || len(vals) == 0 {
			continue
		}
		if filter.Metadata == nil {
			filter.Metadata = make(map[string]string)
		}
		filter.Metadata[name] = vals[0]
	}
	if raw := q.Get("bbox"); raw != "" {
		box, err := parseBBox(raw)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		filter.BBox = &box
	}
	// Total före paging i X-Total-Count (egen ofiltrerad räkning;
	// dyr vid stora kataloger, men korrekt utan separat count-väg).
	totalFilter := filter
	totalFilter.Limit = 0
	totalFilter.Offset = 0
	w.Header().Set("X-Total-Count", strconv.Itoa(len(h.mgr.SearchObjects(r.Context(), t, totalFilter))))
	writeJSON(w, http.StatusOK, h.mgr.SearchObjects(r.Context(), t, filter))
}

func (h *handler) getThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, ReadThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, r.PathValue("id"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusOK, sum)
}

func (h *handler) updateThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	obj, err := decodeBody[catalog.ObjectSpec](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	id := r.PathValue("id")
	if obj.ThingID != "" && obj.ThingID != id {
		writeAPIError(w, fmt.Errorf("%w: path id %q mismatches body thingId %q", things.ErrValidation, id, obj.ThingID))
		return
	}
	obj.ThingID = id
	if _, err := h.mgr.UpdateObject(r.Context(), t, obj, rev); err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, id)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusOK, sum)
}

func (h *handler) deleteThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, DeleteThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := h.mgr.DeleteObject(r.Context(), t, r.PathValue("id")); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) swapBindings(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var body struct {
		Bindings []things.Binding `json:"bindings"`
	}
	if r.Body == nil {
		writeAPIError(w, fmt.Errorf("%w: empty body", things.ErrValidation))
		return
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, fmt.Errorf("%w: invalid JSON body: %v", things.ErrValidation, err))
		return
	}
	id := r.PathValue("id")
	if _, err := h.mgr.SwapBindings(r.Context(), t, id, body.Bindings, rev); err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, id)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusOK, sum)
}

func (h *handler) previewThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	obj, err := decodeBody[catalog.ObjectSpec](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if id := r.PathValue("id"); obj.ThingID != "" && obj.ThingID != id {
		writeAPIError(w, fmt.Errorf("%w: path id %q mismatches body thingId %q", things.ErrValidation, id, obj.ThingID))
		return
	}
	cfg, err := h.mgr.PreviewObject(r.Context(), t, obj)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

type versionRef struct {
	TemplateID      string `json:"templateId"`
	TemplateVersion string `json:"templateVersion"`
	VariantID       string `json:"variantId,omitempty"`
	VariantVersion  string `json:"variantVersion,omitempty"`
}

func (h *handler) previewUpgrade(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	ref, err := decodeBody[versionRef](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	diff, conflicts, err := h.mgr.PreviewUpgrade(r.Context(), t, r.PathValue("id"), ref.TemplateID, ref.TemplateVersion, ref.VariantID, ref.VariantVersion)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"diff": diff, "conflicts": conflicts})
}

func (h *handler) upgradeThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	ref, err := decodeBody[versionRef](r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	force := r.URL.Query().Get("force") == "true"
	diff, conflicts, err := h.mgr.UpgradeObject(r.Context(), t, r.PathValue("id"), ref.TemplateID, ref.TemplateVersion, ref.VariantID, ref.VariantVersion, force, rev)
	if err != nil {
		if errors.Is(err, things.ErrConflict) {
			writeJSON(w, http.StatusConflict, map[string]any{"diff": diff, "conflicts": conflicts, "error": err.Error()})
			return
		}
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"diff": diff, "conflicts": conflicts})
}

func (h *handler) moveThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, UpdateThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var body struct {
		ParentID string `json:"parentId"`
	}
	if r.Body == nil {
		writeAPIError(w, fmt.Errorf("%w: empty body", things.ErrValidation))
		return
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeAPIError(w, fmt.Errorf("%w: invalid JSON body: %v", things.ErrValidation, err))
		return
	}
	id := r.PathValue("id")
	if err := h.mgr.MoveObject(r.Context(), t, id, body.ParentID, rev); err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, id)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusOK, sum)
}

func (h *handler) getHistory(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, ReadThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	q := r.URL.Query()
	limit, err := queryLimit(r, "limit")
	if err != nil {
		writeAPIError(w, err)
		return
	}
	offset, err := queryLimit(r, "offset")
	if err != nil {
		writeAPIError(w, err)
		return
	}
	from, err := queryTime(q.Get("from"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	to, err := queryTime(q.Get("to"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	entries, err := h.mgr.History(r.Context(), t, r.PathValue("id"), q.Get("property"), from, to, limit, offset)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if entries == nil {
		entries = []application.HistoryWrite{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *handler) getOverview(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, ReadThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	ov, err := h.mgr.Overview(r.Context(), t, r.PathValue("id"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ov)
}

func (h *handler) getTree(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, ReadThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	depth := 1
	if raw := r.URL.Query().Get("depth"); raw != "" {
		depth, err = strconv.Atoi(raw)
		if err != nil || depth < 0 {
			writeAPIError(w, fmt.Errorf("%w: invalid depth", things.ErrValidation))
			return
		}
	}
	node, err := h.mgr.Subtree(r.Context(), t, r.PathValue("id"), depth)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

// unlinkThing kopplar loss föräldern (DELETE, men kräver If-Match som
// övriga konfigurationsändringar). Ligger i DeleteThings-gruppen eftersom
// routern förbjuder samma metod i två grupper med olika scope;
// delete-scope är striktare än update och därför det säkra valet.
func (h *handler) unlinkThing(w http.ResponseWriter, r *http.Request) {
	t, err := tenant(r, DeleteThings)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	rev, err := requireIfMatch(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	id := r.PathValue("id")
	if err := h.mgr.UnlinkObject(r.Context(), t, id, rev); err != nil {
		writeAPIError(w, err)
		return
	}
	sum, err := h.mgr.GetObject(r.Context(), t, id)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("ETag", FormatETag(sum.Revision))
	writeJSON(w, http.StatusOK, sum)
}

func requireIfMatch(r *http.Request) (int64, error) {
	raw := r.Header.Get("If-Match")
	if raw == "" {
		return 0, fmt.Errorf("%w: missing If-Match header", things.ErrValidation)
	}
	rev, err := ParseETag(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid If-Match header: %v", things.ErrValidation, err)
	}
	return rev, nil
}

func queryLimit(r *http.Request, key string) (int, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: invalid %s", things.ErrValidation, key)
	}
	return n, nil
}

func queryTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid time %q (want RFC3339)", things.ErrValidation, raw)
	}
	return ts, nil
}

// parseBBox parses "minLon,minLat,maxLon,maxLat" (WGS84) for location search.
func parseBBox(raw string) (things.BBox, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 4 {
		return things.BBox{}, fmt.Errorf("%w: invalid bbox %q (want minLon,minLat,maxLon,maxLat)", things.ErrValidation, raw)
	}
	nums := make([]float64, 4)
	for i, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return things.BBox{}, fmt.Errorf("%w: invalid bbox %q: %v", things.ErrValidation, raw, err)
		}
		nums[i] = v
	}
	box := things.BBox{MinLon: nums[0], MinLat: nums[1], MaxLon: nums[2], MaxLat: nums[3]}
	if box.MinLon < -180 || box.MaxLon > 180 || box.MinLat < -90 || box.MaxLat > 90 ||
		box.MinLon > box.MaxLon || box.MinLat > box.MaxLat {
		return things.BBox{}, fmt.Errorf("%w: invalid bbox %q (WGS84, min <= max)", things.ErrValidation, raw)
	}
	return box, nil
}
