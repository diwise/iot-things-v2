// Manager Etapp 6: hanteringslagret för katalog, objekt, bindningar,
// uppgraderingar, relationer, historik och översikter.
//
// Manager äger katalogskrivningar och objektlivscykel mot Runtime + store;
// mätvägarna (HandleMeasurements) är orörda. Alla metoder tar tenant
// explicit: presentation lagret löser tenant ur OPA-auth och skickar vidare,
// aldrig ur body (tenant i body ignoreras till förmån för path/auth).
package application

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// PropertyValue är ett aktuellt egenskapsvärde med enhet och ursprung.
type PropertyValue struct {
	PropertyID  string    `json:"propertyId"`
	DisplayName string    `json:"displayName,omitempty"`
	Value       *float64  `json:"value,omitempty"`
	Quality     string    `json:"quality,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	ObservedAt  time.Time `json:"observedAt"`
	Source      string    `json:"source,omitempty"`
}

// ThingSummary är läsytan för en sak: identitet, mallreferenser, aktuella
// värden och effektiva parametrar med ursprung.
type ThingSummary struct {
	ThingID         string                        `json:"thingId"`
	Tenant          string                        `json:"tenant"`
	Name            string                        `json:"name,omitempty"`
	Classification  string                        `json:"classification,omitempty"`
	Location        *things.Location              `json:"location,omitempty"`
	Metadata        map[string]string             `json:"metadata,omitempty"`
	TemplateID      string                        `json:"templateId"`
	TemplateVersion string                        `json:"templateVersion"`
	VariantID       string                        `json:"variantId,omitempty"`
	VariantVersion  string                        `json:"variantVersion,omitempty"`
	Revision        int64                         `json:"revision"`
	Values          map[string]PropertyValue      `json:"values,omitempty"`
	ParamValues     map[string]float64            `json:"paramValues,omitempty"`
	ParamSources    map[string]things.ParamSource `json:"paramSources,omitempty"`
}

// Overview är en navigerbar sammanställning: förälder + direkta barns
// aktuella värden, utan att kopiera mätningar till föräldern.
type Overview struct {
	Thing    ThingSummary   `json:"thing"`
	Children []ThingSummary `json:"children,omitempty"`
}

// Manager samordnar katalog, runtime och läsningar.
type Manager struct {
	cat      *catalog.Catalog
	rt       *Runtime
	store    RuntimeStore
	catStore catalog.Store
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithCatalogStore persists published versions across restarts (nil =
// memory-only catalog, lost on restart).
func WithCatalogStore(s catalog.Store) ManagerOption {
	return func(m *Manager) { m.catStore = s }
}

// NewManager skapar ett hanteringslager. cat får delas med anroparen vid
// test (samma pekare).
func NewManager(cat *catalog.Catalog, rt *Runtime, store RuntimeStore, opts ...ManagerOption) *Manager {
	m := &Manager{cat: cat, rt: rt, store: store}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// PublishTemplate publicerar en mallversion (oföränderlig efter publicering)
// och persisterar den vid konfigurerad kataloglagring.
func (m *Manager) PublishTemplate(_ context.Context, spec catalog.TemplateSpec) error {
	if err := m.cat.PutTemplate(spec); err != nil {
		return err
	}
	if m.catStore != nil {
		if err := m.catStore.SaveTemplate(spec); err != nil {
			return err
		}
	}
	return nil
}

// PublishVariant publicerar en variantversion låst till exakt mallversion
// och persisterar den vid konfigurerad kataloglagring.
func (m *Manager) PublishVariant(_ context.Context, spec catalog.VariantSpec) error {
	if err := m.cat.PutVariant(spec); err != nil {
		return err
	}
	if m.catStore != nil {
		if err := m.catStore.SaveVariant(spec); err != nil {
			return err
		}
	}
	return nil
}

// ListTemplates returnerar publicerade mallversioner sorterade på id/version.
func (m *Manager) ListTemplates(_ context.Context) []catalog.TemplateSpec {
	return m.cat.ListTemplates()
}

// ListVariants returnerar publicerade variantversioner sorterade på id/version.
func (m *Manager) ListVariants(_ context.Context) []catalog.VariantSpec {
	return m.cat.ListVariants()
}

// GetTemplate läser en publicerad mallversion. Okänd version ger ErrNotFound.
// Publicerade versioner är oföränderliga: ingen uppdatering/borttagning finns.
func (m *Manager) GetTemplate(_ context.Context, id, version string) (catalog.TemplateSpec, error) {
	spec, ok := m.cat.GetTemplate(id, version)
	if !ok {
		return catalog.TemplateSpec{}, fmt.Errorf("%w: template %s/%s", things.ErrNotFound, id, version)
	}
	return spec, nil
}

// GetVariant läser en publicerad variantversion. Okänd version ger ErrNotFound.
func (m *Manager) GetVariant(_ context.Context, id, version string) (catalog.VariantSpec, error) {
	spec, ok := m.cat.GetVariant(id, version)
	if !ok {
		return catalog.VariantSpec{}, fmt.Errorf("%w: variant %s/%s", things.ErrNotFound, id, version)
	}
	return spec, nil
}

// PreviewObject kompilerar utan att lagra: effektiv konfiguration med
// paramursprung före skapande eller ändring.
func (m *Manager) PreviewObject(_ context.Context, tenant string, obj catalog.ObjectSpec) (*catalog.EffectiveConfig, error) {
	obj.Tenant = tenant
	return m.cat.Compile(obj)
}

// CreateObject skapar en sak med revision 1.
func (m *Manager) CreateObject(_ context.Context, tenant string, obj catalog.ObjectSpec) (*catalog.EffectiveConfig, error) {
	obj.Tenant = tenant
	if _, exists := m.store.LoadThing(tenant, obj.ThingID); exists {
		return nil, fmt.Errorf("%w: thing %q already exists", things.ErrConflict, obj.ThingID)
	}
	return m.rt.RegisterThing(tenant, m.cat, obj, 1)
}

// UpdateObject omsparar en sak efter ompilering; gammal revision nekas (409).
func (m *Manager) UpdateObject(_ context.Context, tenant string, obj catalog.ObjectSpec, revision int64) (*catalog.EffectiveConfig, error) {
	obj.Tenant = tenant
	if _, exists := m.store.LoadThing(tenant, obj.ThingID); !exists {
		return nil, fmt.Errorf("%w: thing %q", things.ErrNotFound, obj.ThingID)
	}
	return m.rt.RegisterThing(tenant, m.cat, obj, revision)
}

// SwapBindings byter sensorkopplingar utan att byta objektets identitet,
// historik eller beräkningsreferenser: övrig konfiguration rekonstrueras
// från lagrad effektiv config och kompileras om.
func (m *Manager) SwapBindings(_ context.Context, tenant, thingID string, bindings []things.Binding, revision int64) (*catalog.EffectiveConfig, error) {
	obj, err := m.reconstructSpec(tenant, thingID)
	if err != nil {
		return nil, err
	}
	obj.Bindings = bindings
	return m.rt.RegisterThing(tenant, m.cat, obj, revision)
}

// PreviewUpgrade visar diff och konflikter mot nya versioner utan att ändra.
func (m *Manager) PreviewUpgrade(_ context.Context, tenant, thingID, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion string) (catalog.Diff, []string, error) {
	obj, err := m.reconstructSpec(tenant, thingID)
	if err != nil {
		return catalog.Diff{}, nil, err
	}
	return m.cat.PreviewUpgrade(obj, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion)
}

// UpgradeObject aktiverar nya versioner. Med konflikter och utan force
// nekas uppgraderingen (undantag försvinner aldrig tyst).
func (m *Manager) UpgradeObject(_ context.Context, tenant, thingID, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion string, force bool, revision int64) (catalog.Diff, []string, error) {
	obj, err := m.reconstructSpec(tenant, thingID)
	if err != nil {
		return catalog.Diff{}, nil, err
	}
	diff, conflicts, err := m.cat.PreviewUpgrade(obj, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion)
	if err != nil {
		return diff, conflicts, err
	}
	if len(conflicts) > 0 && !force {
		return diff, conflicts, fmt.Errorf("%w: upgrade of %q has %d conflict(s): %s",
			things.ErrConflict, thingID, len(conflicts), strings.Join(conflicts, "; "))
	}
	if len(conflicts) > 0 {
		obj = stripConflicting(obj, m.cat, newTemplateID, newTemplateVersion)
	}
	obj.TemplateID, obj.TemplateVersion = newTemplateID, newTemplateVersion
	obj.VariantID, obj.VariantVersion = newVariantID, newVariantVersion
	if _, err := m.rt.RegisterThing(tenant, m.cat, obj, revision); err != nil {
		return diff, conflicts, err
	}
	return diff, conflicts, nil
}

// stripConflicting tar bort egna val som nya versionerna inte längre
// tillåter (force-uppgradering): borttagna parametrar, otillåtna egenskaper
// och bindningar samt relationsmål utanför mallen. Krävda nya egenskaper
// kan inte hittas på: saknas de faller kompileringen även med force.
func stripConflicting(obj catalog.ObjectSpec, cat *catalog.Catalog, newTemplateID, newTemplateVersion string) catalog.ObjectSpec {
	tmpl, ok := cat.GetTemplate(newTemplateID, newTemplateVersion)
	if !ok {
		return obj
	}
	t := tmpl.Template
	allowedParam := func(p string) bool {
		if _, ok := tmpl.ParamDefaults[p]; !ok {
			return false
		}
		if len(tmpl.Overridable) == 0 {
			return true
		}
		return slices.Contains(tmpl.Overridable, p)
	}
	for p := range obj.ParamOverrides {
		if !allowedParam(p) {
			delete(obj.ParamOverrides, p)
		}
	}
	for id := range obj.Properties {
		if err := t.ValidatePropertyID(id); err != nil {
			delete(obj.Properties, id)
		}
	}
	kept := obj.Bindings[:0]
	for _, b := range obj.Bindings {
		if err := things.ValidateBindingForTemplate(t, b); err == nil {
			kept = append(kept, b)
		}
	}
	obj.Bindings = kept
	slots := make(map[string]things.RelationSpec, len(t.Relations))
	for _, s := range t.Relations {
		slots[s.Name] = s
	}
	keptRels := obj.Relations[:0]
	for _, rel := range obj.Relations {
		slot, ok := slots[rel.Name]
		if !ok {
			continue
		}
		allowed := len(slot.AllowedTargets) == 0
		if slices.Contains(slot.AllowedTargets, rel.TargetTemplateID) {
			allowed = true
		}
		if allowed {
			keptRels = append(keptRels, rel)
		}
	}
	obj.Relations = keptRels
	return obj
}

// DeleteObject tar bort en sak (blockeras med aktiva barn, ingen kaskad).
func (m *Manager) DeleteObject(_ context.Context, tenant, thingID string) error {
	return m.rt.DeleteThing(tenant, thingID)
}

// MoveObject flyttar ett barns partOf-förälder atomärt med optimistisk
// låsning (B19): revision måste matcha lagrad revision, annars 409.
// Flytten går via omkompilering+RegisterThing (samma väg som Update) så att
// både hierarkilagringen och den lagrade effektiva konfigurationen
// uppdateras tillsammans: en efterföljande SwapBindings/Upgrade
// (reconstructSpec) återställer aldrig den gamla föräldern. Övriga
// relationstyper än partOf bevaras. Revision bumpas vid lyckad flytt.
func (m *Manager) MoveObject(_ context.Context, tenant, childID, newParentID string, revision int64) error {
	child, err := m.reconstructSpec(tenant, childID)
	if err != nil {
		return err
	}
	parentState, ok := m.store.LoadThing(tenant, newParentID)
	if !ok {
		return fmt.Errorf("%w: thing %q", things.ErrNotFound, newParentID)
	}
	parentCfg, err := decodeConfig(parentState.ConfigJSON)
	if err != nil {
		return err
	}
	kept := child.Relations[:0]
	for _, rel := range child.Relations {
		if rel.Name != "partOf" {
			kept = append(kept, rel)
		}
	}
	child.Relations = append(kept, catalog.RelationRef{
		Name:             "partOf",
		TargetThingID:    newParentID,
		TargetTemplateID: parentCfg.TemplateID,
	})
	_, err = m.rt.RegisterThing(tenant, m.cat, child, revision)
	return err
}

// History läser tidsbegränsad egenskapshistorik (tom prop = alla).
// Okänt thing ger ErrNotFound (404) i stället för tom 200-lista så att
// tenant-isolering inte läcker via statuskoder. limit > 0 kapar äldst
// först, offset > 0 hoppar över de äldsta raderna (paging).
func (m *Manager) History(_ context.Context, tenant, thingID, prop string, from, to time.Time, limit, offset int) ([]HistoryWrite, error) {
	if _, ok := m.store.LoadThing(tenant, thingID); !ok {
		return nil, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	return m.store.History(tenant, thingID, prop, from, to, limit, offset), nil
}

// GetObject läser en sammanfattning med aktuella värden.
func (m *Manager) GetObject(_ context.Context, tenant, thingID string) (ThingSummary, error) {
	st, ok := m.store.LoadThing(tenant, thingID)
	if !ok {
		return ThingSummary{}, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	cfg, err := decodeConfig(st.ConfigJSON)
	if err != nil {
		return ThingSummary{}, err
	}
	return summarize(tenant, thingID, cfg, st), nil
}

// SearchFilter styr saklistning: exakta matchningar på mall/variant samt
// skiftlägesokänslig delsträng på namn. Tomma fält filtrerar inget.
// Limit > 0 kapar, Offset > 0 hoppar över de första (sorterat på thing-ID).
type SearchFilter struct {
	TemplateID      string
	TemplateVersion string
	VariantID       string
	NameContains    string
	Classification  string
	Metadata        map[string]string
	BBox            *things.BBox
	Limit           int
	Offset          int
}

// SearchObjects listar saker för tenant enligt filter, sorterat på thing-ID.
func (m *Manager) SearchObjects(_ context.Context, tenant string, f SearchFilter) []ThingSummary {
	var out []ThingSummary
	for _, id := range m.store.ListThingIDs(tenant) {
		st, ok := m.store.LoadThing(tenant, id)
		if !ok {
			continue
		}
		cfg, err := decodeConfig(st.ConfigJSON)
		if err != nil {
			continue
		}
		if f.TemplateID != "" && cfg.TemplateID != f.TemplateID {
			continue
		}
		if f.TemplateVersion != "" && cfg.TemplateVersion != f.TemplateVersion {
			continue
		}
		if f.VariantID != "" && cfg.VariantID != f.VariantID {
			continue
		}
		if f.NameContains != "" && !strings.Contains(strings.ToLower(cfg.Name), strings.ToLower(f.NameContains)) {
			continue
		}
		if f.Classification != "" && cfg.Classification != f.Classification {
			continue
		}
		if len(f.Metadata) > 0 {
			match := true
			for k, v := range f.Metadata {
				if got, ok := cfg.Metadata[k]; !ok || got != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		if f.BBox != nil {
			if cfg.Location == nil || !cfg.Location.IntersectsBBox(*f.BBox) {
				continue
			}
		}
		out = append(out, summarize(tenant, id, cfg, st))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ThingID < out[j].ThingID })
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			return nil
		}
		out = out[f.Offset:]
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out
}

// Overview läser förälder + direkta barn med aktuella värden för gemensamma
// översikter (byggnad/rum, växthus/bänkar). Otillgängliga objekt (annan
// tenant) syns aldrig: allt läses inom tenant.
func (m *Manager) Overview(ctx context.Context, tenant, thingID string) (Overview, error) {
	parent, err := m.GetObject(ctx, tenant, thingID)
	if err != nil {
		return Overview{}, err
	}
	ov := Overview{Thing: parent}
	for _, child := range m.store.Children(tenant, thingID) {
		sum, err := m.GetObject(ctx, tenant, child)
		if err != nil {
			continue
		}
		ov.Children = append(ov.Children, sum)
	}
	return ov, nil
}

func summarize(tenant, thingID string, cfg *catalog.EffectiveConfig, st StoredThingState) ThingSummary {
	sum := ThingSummary{
		ThingID:         thingID,
		Tenant:          tenant,
		Name:            cfg.Name,
		Classification:  cfg.Classification,
		Location:        cfg.Location,
		Metadata:        cfg.Metadata,
		TemplateID:      cfg.TemplateID,
		TemplateVersion: cfg.TemplateVersion,
		VariantID:       cfg.VariantID,
		VariantVersion:  cfg.VariantVersion,
		Revision:        st.Revision,
		Values:          make(map[string]PropertyValue, len(cfg.Properties)),
		ParamValues:     cfg.ParamValues,
		ParamSources:    cfg.ParamSources,
	}
	units := make(map[string]string, len(cfg.Properties))
	for id, prop := range cfg.Properties {
		units[id] = prop.Unit
	}
	for id, prop := range cfg.Properties {
		pv := PropertyValue{PropertyID: id, DisplayName: prop.DisplayName, Unit: units[id]}
		if cur, ok := st.Current[id]; ok {
			pv.Value = cur.Value
			pv.Quality = cur.Quality
			pv.ObservedAt = cur.ObservedAt
			pv.Source = cur.Source
		}
		sum.Values[id] = pv
	}
	return sum
}

// reconstructSpec återskapar en kompilerbar spec från lagrad effektiv
// config: bindningar kan då bytas och versioner uppgraderas utan att
// objektets egna val (egenskaper, thing-overrides, relationer) tappas.
func (m *Manager) reconstructSpec(tenant, thingID string) (catalog.ObjectSpec, error) {
	st, ok := m.store.LoadThing(tenant, thingID)
	if !ok {
		return catalog.ObjectSpec{}, fmt.Errorf("%w: thing %q", things.ErrNotFound, thingID)
	}
	cfg, err := decodeConfig(st.ConfigJSON)
	if err != nil {
		return catalog.ObjectSpec{}, err
	}
	obj := catalog.ObjectSpec{
		ThingID:         thingID,
		Tenant:          tenant,
		Name:            cfg.Name,
		Classification:  cfg.Classification,
		Location:        cfg.Location,
		Metadata:        cfg.Metadata,
		TemplateID:      cfg.TemplateID,
		TemplateVersion: cfg.TemplateVersion,
		VariantID:       cfg.VariantID,
		VariantVersion:  cfg.VariantVersion,
		Properties:      cfg.Properties,
		Bindings:        cfg.Bindings,
		ParamOverrides:  map[string]float64{},
	}
	for k, v := range cfg.ParamValues {
		if cfg.ParamSources[k] == things.ParamSourceThing {
			obj.ParamOverrides[k] = v
		}
	}
	for _, r := range cfg.Relations {
		obj.Relations = append(obj.Relations, catalog.RelationRef{
			Name:             r.Name,
			TargetThingID:    r.TargetThingID,
			TargetTemplateID: r.TargetTemplateID,
		})
	}
	return obj, nil
}
