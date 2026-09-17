// Package catalog implements Etapp 2: versioned template/variant catalog
// and object configuration compilation.
//
// Rules from THNGS_NEW.md chapters 3 and 10:
//   - Published template, variant and operator versions are immutable.
//   - Objects never upgrade automatically; upgrades preview diffs first and
//     preserve or flag the object's own overrides (never drop silently).
//   - One variant layer: variants reference exactly one template version;
//     variants of variants do not exist.
//   - Parameter precedence: template default -> variant value -> explicit
//     thing override, always reported with its origin.
package catalog

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

// Recipe is a computation prescription from a template: a configured use of
// one versioned operator over explicit inputs.
type Recipe struct {
	Name     string   `json:"name"`
	Operator string   `json:"operator"`
	Version  string   `json:"version,omitempty"`
	Inputs   []string `json:"inputs"`
	Outputs  []string `json:"outputs"`
	Params   []string `json:"params,omitempty"`
}

// TemplateSpec is a publishable template version: the closed property
// definition plus parameter defaults, overridability, recipes and allowed
// relations.
type TemplateSpec struct {
	Template        things.Template    `json:"template"`
	ParamDefaults   map[string]float64 `json:"paramDefaults,omitempty"`
	Overridable     []string           `json:"overridable,omitempty"`
	Recipes         []Recipe           `json:"recipes,omitempty"`
	OperatorVersion map[string]string  `json:"operatorVersions,omitempty"`
}

// VariantSpec is a publishable variant version: exactly one template
// version plus reused parameter values and optional preset properties.
type VariantSpec struct {
	Variant          things.Variant `json:"variant"`
	PresetProperties []string       `json:"presetProperties,omitempty"`
}

// RelationRef is one requested relation of an object under compilation.
type RelationRef struct {
	Name             string `json:"name"`
	TargetThingID    string `json:"targetThingId"`
	TargetTemplateID string `json:"targetTemplateId,omitempty"`
}

// ObjectSpec is the creation/update request compiled into an effective
// configuration: activated properties, bindings, overrides and relations.
type ObjectSpec struct {
	ThingID         string                     `json:"thingId"`
	Tenant          string                     `json:"tenant,omitempty"`
	Name            string                     `json:"name,omitempty"`
	Category        string                     `json:"category,omitempty"`
	Location        *things.Location           `json:"location,omitempty"`
	Metadata        map[string]string          `json:"metadata,omitempty"`
	TemplateID      string                     `json:"templateId"`
	TemplateVersion string                     `json:"templateVersion"`
	VariantID       string                     `json:"variantId,omitempty"`
	VariantVersion  string                     `json:"variantVersion,omitempty"`
	Properties      map[string]things.Property `json:"properties,omitempty"`
	Bindings        []things.Binding           `json:"bindings,omitempty"`
	ParamOverrides  map[string]float64         `json:"paramOverrides,omitempty"`
	Relations       []RelationRef              `json:"relations,omitempty"`
}

// ResolvedRelation is a validated relation with its named slot.
type ResolvedRelation struct {
	Name             string `json:"name"`
	TargetThingID    string `json:"targetThingId"`
	TargetTemplateID string `json:"targetTemplateId,omitempty"`
}

// EffectiveConfig is the materialized configuration stored on the thing:
// version references plus resolved values, so it stays reproducible without
// reading a later template version.
type EffectiveConfig struct {
	ThingID         string                        `json:"thingId,omitempty"`
	Name            string                        `json:"name,omitempty"`
	Category        string                        `json:"category,omitempty"`
	Location        *things.Location              `json:"location,omitempty"`
	Metadata        map[string]string             `json:"metadata,omitempty"`
	TemplateID      string                        `json:"templateId"`
	TemplateVersion string                        `json:"templateVersion"`
	VariantID       string                        `json:"variantId,omitempty"`
	VariantVersion  string                        `json:"variantVersion,omitempty"`
	Properties      map[string]things.Property    `json:"properties,omitempty"`
	Bindings        []things.Binding              `json:"bindings,omitempty"`
	ParamValues     map[string]float64            `json:"paramValues,omitempty"`
	ParamSources    map[string]things.ParamSource `json:"paramSources,omitempty"`
	Recipes         []Recipe                      `json:"recipes,omitempty"`
	Relations       []ResolvedRelation            `json:"relations,omitempty"`
}

// Diff describes template/variant changes for upgrade preview.
type Diff struct {
	AddedProps     []string `json:"addedProperties,omitempty"`
	RemovedProps   []string `json:"removedProperties,omitempty"`
	AddedParams    []string `json:"addedParams,omitempty"`
	RemovedParams  []string `json:"removedParams,omitempty"`
	ChangedRecipes []string `json:"changedRecipes,omitempty"`
}

// Catalog stores immutable published versions. Zero value is unusable;
// use New.
type Catalog struct {
	mu        sync.RWMutex
	templates map[string]TemplateSpec
	variants  map[string]VariantSpec
}

// Store persists published versions across restarts. Memory-only
// deployments pass nil and lose the catalog on restart (dev/test).
// The database is source of truth on conflicts: published versions are
// immutable, so equal keys must carry equal content.
type Store interface {
	SaveTemplate(spec TemplateSpec) error
	SaveVariant(spec VariantSpec) error
	LoadTemplates() []TemplateSpec
	LoadVariants() []VariantSpec
}

// New returns an empty catalog.
func New() *Catalog {
	return &Catalog{
		templates: make(map[string]TemplateSpec),
		variants:  make(map[string]VariantSpec),
	}
}

func versionKey(id, version string) string {
	return id + "/" + version
}

func validateVersionedID(id, version, kind string) error {
	if id == "" {
		return fmt.Errorf("%w: %s id is empty", things.ErrValidation, kind)
	}
	if strings.Contains(id, "/") {
		return fmt.Errorf("%w: %s id %q must not contain %q", things.ErrValidation, kind, id, "/")
	}
	if version == "" {
		return fmt.Errorf("%w: %s %q version is empty", things.ErrValidation, kind, id)
	}
	if strings.Contains(version, "/") {
		return fmt.Errorf("%w: %s %q version must not contain %q", things.ErrValidation, kind, id, "/")
	}
	return nil
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneFloatMap(in map[string]float64) map[string]float64 {
	if in == nil {
		return nil
	}
	out := make(map[string]float64, len(in))
	maps.Copy(out, in)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}

// cloneLocation deep-copies a location so compiled configs never alias
// caller memory.
func cloneLocation(in *things.Location) *things.Location {
	if in == nil {
		return nil
	}
	out := &things.Location{Type: in.Type}
	if in.Coordinates != nil {
		out.Coordinates = append([]byte(nil), in.Coordinates...)
	}
	return out
}

func cloneRecipes(in []Recipe) []Recipe {
	if in == nil {
		return nil
	}
	out := make([]Recipe, len(in))
	for i, r := range in {
		out[i] = Recipe{
			Name:     r.Name,
			Operator: r.Operator,
			Version:  r.Version,
			Inputs:   cloneStrings(r.Inputs),
			Outputs:  cloneStrings(r.Outputs),
			Params:   cloneStrings(r.Params),
		}
	}
	return out
}

func cloneTemplateSpec(spec TemplateSpec) TemplateSpec {
	t := spec.Template
	t.Required = cloneStrings(t.Required)
	t.Optional = cloneStrings(t.Optional)
	t.RepeatGroups = cloneStrings(t.RepeatGroups)
	t.AllowedQuantities = cloneStrings(t.AllowedQuantities)
	t.Labels = cloneStrings(t.Labels)
	if t.Relations != nil {
		rels := make([]things.RelationSpec, len(t.Relations))
		for i, r := range t.Relations {
			rels[i] = things.RelationSpec{
				Name:           r.Name,
				AllowedTargets: cloneStrings(r.AllowedTargets),
				Min:            r.Min,
				Max:            r.Max,
			}
		}
		t.Relations = rels
	}
	return TemplateSpec{
		Template:        t,
		ParamDefaults:   cloneFloatMap(spec.ParamDefaults),
		Overridable:     cloneStrings(spec.Overridable),
		Recipes:         cloneRecipes(spec.Recipes),
		OperatorVersion: cloneStringMap(spec.OperatorVersion),
	}
}

func cloneVariantSpec(spec VariantSpec) VariantSpec {
	v := spec.Variant
	v.ParamValues = cloneFloatMap(v.ParamValues)
	return VariantSpec{
		Variant:          v,
		PresetProperties: cloneStrings(spec.PresetProperties),
	}
}

// PutTemplate publishes one template version. Re-publishing an existing
// version always conflicts, even with identical content: published versions
// are immutable, so a fix requires a new version.
func (c *Catalog) PutTemplate(spec TemplateSpec) error {
	t := spec.Template
	if err := validateVersionedID(t.ID, t.Version, "template"); err != nil {
		return err
	}
	seenProps := make(map[string]struct{}, len(t.Required)+len(t.Optional))
	for _, id := range append(append([]string{}, t.Required...), t.Optional...) {
		if id == "" {
			return fmt.Errorf("%w: template %s/%s has empty property id", things.ErrValidation, t.ID, t.Version)
		}
		if _, dup := seenProps[id]; dup {
			return fmt.Errorf("%w: template %s/%s has duplicate property id %q", things.ErrValidation, t.ID, t.Version, id)
		}
		seenProps[id] = struct{}{}
	}
	for _, prefix := range t.RepeatGroups {
		if prefix == "" || !strings.HasSuffix(prefix, ".") {
			return fmt.Errorf("%w: template %s/%s repeat group %q must be a non-empty prefix ending with '.'", things.ErrValidation, t.ID, t.Version, prefix)
		}
	}
	for _, rel := range t.Relations {
		if rel.Name == "" {
			return fmt.Errorf("%w: template %s/%s has relation without name", things.ErrValidation, t.ID, t.Version)
		}
		if rel.Min < 0 {
			return fmt.Errorf("%w: template %s/%s relation %q has negative min", things.ErrValidation, t.ID, t.Version, rel.Name)
		}
		if rel.Max > 0 && rel.Min > rel.Max {
			return fmt.Errorf("%w: template %s/%s relation %q min exceeds max", things.ErrValidation, t.ID, t.Version, rel.Name)
		}
	}
	seenRecipes := make(map[string]struct{}, len(spec.Recipes))
	for _, r := range spec.Recipes {
		if err := validateRecipe(t, r); err != nil {
			return err
		}
		if _, dup := seenRecipes[r.Name]; dup {
			return fmt.Errorf("%w: template %s/%s has duplicate recipe %q", things.ErrValidation, t.ID, t.Version, r.Name)
		}
		seenRecipes[r.Name] = struct{}{}
		for _, p := range r.Params {
			if _, ok := spec.ParamDefaults[p]; !ok {
				return fmt.Errorf("%w: template %s/%s recipe %q references unknown parameter %q", things.ErrValidation, t.ID, t.Version, r.Name, p)
			}
		}
	}
	for p := range spec.ParamDefaults {
		if p == "" {
			return fmt.Errorf("%w: template %s/%s has empty parameter name", things.ErrValidation, t.ID, t.Version)
		}
	}
	if spec.Template.PrimaryProperty != "" {
		if err := t.ValidatePropertyID(spec.Template.PrimaryProperty); err != nil {
			return fmt.Errorf("%w: template %s/%s primary property: %w", things.ErrValidation, t.ID, t.Version, err)
		}
	}
	overridable := make(map[string]struct{}, len(spec.Overridable))
	for _, p := range spec.Overridable {
		if _, ok := spec.ParamDefaults[p]; !ok {
			return fmt.Errorf("%w: template %s/%s overridable param %q has no default", things.ErrValidation, t.ID, t.Version, p)
		}
		overridable[p] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	key := versionKey(t.ID, t.Version)
	if _, exists := c.templates[key]; exists {
		return fmt.Errorf("%w: template %s/%s already published", things.ErrConflict, t.ID, t.Version)
	}
	c.templates[key] = cloneTemplateSpec(spec)
	return nil
}

func validateRecipe(t things.Template, r Recipe) error {
	if r.Name == "" {
		return fmt.Errorf("%w: template %s/%s has recipe without name", things.ErrValidation, t.ID, t.Version)
	}
	if r.Operator == "" {
		return fmt.Errorf("%w: recipe %q has no operator", things.ErrValidation, r.Name)
	}
	if len(r.Inputs) == 0 {
		return fmt.Errorf("%w: recipe %q has no inputs", things.ErrValidation, r.Name)
	}
	if len(r.Outputs) == 0 {
		return fmt.Errorf("%w: recipe %q has no outputs", things.ErrValidation, r.Name)
	}
	for _, in := range append(append([]string{}, r.Inputs...), r.Outputs...) {
		if err := t.ValidatePropertyID(in); err != nil {
			return fmt.Errorf("%w: recipe %q references %q: %w", things.ErrValidation, r.Name, in, err)
		}
	}
	return nil
}

// GetTemplate returns a published template version.
func (c *Catalog) GetTemplate(id, version string) (TemplateSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	spec, ok := c.templates[versionKey(id, version)]
	if !ok {
		return TemplateSpec{}, false
	}
	return cloneTemplateSpec(spec), true
}

// PutVariant publishes one variant version. The referenced template version
// must already be published (exact version pin), every parameter must exist
// in the template defaults, and every preset property must be allowed.
func (c *Catalog) PutVariant(spec VariantSpec) error {
	v := spec.Variant
	if err := validateVersionedID(v.ID, v.Version, "variant"); err != nil {
		return err
	}
	if v.TemplateID == "" || v.TemplateVersion == "" {
		return fmt.Errorf("%w: variant %s/%s must reference exactly one template version", things.ErrValidation, v.ID, v.Version)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	tmpl, ok := c.templates[versionKey(v.TemplateID, v.TemplateVersion)]
	if !ok {
		return fmt.Errorf("%w: variant %s/%s references unknown template %s/%s", things.ErrValidation, v.ID, v.Version, v.TemplateID, v.TemplateVersion)
	}
	for p := range v.ParamValues {
		if _, ok := tmpl.ParamDefaults[p]; !ok {
			return fmt.Errorf("%w: variant %s/%s sets unknown parameter %q", things.ErrValidation, v.ID, v.Version, p)
		}
	}
	for _, prop := range spec.PresetProperties {
		if err := tmpl.Template.ValidatePropertyID(prop); err != nil {
			return fmt.Errorf("%w: variant %s/%s presets %q: %w", things.ErrValidation, v.ID, v.Version, prop, err)
		}
	}
	key := versionKey(v.ID, v.Version)
	if _, exists := c.variants[key]; exists {
		return fmt.Errorf("%w: variant %s/%s already published", things.ErrConflict, v.ID, v.Version)
	}
	c.variants[key] = cloneVariantSpec(spec)
	return nil
}

// ListTemplates returnerar publicerade mallversioner sorterade på id/version.
func (c *Catalog) ListTemplates() []TemplateSpec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]TemplateSpec, 0, len(c.templates))
	for _, spec := range c.templates {
		out = append(out, cloneTemplateSpec(spec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Template.ID != out[j].Template.ID {
			return out[i].Template.ID < out[j].Template.ID
		}
		return out[i].Template.Version < out[j].Template.Version
	})
	return out
}

// ListVariants returnerar publicerade variantversioner sorterade på id/version.
func (c *Catalog) ListVariants() []VariantSpec {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]VariantSpec, 0, len(c.variants))
	for _, spec := range c.variants {
		out = append(out, cloneVariantSpec(spec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Variant.ID != out[j].Variant.ID {
			return out[i].Variant.ID < out[j].Variant.ID
		}
		return out[i].Variant.Version < out[j].Variant.Version
	})
	return out
}

// GetVariant returns a published variant version.
func (c *Catalog) GetVariant(id, version string) (VariantSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	spec, ok := c.variants[versionKey(id, version)]
	if !ok {
		return VariantSpec{}, false
	}
	return cloneVariantSpec(spec), true
}

// Restore loads previously published versions from a Store at startup.
// Specs were validated at publish time; restore overwrites without
// validation or conflict errors (the database is source of truth).
func (c *Catalog) Restore(templates []TemplateSpec, variants []VariantSpec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, spec := range templates {
		if spec.Template.ID == "" || spec.Template.Version == "" {
			continue
		}
		c.templates[versionKey(spec.Template.ID, spec.Template.Version)] = cloneTemplateSpec(spec)
	}
	for _, spec := range variants {
		if spec.Variant.ID == "" || spec.Variant.Version == "" {
			continue
		}
		c.variants[versionKey(spec.Variant.ID, spec.Variant.Version)] = cloneVariantSpec(spec)
	}
}

// Compile materializes the effective configuration for obj: required
// properties must be activated, every property/binding/relation is validated
// against the pinned template version, and parameters resolve with origin
// template -> variant -> thing.
func (c *Catalog) Compile(obj ObjectSpec) (*EffectiveConfig, error) {
	if err := things.ValidateThingID(obj.ThingID); err != nil {
		return nil, err
	}
	if obj.Tenant == "" {
		return nil, fmt.Errorf("%w: tenant must be provided", things.ErrValidation)
	}
	if obj.TemplateID == "" || obj.TemplateVersion == "" {
		return nil, fmt.Errorf("%w: object %q must reference a template version", things.ErrValidation, obj.ThingID)
	}
	// Beskrivande fält är obligatoriska för alla saker: namn, plats och
	// kategori (ärvd eller egen). Metadata, bindningar och relationer är
	// frivilliga: överordnade objekt kan sakna egna sensorer och
	// fristående objekt sakna föräldrar.
	if strings.TrimSpace(obj.Name) == "" {
		return nil, fmt.Errorf("%w: object %q must have a name", things.ErrValidation, obj.ThingID)
	}
	if obj.Location == nil {
		return nil, fmt.Errorf("%w: object %q must have a location", things.ErrValidation, obj.ThingID)
	}

	c.mu.RLock()
	tmpl, ok := c.templates[versionKey(obj.TemplateID, obj.TemplateVersion)]
	if ok {
		tmpl = cloneTemplateSpec(tmpl)
	}
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: unknown template %s/%s", things.ErrValidation, obj.TemplateID, obj.TemplateVersion)
	}
	t := tmpl.Template

	var variant things.Variant
	var variantID, variantVersion string
	if obj.VariantID != "" || obj.VariantVersion != "" {
		if obj.VariantID == "" || obj.VariantVersion == "" {
			return nil, fmt.Errorf("%w: object %q must reference a full variant version", things.ErrValidation, obj.ThingID)
		}
		c.mu.RLock()
		vspec, ok := c.variants[versionKey(obj.VariantID, obj.VariantVersion)]
		if ok {
			vspec = cloneVariantSpec(vspec)
		}
		c.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("%w: unknown variant %s/%s", things.ErrValidation, obj.VariantID, obj.VariantVersion)
		}
		if vspec.Variant.TemplateID != obj.TemplateID || vspec.Variant.TemplateVersion != obj.TemplateVersion {
			return nil, fmt.Errorf("%w: variant %s/%s targets %s/%s, object pins %s/%s",
				things.ErrValidation, obj.VariantID, obj.VariantVersion,
				vspec.Variant.TemplateID, vspec.Variant.TemplateVersion,
				obj.TemplateID, obj.TemplateVersion)
		}
		variant = vspec.Variant
		variantID, variantVersion = variant.ID, variant.Version
	}

	for _, req := range t.Required {
		if _, ok := obj.Properties[req]; !ok {
			return nil, fmt.Errorf("%w: object %q misses required property %q", things.ErrValidation, obj.ThingID, req)
		}
	}
	if obj.Location != nil {
		if err := obj.Location.Validate(); err != nil {
			return nil, fmt.Errorf("%w: object %q: %w", things.ErrValidation, obj.ThingID, err)
		}
	}
	if err := validateMetadata(obj.ThingID, obj.Metadata); err != nil {
		return nil, err
	}
	for id, prop := range obj.Properties {
		if err := t.ValidatePropertyID(id); err != nil {
			return nil, fmt.Errorf("%w: object %q: %w", things.ErrValidation, obj.ThingID, err)
		}
		if prop.ID != "" && prop.ID != id {
			return nil, fmt.Errorf("%w: object %q property key %q mismatches id %q", things.ErrValidation, obj.ThingID, id, prop.ID)
		}
	}
	for _, b := range obj.Bindings {
		if err := things.ValidateBindingForTemplate(t, b); err != nil {
			return nil, fmt.Errorf("%w: object %q: %w", things.ErrValidation, obj.ThingID, err)
		}
	}

	relations, err := resolveRelations(t, obj)
	if err != nil {
		return nil, err
	}

	params, sources, err := resolveParams(tmpl, variant, obj)
	if err != nil {
		return nil, err
	}

	props := make(map[string]things.Property, len(obj.Properties))
	for id, prop := range obj.Properties {
		prop.ID = id
		props[id] = prop
	}

	category := obj.Category
	if category == "" {
		category = t.Category
	}
	if category == "" {
		return nil, fmt.Errorf("%w: object %q must have a category (thing or template)", things.ErrValidation, obj.ThingID)
	}

	return &EffectiveConfig{
		ThingID:         obj.ThingID,
		Name:            obj.Name,
		Category:        category,
		Location:        cloneLocation(obj.Location),
		Metadata:        cloneStringMap(obj.Metadata),
		TemplateID:      obj.TemplateID,
		TemplateVersion: obj.TemplateVersion,
		VariantID:       variantID,
		VariantVersion:  variantVersion,
		Properties:      props,
		Bindings:        append([]things.Binding(nil), obj.Bindings...),
		ParamValues:     params,
		ParamSources:    sources,
		Recipes:         cloneRecipes(tmpl.Recipes),
		Relations:       relations,
	}, nil
}

// Metadata caps keep free-form annotations bounded without schema
// bureaucracy: descriptive only, never structural.
const (
	maxMetadataEntries = 32
	maxMetadataKeyLen  = 64
	maxMetadataValueLn = 512
)

func validateMetadata(thingID string, md map[string]string) error {
	if len(md) > maxMetadataEntries {
		return fmt.Errorf("%w: object %q has %d metadata entries, max %d",
			things.ErrValidation, thingID, len(md), maxMetadataEntries)
	}
	for k, v := range md {
		if k == "" {
			return fmt.Errorf("%w: object %q has empty metadata key", things.ErrValidation, thingID)
		}
		if len(k) > maxMetadataKeyLen || len(v) > maxMetadataValueLn {
			return fmt.Errorf("%w: object %q metadata entry %q too long", things.ErrValidation, thingID, k)
		}
	}
	return nil
}

func resolveParams(tmpl TemplateSpec, variant things.Variant, obj ObjectSpec) (map[string]float64, map[string]things.ParamSource, error) {
	values := make(map[string]float64, len(tmpl.ParamDefaults))
	sources := make(map[string]things.ParamSource, len(tmpl.ParamDefaults))
	for k, v := range tmpl.ParamDefaults {
		values[k] = v
		sources[k] = things.ParamSourceTemplate
	}
	for k, v := range variant.ParamValues {
		if _, ok := tmpl.ParamDefaults[k]; !ok {
			return nil, nil, fmt.Errorf("%w: variant sets unknown parameter %q", things.ErrValidation, k)
		}
		values[k] = v
		sources[k] = things.ParamSourceVariant
	}
	overridable := make(map[string]struct{}, len(tmpl.Overridable))
	for _, p := range tmpl.Overridable {
		overridable[p] = struct{}{}
	}
	for k, v := range obj.ParamOverrides {
		if _, ok := tmpl.ParamDefaults[k]; !ok {
			return nil, nil, fmt.Errorf("%w: object %q overrides unknown parameter %q", things.ErrValidation, obj.ThingID, k)
		}
		if len(overridable) > 0 {
			if _, ok := overridable[k]; !ok {
				return nil, nil, fmt.Errorf("%w: object %q must not override parameter %q", things.ErrValidation, obj.ThingID, k)
			}
		}
		values[k] = v
		sources[k] = things.ParamSourceThing
	}
	return values, sources, nil
}

func resolveRelations(t things.Template, obj ObjectSpec) ([]ResolvedRelation, error) {
	slots := make(map[string]things.RelationSpec, len(t.Relations))
	for _, spec := range t.Relations {
		slots[spec.Name] = things.RelationSpec{
			Name:           spec.Name,
			AllowedTargets: append([]string(nil), spec.AllowedTargets...),
			Min:            spec.Min,
			Max:            spec.Max,
		}
	}
	if len(obj.Relations) == 0 {
		for _, slot := range slots {
			if slot.Min > 0 {
				return nil, fmt.Errorf("%w: object %q relation %q needs at least %d target(s)",
					things.ErrValidation, obj.ThingID, slot.Name, slot.Min)
			}
		}
		return nil, nil
	}
	counts := make(map[string]int, len(obj.Relations))
	out := make([]ResolvedRelation, 0, len(obj.Relations))
	for _, rel := range obj.Relations {
		slot, ok := slots[rel.Name]
		if !ok {
			return nil, fmt.Errorf("%w: object %q relation %q is not offered by template %s/%s",
				things.ErrValidation, obj.ThingID, rel.Name, t.ID, t.Version)
		}
		if rel.TargetThingID == "" {
			return nil, fmt.Errorf("%w: object %q relation %q has no target", things.ErrValidation, obj.ThingID, rel.Name)
		}
		if rel.TargetThingID == obj.ThingID {
			return nil, fmt.Errorf("%w: object %q must not relate to itself", things.ErrValidation, obj.ThingID)
		}
		if len(slot.AllowedTargets) > 0 {
			allowed := slices.Contains(slot.AllowedTargets, rel.TargetTemplateID)
			if !allowed {
				return nil, fmt.Errorf("%w: object %q relation %q target template %q not allowed",
					things.ErrValidation, obj.ThingID, rel.Name, rel.TargetTemplateID)
			}
		}
		counts[rel.Name]++
		if slot.Max > 0 && counts[rel.Name] > slot.Max {
			return nil, fmt.Errorf("%w: object %q relation %q exceeds max %d",
				things.ErrValidation, obj.ThingID, rel.Name, slot.Max)
		}
		out = append(out, ResolvedRelation{
			Name:             rel.Name,
			TargetThingID:    rel.TargetThingID,
			TargetTemplateID: rel.TargetTemplateID,
		})
	}
	for _, slot := range slots {
		if counts[slot.Name] < slot.Min {
			return nil, fmt.Errorf("%w: object %q relation %q needs at least %d target(s)",
				things.ErrValidation, obj.ThingID, slot.Name, slot.Min)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].TargetThingID < out[j].TargetThingID
	})
	return out, nil
}

// PreviewUpgrade diffs an object's pinned configuration against new template
// and variant versions without changing anything. Overrides, properties or
// bindings that the new versions drop are reported as conflicts: the caller
// must resolve them explicitly, they never vanish silently.
func (c *Catalog) PreviewUpgrade(obj ObjectSpec, newTemplateID, newTemplateVersion, newVariantID, newVariantVersion string) (Diff, []string, error) {
	old, err := c.Compile(obj)
	if err != nil {
		return Diff{}, nil, err
	}

	c.mu.RLock()
	newTmpl, ok := c.templates[versionKey(newTemplateID, newTemplateVersion)]
	if ok {
		newTmpl = cloneTemplateSpec(newTmpl)
	}
	c.mu.RUnlock()
	if !ok {
		return Diff{}, nil, fmt.Errorf("%w: unknown template %s/%s", things.ErrValidation, newTemplateID, newTemplateVersion)
	}
	var newVariant things.Variant
	if newVariantID != "" || newVariantVersion != "" {
		if newVariantID == "" || newVariantVersion == "" {
			return Diff{}, nil, fmt.Errorf("%w: upgrade must reference a full variant version", things.ErrValidation)
		}
		c.mu.RLock()
		vspec, ok := c.variants[versionKey(newVariantID, newVariantVersion)]
		if ok {
			vspec = cloneVariantSpec(vspec)
		}
		c.mu.RUnlock()
		if !ok {
			return Diff{}, nil, fmt.Errorf("%w: unknown variant %s/%s", things.ErrValidation, newVariantID, newVariantVersion)
		}
		if vspec.Variant.TemplateID != newTemplateID || vspec.Variant.TemplateVersion != newTemplateVersion {
			return Diff{}, nil, fmt.Errorf("%w: variant %s/%s targets %s/%s, upgrade pins %s/%s",
				things.ErrValidation, newVariantID, newVariantVersion,
				vspec.Variant.TemplateID, vspec.Variant.TemplateVersion,
				newTemplateID, newTemplateVersion)
		}
		newVariant = vspec.Variant
	}

	var diff Diff
	var conflicts []string

	newAllowed := func(id string) bool {
		return newTmpl.Template.ValidatePropertyID(id) == nil
	}
	for id := range old.Properties {
		if !newAllowed(id) {
			diff.RemovedProps = append(diff.RemovedProps, id)
			conflicts = append(conflicts, fmt.Sprintf("property %q not allowed by %s/%s", id, newTemplateID, newTemplateVersion))
		}
	}
	for _, req := range newTmpl.Template.Required {
		if _, ok := old.Properties[req]; !ok {
			diff.AddedProps = append(diff.AddedProps, req)
			conflicts = append(conflicts, fmt.Sprintf("property %q required by %s/%s is missing", req, newTemplateID, newTemplateVersion))
		}
	}
	for p := range newTmpl.ParamDefaults {
		if _, ok := old.ParamValues[p]; !ok {
			diff.AddedParams = append(diff.AddedParams, p)
		}
	}
	overridable := make(map[string]struct{}, len(newTmpl.Overridable))
	for _, p := range newTmpl.Overridable {
		overridable[p] = struct{}{}
	}
	for p := range old.ParamValues {
		if _, ok := newTmpl.ParamDefaults[p]; !ok {
			diff.RemovedParams = append(diff.RemovedParams, p)
			if src := old.ParamSources[p]; src == things.ParamSourceThing || src == things.ParamSourceVariant {
				conflicts = append(conflicts, fmt.Sprintf("override %q dropped by %s/%s", p, newTemplateID, newTemplateVersion))
			}
			continue
		}
		if src := old.ParamSources[p]; src == things.ParamSourceThing && len(overridable) > 0 {
			if _, ok := overridable[p]; !ok {
				conflicts = append(conflicts, fmt.Sprintf("override %q not overridable in %s/%s", p, newTemplateID, newTemplateVersion))
			}
		}
	}
	oldRecipes := make(map[string]Recipe, len(old.Recipes))
	for _, r := range old.Recipes {
		oldRecipes[r.Name] = r
	}
	for _, r := range newTmpl.Recipes {
		prev, ok := oldRecipes[r.Name]
		if !ok || prev.Operator != r.Operator || prev.Version != r.Version || !equalStrings(prev.Inputs, r.Inputs) || !equalStrings(prev.Outputs, r.Outputs) || !equalStrings(prev.Params, r.Params) {
			diff.ChangedRecipes = append(diff.ChangedRecipes, r.Name)
		}
		delete(oldRecipes, r.Name)
	}
	for name := range oldRecipes {
		diff.ChangedRecipes = append(diff.ChangedRecipes, name+" (removed)")
	}
	for _, b := range old.Bindings {
		if err := things.ValidateBindingForTemplate(newTmpl.Template, b); err != nil {
			conflicts = append(conflicts, fmt.Sprintf("binding %s/%s/%s for %q invalid under %s/%s",
				b.DeviceID, b.Object, b.Resource, b.Input, newTemplateID, newTemplateVersion))
		}
	}
	if newVariant.ID != "" {
		for p := range newVariant.ParamValues {
			if _, ok := newTmpl.ParamDefaults[p]; !ok {
				conflicts = append(conflicts, fmt.Sprintf("variant %s/%s sets %q missing in %s/%s",
					newVariant.ID, newVariant.Version, p, newTemplateID, newTemplateVersion))
			}
		}
	}
	conflicts = append(conflicts, previewRelationConflicts(newTmpl.Template, old.Relations, newTemplateID, newTemplateVersion)...)

	sort.Strings(diff.AddedProps)
	sort.Strings(diff.RemovedProps)
	sort.Strings(diff.AddedParams)
	sort.Strings(diff.RemovedParams)
	sort.Strings(diff.ChangedRecipes)
	sort.Strings(conflicts)

	return diff, conflicts, nil
}

func previewRelationConflicts(tmpl things.Template, old []ResolvedRelation, newTemplateID, newTemplateVersion string) []string {
	if len(old) == 0 {
		return nil
	}
	slots := make(map[string]things.RelationSpec, len(tmpl.Relations))
	for _, s := range tmpl.Relations {
		slots[s.Name] = s
	}
	var out []string
	counts := make(map[string]int, len(old))
	for _, rel := range old {
		slot, ok := slots[rel.Name]
		if !ok {
			out = append(out, fmt.Sprintf("relation %q not offered by %s/%s", rel.Name, newTemplateID, newTemplateVersion))
			continue
		}
		if len(slot.AllowedTargets) > 0 {
			allowed := slices.Contains(slot.AllowedTargets, rel.TargetTemplateID)
			if !allowed {
				out = append(out, fmt.Sprintf("relation %q target template %q not allowed by %s/%s", rel.Name, rel.TargetTemplateID, newTemplateID, newTemplateVersion))
			}
		}
		counts[rel.Name]++
		if slot.Max > 0 && counts[rel.Name] > slot.Max {
			out = append(out, fmt.Sprintf("relation %q exceeds max %d in %s/%s", rel.Name, slot.Max, newTemplateID, newTemplateVersion))
		}
	}
	for _, slot := range slots {
		if counts[slot.Name] < slot.Min {
			out = append(out, fmt.Sprintf("relation %q needs at least %d target(s) in %s/%s", slot.Name, slot.Min, newTemplateID, newTemplateVersion))
		}
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SeedFile is the on-disk start catalog (assets/config/catalog-seed.json):
// API-shaped specs, one entry per thing type in iot-things. Published
// versions are immutable: never change a shipped version, add a new one.
//
// The seed holds templates and variants only: things (instances with
// bindings and locations) are created via the API, never from seed.
// ParseSeed rejects an "objects" section to enforce that boundary.
type SeedFile struct {
	Description string          `json:"description,omitempty"`
	Templates   []TemplateSpec  `json:"templates"`
	Variants    []VariantSpec   `json:"variants,omitempty"`
	Objects     json.RawMessage `json:"objects,omitempty"`
}

// ParseSeed decodes a seed file. Validation happens at publish time
// (SeedCatalog), so malformed specs surface with their template identity.
func ParseSeed(data []byte) (SeedFile, error) {
	var seed SeedFile
	if err := json.Unmarshal(data, &seed); err != nil {
		return SeedFile{}, fmt.Errorf("catalog: invalid seed file: %w", err)
	}
	if len(seed.Objects) > 0 {
		return SeedFile{}, fmt.Errorf("catalog: seed file must not contain things (objects); create them via the API")
	}
	return seed, nil
}
