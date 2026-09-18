package catalog

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

const (
	urnDistance = "urn:oma:lwm2m:ext:3330"
	urnTemp     = "urn:oma:lwm2m:ext:3303"
	urnHum      = "urn:oma:lwm2m:ext:3304"
	urnLux      = "urn:oma:lwm2m:ext:3301"
)

func wastebinTemplate() TemplateSpec {
	return TemplateSpec{
		Template: things.Template{
			ID:           "wastebin",
			Version:      "v1",
			Required:     []string{"distance", "level", "fillRate"},
			RepeatGroups: []string{},
			Category:     "wastebin",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
		Recipes: []Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
		},
	}
}

func testLocation() *things.Location {
	return &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)}
}

func testMetadata() map[string]string {
	return map[string]string{"test": "true"}
}

func seedBins(t *testing.T, c *Catalog) {
	t.Helper()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("put wastebin template: %v", err)
	}
	for id, h := range map[string]float64{"wastebin-A": 0.8, "wastebin-B": 1.2, "wastebin-C": 1.5, "wastebin-D": 1.8} {
		if err := c.PutVariant(VariantSpec{Variant: things.Variant{
			ID: id, Version: "v1",
			TemplateID: "wastebin", TemplateVersion: "v1",
			ParamValues: map[string]float64{"usableHeight": h},
		}}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
}

func TestFourBinsShareRecipe(t *testing.T) {
	c := New()
	seedBins(t, c)
	for id, h := range map[string]float64{"wastebin-A": 0.8, "wastebin-B": 1.2, "wastebin-C": 1.5, "wastebin-D": 1.8} {
		cfg, err := c.Compile(ObjectSpec{
			ThingID: "bin-" + id, Tenant: "t", Name: "bin",
			Location: testLocation(), Metadata: testMetadata(),
			TemplateID: "wastebin", TemplateVersion: "v1",
			VariantID: id, VariantVersion: "v1",
			Properties: map[string]things.Property{
				"distance": {}, "level": {}, "fillRate": {},
			},
			Bindings: []things.Binding{
				{DeviceID: "milesight:214", Object: urnDistance, Resource: "5700", Input: "distance"},
			},
		})
		if err != nil {
			t.Fatalf("compile %s: %v", id, err)
		}
		if len(cfg.Recipes) != 1 || cfg.Recipes[0].Name != "wastebin-level" {
			t.Fatalf("%s: expected shared recipe wastebin-level, got %+v", id, cfg.Recipes)
		}
		v, src, ok := cfg.ParamValues["usableHeight"], cfg.ParamSources["usableHeight"], true
		_ = ok
		if v != h || src != things.ParamSourceVariant {
			t.Fatalf("%s: usableHeight=%v (%s), want %v (variant)", id, v, src, h)
		}
	}
}

func TestParamPrecedenceWithOrigin(t *testing.T) {
	c := New()
	seedBins(t, c)
	cfg, err := c.Compile(ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cfg.ParamValues["usableHeight"] != 1.2 || cfg.ParamSources["usableHeight"] != things.ParamSourceVariant {
		t.Fatalf("usableHeight precedence broken: %+v", cfg.ParamValues)
	}
	if cfg.ParamValues["sensorToBottom"] != 1.42 || cfg.ParamSources["sensorToBottom"] != things.ParamSourceThing {
		t.Fatalf("sensorToBottom override broken: %+v", cfg.ParamValues)
	}
}

func TestPublishedVersionsImmutable(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := c.PutTemplate(wastebinTemplate()); err == nil {
		t.Fatal("republishing identical template version must conflict")
	}
	before, err := c.Compile(ObjectSpec{
		ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	})
	if err != nil {
		t.Fatalf("compile v1: %v", err)
	}
	v2 := wastebinTemplate()
	v2.Template.Version = "v2"
	v2.ParamDefaults["usableHeight"] = 2.0
	if err := c.PutTemplate(v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	after, err := c.Compile(ObjectSpec{
		ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	})
	if err != nil {
		t.Fatalf("compile pinned v1 after v2 publish: %v", err)
	}
	if before.ParamValues["usableHeight"] != after.ParamValues["usableHeight"] {
		t.Fatal("published catalog change altered existing pinned object")
	}
}

func roomTemplate() TemplateSpec {
	return TemplateSpec{
		Template: things.Template{
			ID:           "room",
			Version:      "v1",
			RepeatGroups: []string{"temperature."},
			Optional:     []string{"humidity", "illuminance", "temperature.average"},
			Category:     "room",
			Relations: []things.RelationSpec{
				{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
			},
		},
	}
}

func TestTwoRoomsSameTemplate(t *testing.T) {
	c := New()
	if err := c.PutTemplate(roomTemplate()); err != nil {
		t.Fatalf("put room: %v", err)
	}
	a, err := c.Compile(ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {}, "temperature.center": {},
			"temperature.door": {}, "temperature.average": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: urnTemp, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s2", Object: urnTemp, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "s3", Object: urnTemp, Resource: "5700", Input: "temperature.door"},
		},
	})
	if err != nil {
		t.Fatalf("compile room-a: %v", err)
	}
	b, err := c.Compile(ObjectSpec{
		ThingID: "room-b", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {}, "temperature.center": {},
			"humidity": {}, "illuminance": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s4", Object: urnTemp, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s5", Object: urnTemp, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "s6", Object: urnHum, Resource: "5700", Input: "humidity"},
			{DeviceID: "s6", Object: urnLux, Resource: "5700", Input: "illuminance"},
		},
	})
	if err != nil {
		t.Fatalf("compile room-b without new template: %v", err)
	}
	if len(a.Properties) != 4 || len(b.Properties) != 4 {
		t.Fatalf("unexpected property counts: a=%d b=%d", len(a.Properties), len(b.Properties))
	}
}

func TestClosedTemplateRejects(t *testing.T) {
	c := New()
	if err := c.PutTemplate(roomTemplate()); err != nil {
		t.Fatalf("put room: %v", err)
	}
	for _, tc := range []struct {
		name  string
		props map[string]things.Property
	}{
		{"unknown", map[string]things.Property{"co2": {}}},
		{"bare-prefix", map[string]things.Property{"temperature.": {}}},
	} {
		_, err := c.Compile(ObjectSpec{
			ThingID: "room-x", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
			TemplateID: "room", TemplateVersion: "v1",
			Properties: tc.props,
		})
		if err == nil {
			t.Fatalf("%s: must be rejected", tc.name)
		}
	}
	_, err := c.Compile(ObjectSpec{
		ThingID: "room-x", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings:   []things.Binding{{Object: urnTemp, Resource: "5700", Input: "temperature.window"}},
	})
	if err == nil {
		t.Fatal("binding without device must be rejected")
	}
}

func TestVariantSingleLayerAndPin(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("put: %v", err)
	}
	bad := VariantSpec{Variant: things.Variant{
		ID: "bad", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v9",
		ParamValues: map[string]float64{"usableHeight": 1},
	}}
	if err := c.PutVariant(bad); err == nil {
		t.Fatal("variant pinning unknown template version must fail")
	}
	unknown := VariantSpec{Variant: things.Variant{
		ID: "bad2", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"noSuchParam": 1},
	}}
	if err := c.PutVariant(unknown); err == nil {
		t.Fatal("variant with unknown param must fail")
	}
}

func TestUpgradePreviewKeepsOverridesVisible(t *testing.T) {
	c := New()
	seedBins(t, c)
	obj := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "milesight:214", Object: urnDistance, Resource: "5700", Input: "distance"},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	}
	v2 := wastebinTemplate()
	v2.Template.Version = "v2"
	delete(v2.ParamDefaults, "sensorToBottom")
	v2.Overridable = nil
	v2.Recipes[0].Version = "v2"
	v2.Recipes[0].Params = nil
	if err := c.PutTemplate(v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	diff, conflicts, err := c.PreviewUpgrade(obj, "wastebin", "v2", "", "")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	joined := strings.Join(conflicts, "\n")
	if !strings.Contains(joined, "sensorToBottom") {
		t.Fatalf("dropped override must be a visible conflict, got %v", conflicts)
	}
	found := false
	for _, r := range diff.ChangedRecipes {
		if strings.Contains(r, "wastebin-level") {
			found = true
		}
	}
	if !found {
		t.Fatalf("recipe change must appear in diff, got %+v", diff)
	}
}

func TestRelationsValidatedAgainstTemplate(t *testing.T) {
	c := New()
	building := TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1",
		Category: "building",
		Relations: []things.RelationSpec{
			{Name: "rooms", AllowedTargets: []string{"room"}, Min: 0, Max: 10},
		},
	}}
	if err := c.PutTemplate(building); err != nil {
		t.Fatalf("put building: %v", err)
	}
	if err := c.PutTemplate(roomTemplate()); err != nil {
		t.Fatalf("put room: %v", err)
	}
	_, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations: []RelationRef{
			{Name: "rooms", TargetThingID: "room-a", TargetTemplateID: "room"},
			{Name: "rooms", TargetThingID: "room-b", TargetTemplateID: "bench"},
		},
	})
	if err == nil {
		t.Fatal("disallowed relation target must be rejected")
	}
	cfg, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations: []RelationRef{
			{Name: "rooms", TargetThingID: "room-a", TargetTemplateID: "room"},
		},
	})
	if err != nil {
		t.Fatalf("valid relation rejected: %v", err)
	}
	if len(cfg.Relations) != 1 || cfg.Relations[0].TargetThingID != "room-a" {
		t.Fatalf("unexpected relations: %+v", cfg.Relations)
	}
}

func TestStoredVersionsAreCopies(t *testing.T) {
	c := New()
	spec := wastebinTemplate()
	if err := c.PutTemplate(spec); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Mutating the caller's maps/slices after publish must not leak in.
	spec.ParamDefaults["usableHeight"] = 999
	spec.Recipes[0].Inputs[0] = "mutated"
	got, ok := c.GetTemplate("wastebin", "v1")
	if !ok {
		t.Fatal("missing template")
	}
	if got.ParamDefaults["usableHeight"] == 999 {
		t.Fatal("PutTemplate stored shared ParamDefaults map")
	}
	if got.Recipes[0].Inputs[0] == "mutated" {
		t.Fatal("PutTemplate stored shared recipe slice")
	}
	// Mutating a returned value must not corrupt the catalog.
	got.ParamDefaults["usableHeight"] = -1
	got.Recipes[0].Inputs[0] = "evil"
	again, _ := c.GetTemplate("wastebin", "v1")
	if again.ParamDefaults["usableHeight"] == -1 || again.Recipes[0].Inputs[0] == "evil" {
		t.Fatal("GetTemplate returned shared state")
	}
	// Compiled recipes must be deep copies as well.
	cfg, err := c.Compile(ObjectSpec{
		ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cfg.Recipes[0].Inputs[0] = "evil"
	cfg.ParamValues["usableHeight"] = -5
	after, _ := c.GetTemplate("wastebin", "v1")
	if after.Recipes[0].Inputs[0] == "evil" {
		t.Fatal("Compile exposed shared recipe inputs")
	}
	cfg2, err := c.Compile(ObjectSpec{
		ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
	})
	if err != nil {
		t.Fatalf("recompile: %v", err)
	}
	if cfg2.Recipes[0].Inputs[0] == "evil" || cfg2.ParamValues["usableHeight"] == -5 {
		t.Fatal("Compile result mutation leaked into catalog")
	}
}

func TestValidationErrorsWrap(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("put: %v", err)
	}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"slash-template-id", func() error {
			bad := wastebinTemplate()
			bad.Template.ID = "a/b"
			return c.PutTemplate(bad)
		}},
		{"empty-template-version", func() error {
			bad := wastebinTemplate()
			bad.Template.Version = ""
			return c.PutTemplate(bad)
		}},
		{"recipe-no-inputs", func() error {
			bad := wastebinTemplate()
			bad.Template.Version = "bad-io"
			bad.Recipes[0].Inputs = nil
			return c.PutTemplate(bad)
		}},
		{"recipe-unknown-param", func() error {
			bad := wastebinTemplate()
			bad.Template.Version = "bad-param"
			bad.Recipes[0].Params = []string{"noSuch"}
			return c.PutTemplate(bad)
		}},
		{"variant-preset-unknown", func() error {
			return c.PutVariant(VariantSpec{Variant: things.Variant{
				ID: "vps", Version: "v1",
				TemplateID: "wastebin", TemplateVersion: "v1",
			}, PresetProperties: []string{"nope"}})
		}},
		{"compile-unknown-prop", func() error {
			_, err := c.Compile(ObjectSpec{
				ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
				TemplateID: "wastebin", TemplateVersion: "v1",
				Properties: map[string]things.Property{"nope": {}},
			})
			return err
		}},
		{"compile-bad-binding", func() error {
			_, err := c.Compile(ObjectSpec{
				ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
				TemplateID: "wastebin", TemplateVersion: "v1",
				Properties: map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
				Bindings:   []things.Binding{{DeviceID: "", Object: urnDistance, Resource: "5700", Input: "distance"}},
			})
			return err
		}},
		{"override-unknown", func() error {
			_, err := c.Compile(ObjectSpec{
				ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
				TemplateID: "wastebin", TemplateVersion: "v1",
				Properties:     map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
				ParamOverrides: map[string]float64{"noSuch": 1},
			})
			return err
		}},
		{"override-not-overridable", func() error {
			_, err := c.Compile(ObjectSpec{
				ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
				TemplateID: "wastebin", TemplateVersion: "v1",
				Properties:     map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
				ParamOverrides: map[string]float64{"usableHeight": 2},
			})
			return err
		}},
	}
	for _, tc := range cases {
		if err := tc.fn(); err == nil {
			t.Fatalf("%s: expected error", tc.name)
		} else if !errors.Is(err, things.ErrValidation) {
			t.Fatalf("%s: error %v does not wrap ErrValidation", tc.name, err)
		}
	}
}

func TestPutTemplateRejectsDuplicates(t *testing.T) {
	c := New()
	dupProp := wastebinTemplate()
	dupProp.Template.Version = "v1"
	dupProp.Template.Optional = []string{"level"}
	if err := c.PutTemplate(dupProp); err == nil {
		t.Fatal("duplicate property across required/optional must fail")
	}
	dupRecipe := wastebinTemplate()
	dupRecipe.Template.Version = "v1"
	dupRecipe.Recipes = append(dupRecipe.Recipes, dupRecipe.Recipes[0])
	if err := c.PutTemplate(dupRecipe); err == nil {
		t.Fatal("duplicate recipe name must fail")
	}
}

func TestRelationsMinMaxSelfRef(t *testing.T) {
	c := New()
	tmpl := TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1",
		Category: "building",
		Relations: []things.RelationSpec{
			{Name: "rooms", AllowedTargets: []string{"room"}, Min: 1, Max: 1},
		},
	}}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Min violation: no relations while Min=1.
	if _, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}); err == nil {
		t.Fatal("missing required relation must fail (min)")
	}
	// Max violation: two relations while Max=1.
	if _, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations: []RelationRef{
			{Name: "rooms", TargetThingID: "room-a", TargetTemplateID: "room"},
			{Name: "rooms", TargetThingID: "room-b", TargetTemplateID: "room"},
		},
	}); err == nil {
		t.Fatal("exceeding max must fail")
	}
	// Self reference.
	if _, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations: []RelationRef{
			{Name: "rooms", TargetThingID: "building-a", TargetTemplateID: "building"},
		},
	}); err == nil {
		t.Fatal("self reference must fail")
	}
	// Unknown slot.
	if _, err := c.Compile(ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations: []RelationRef{
			{Name: "nope", TargetThingID: "room-a", TargetTemplateID: "room"},
		},
	}); err == nil {
		t.Fatal("unknown relation slot must fail")
	}
}

func TestPreviewFlagsNonOverridableAndRecipeIO(t *testing.T) {
	c := New()
	seedBins(t, c)
	obj := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties:     map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
		ParamOverrides: map[string]float64{"sensorToBottom": 1.42},
	}
	// v2 keeps the param but makes it non-overridable: thing override must conflict.
	v2 := wastebinTemplate()
	v2.Template.Version = "v2"
	v2.Overridable = []string{"usableHeight"}
	if err := c.PutTemplate(v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	_, conflicts, err := c.PreviewUpgrade(obj, "wastebin", "v2", "", "")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if joined := strings.Join(conflicts, "\n"); !strings.Contains(joined, "sensorToBottom") {
		t.Fatalf("non-overridable override must conflict, got %v", conflicts)
	}
	// Recipe output-only change must appear in diff.
	v3 := wastebinTemplate()
	v3.Template.Version = "v3"
	v3.Recipes[0].Outputs = []string{"level", "fillRate"}
	if err := c.PutTemplate(v3); err != nil {
		t.Fatalf("put v3: %v", err)
	}
	diff, _, err := c.PreviewUpgrade(obj, "wastebin", "v3", "", "")
	if err != nil {
		t.Fatalf("preview v3: %v", err)
	}
	found := false
	for _, r := range diff.ChangedRecipes {
		if strings.Contains(r, "wastebin-level") {
			found = true
		}
	}
	if !found {
		t.Fatalf("recipe output change must appear in diff, got %+v", diff)
	}
}

func TestPreviewVariantPinMismatch(t *testing.T) {
	c := New()
	seedBins(t, c)
	v2 := wastebinTemplate()
	v2.Template.Version = "v2"
	v2.Recipes[0].Params = nil
	delete(v2.ParamDefaults, "sensorToBottom")
	v2.Overridable = nil
	if err := c.PutTemplate(v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	obj := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
	}
	// Variant B pins v1 but upgrade pins v2: must be an error, not silent.
	if _, _, err := c.PreviewUpgrade(obj, "wastebin", "v2", "wastebin-B", "v1"); err == nil {
		t.Fatal("variant pin mismatch must fail")
	} else if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("mismatch error must wrap ErrValidation, got %v", err)
	}
}

func TestPreviewRelationConflicts(t *testing.T) {
	c := New()
	v1 := TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1",
		Category:  "building",
		Relations: []things.RelationSpec{{Name: "rooms", AllowedTargets: []string{"room"}, Min: 0, Max: 10}},
	}}
	v2 := TemplateSpec{Template: things.Template{
		ID: "building", Version: "v2",
		Category:  "building",
		Relations: []things.RelationSpec{{Name: "rooms", AllowedTargets: []string{"office"}, Min: 0, Max: 10}},
	}}
	if err := c.PutTemplate(v1); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if err := c.PutTemplate(v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	obj := ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
		Relations:  []RelationRef{{Name: "rooms", TargetThingID: "room-a", TargetTemplateID: "room"}},
	}
	_, conflicts, err := c.PreviewUpgrade(obj, "building", "v2", "", "")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if joined := strings.Join(conflicts, "\n"); !strings.Contains(joined, "room") {
		t.Fatalf("tightened relation target must conflict, got %v", conflicts)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("put: %v", err)
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = c.GetTemplate("wastebin", "v1")
			_, _ = c.Compile(ObjectSpec{
				ThingID: "bin-1", Tenant: "t", Name: "Test", Location: testLocation(), Metadata: testMetadata(),
				TemplateID: "wastebin", TemplateVersion: "v1",
				Properties: map[string]things.Property{"distance": {}, "level": {}, "fillRate": {}},
			})
		}(i)
	}
	wg.Wait()
}

func TestRestore(t *testing.T) {
	src := New()
	if err := src.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	dst := New()
	if _, ok := dst.GetTemplate("wastebin", "v1"); ok {
		t.Fatal("dst must start empty")
	}
	dst.Restore(src.ListTemplates(), src.ListVariants())
	got, ok := dst.GetTemplate("wastebin", "v1")
	if !ok {
		t.Fatal("restored template missing")
	}
	if len(got.Recipes) != len(wastebinTemplate().Recipes) {
		t.Fatalf("recipes=%d, want %d", len(got.Recipes), len(wastebinTemplate().Recipes))
	}
	// Ogiltiga poster ignoreras, giltiga lastas.
	dst.Restore([]TemplateSpec{{Template: things.Template{}}}, nil)
	if _, ok := dst.GetTemplate("wastebin", "v1"); !ok {
		t.Fatal("valid entry must survive restore with invalid rows")
	}
}

func TestCompileLocationMetadataCategory(t *testing.T) {
	c := New()
	tmpl := wastebinTemplate()
	tmpl.Template.Category = "wastebin"
	tmpl.Template.Labels = []string{"Avfall"}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("publish: %v", err)
	}
	obj := ObjectSpec{
		ThingID: "bin-42", Tenant: "t",
		TemplateID: "wastebin", TemplateVersion: "v1",
		Name:     "Soptunna 42",
		Location: &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata: map[string]string{"district": "centrum"},
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	}
	cfg, err := c.Compile(obj)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cfg.Category != "wastebin" {
		t.Fatalf("category=%q, want inherited wastebin", cfg.Category)
	}
	if cfg.Location == nil || cfg.Location.Type != "Point" {
		t.Fatalf("location=%+v, want point", cfg.Location)
	}
	if cfg.Metadata["district"] != "centrum" {
		t.Fatalf("metadata=%+v", cfg.Metadata)
	}

	bad := obj
	bad.ThingID = "bad-loc"
	bad.Location = &things.Location{Type: "Point", Coordinates: json.RawMessage(`[999,0]`)}
	if _, err := c.Compile(bad); err == nil {
		t.Fatal("out-of-range location must fail")
	}
	badMeta := obj
	badMeta.ThingID = "bad-meta"
	badMeta.Location = nil
	badMeta.Metadata = map[string]string{"": "x"}
	if _, err := c.Compile(badMeta); err == nil {
		t.Fatal("empty metadata key must fail")
	}
	override := obj
	override.ThingID = "bin-43"
	override.Category = "custom"
	cfg2, err := c.Compile(override)
	if err != nil {
		t.Fatalf("compile override: %v", err)
	}
	if cfg2.Category != "custom" {
		t.Fatalf("category=%q, want custom", cfg2.Category)
	}
}

func TestPutTemplatePrimaryProperty(t *testing.T) {
	c := New()
	tmpl := wastebinTemplate()
	tmpl.Template.PrimaryProperty = "nope"
	if err := c.PutTemplate(tmpl); err == nil {
		t.Fatal("unknown primary property must fail")
	}
	tmpl.Template.PrimaryProperty = "level"
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("valid primary property: %v", err)
	}
}

// Startkatalogen: seed-filen parsar och varje mall publiceras utan fel.
func TestSeedFileParsesAndPublishes(t *testing.T) {
	raw, err := os.ReadFile("../../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	if len(seed.Templates) != 27 {
		t.Fatalf("templates=%d, want 27 (14 publicerade + 13 nya versioner med partOf)", len(seed.Templates))
	}
	c := New()
	for _, spec := range seed.Templates {
		if err := c.PutTemplate(spec); err != nil {
			t.Fatalf("publish %s/%s: %v", spec.Template.ID, spec.Template.Version, err)
		}
	}
	// Alla nya versioner bär en öppen partOf-slot (valfri, max en förälder).
	latest := map[string]string{}
	for _, spec := range seed.Templates {
		if spec.Template.Version > latest[spec.Template.ID] {
			latest[spec.Template.ID] = spec.Template.Version
		}
	}
	for id, version := range latest {
		spec, ok := c.GetTemplate(id, version)
		if !ok {
			t.Fatalf("missing %s/%s", id, version)
		}
		found := false
		for _, rel := range spec.Template.Relations {
			if rel.Name == "partOf" && len(rel.AllowedTargets) == 0 && rel.Min == 0 && rel.Max == 1 {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s/%s saknar öppen partOf-slot", id, version)
		}
	}
	for _, spec := range seed.Variants {
		if err := c.PutVariant(spec); err != nil {
			t.Fatalf("publish variant %s/%s: %v", spec.Variant.ID, spec.Variant.Version, err)
		}
	}
	if got := c.ListTemplates(); len(got) != 27 {
		t.Fatalf("listed=%d, want 27", len(got))
	}
	if got := c.ListVariants(); len(got) != 12 {
		t.Fatalf("variants=%d, want 12 (6 v1 + 6 v2)", len(got))
	}
}

// Gräns: seed-filen får aldrig innehålla saker (objects); de skapas via API.
func TestSeedFileRejectsObjects(t *testing.T) {
	if _, err := ParseSeed([]byte(`{"templates": [], "objects": [{"thingId": "x"}]}`)); err == nil {
		t.Fatal("objects in seed must fail")
	}
	if _, err := ParseSeed([]byte(`{"templates": []}`)); err != nil {
		t.Fatalf("templates-only seed: %v", err)
	}
}

// Alla saker kräver namn, plats och kategori (ärvd eller egen).
// Metadata, bindningar och relationer är frivilliga.
func TestCompileRequiresDescriptiveFields(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	base := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	}
	if _, err := c.Compile(base); err != nil {
		t.Fatalf("valid object: %v", err)
	}
	cases := map[string]func(*ObjectSpec){
		"namnlös":  func(o *ObjectSpec) { o.Name = "  " },
		"platslös": func(o *ObjectSpec) { o.Location = nil },
	}
	for name, mutate := range cases {
		obj := base
		mutate(&obj)
		if _, err := c.Compile(obj); err == nil {
			t.Fatalf("%s must fail", name)
		}
	}

	// Kategori vare sig på sak eller mall nekas.
	bare := TemplateSpec{Template: things.Template{
		ID: "bare", Version: "v1", Required: []string{"p"},
	}}
	if err := c.PutTemplate(bare); err != nil {
		t.Fatalf("publish bare: %v", err)
	}
	obj := base
	obj.ThingID = "bare-1"
	obj.TemplateID = "bare"
	obj.Properties = map[string]things.Property{"p": {}}
	if _, err := c.Compile(obj); err == nil {
		t.Fatal("missing category must fail")
	}
	obj.Category = "custom"
	if _, err := c.Compile(obj); err != nil {
		t.Fatalf("explicit category: %v", err)
	}
}

func ptr(v float64) *float64 { return &v }

// Egenskapsdefinitioner: okänd egenskap nekas; prefill fyller tomma fält
// men skriver aldrig över uttryckliga värden.
func TestPropertyDefs(t *testing.T) {
	c := New()
	tmpl := wastebinTemplate()
	tmpl.Template.PropertyDefs = map[string]things.PropertyDef{
		"distance": {DisplayName: "Avstånd", Unit: "m", Signals: []things.SignalHint{
			{Object: urnDistance, Resource: "5700"},
		}},
		"nope": {DisplayName: "Ogiltig"},
	}
	if err := c.PutTemplate(tmpl); err == nil {
		t.Fatal("def for unknown property must fail")
	}
	delete(tmpl.Template.PropertyDefs, "nope")
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put with defs: %v", err)
	}
	cfg, err := c.Compile(ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata:   map[string]string{"test": "true"},
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {},
			"level":    {DisplayName: "Egen nivå", Unit: "cm"},
			"fillRate": {},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if got := cfg.Properties["distance"]; got.DisplayName != "Avstånd" || got.Unit != "m" {
		t.Fatalf("prefill=%+v, want Avstånd/m", got)
	}
	if got := cfg.Properties["level"]; got.DisplayName != "Egen nivå" || got.Unit != "cm" {
		t.Fatalf("explicit=%+v, must win over def", got)
	}
}

// Parameterinfo: okänd nyckel, min>max och default utanför intervall nekas
// vid publicering; variant- och objektsvärden utanför nekas vid kompilering.
func TestParamInfoRanges(t *testing.T) {
	mk := func() TemplateSpec {
		tmpl := wastebinTemplate()
		tmpl.Overridable = []string{"usableHeight", "sensorToBottom"}
		tmpl.ParamInfo = map[string]ParamDef{
			"usableHeight":   {Unit: "m", Description: "Användbar höjd", Min: ptr(0.05), Max: ptr(10)},
			"sensorToBottom": {Unit: "m", Description: "Avstånd givare–tom botten", Min: ptr(0.05), Max: ptr(10)},
		}
		return tmpl
	}
	bad := mk()
	bad.ParamInfo["unknown"] = ParamDef{}
	c := New()
	if err := c.PutTemplate(bad); err == nil {
		t.Fatal("info for unknown param must fail")
	}
	bad = mk()
	bad.ParamInfo["usableHeight"] = ParamDef{Min: ptr(5), Max: ptr(1)}
	if err := c.PutTemplate(bad); err == nil {
		t.Fatal("min>max must fail")
	}
	bad = mk()
	bad.ParamDefaults["usableHeight"] = 99
	if err := c.PutTemplate(bad); err == nil {
		t.Fatal("default out of range must fail")
	}
	if err := c.PutTemplate(mk()); err != nil {
		t.Fatalf("valid info: %v", err)
	}
	// Variant utanför intervall.
	if err := c.PutVariant(VariantSpec{Variant: things.Variant{
		ID: "too-tall", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 50},
	}}); err == nil {
		t.Fatal("variant out of range must fail")
	}
	// Override utanför intervall.
	obj := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata:   map[string]string{"test": "true"},
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		ParamOverrides: map[string]float64{"sensorToBottom": 99},
	}
	if _, err := c.Compile(obj); err == nil {
		t.Fatal("override above max must fail")
	}
	obj.ParamOverrides = map[string]float64{"usableHeight": 0.01}
	if _, err := c.Compile(obj); err == nil {
		t.Fatal("override below min must fail")
	}
	obj.ParamOverrides = map[string]float64{"usableHeight": 1.2}
	if _, err := c.Compile(obj); err != nil {
		t.Fatalf("override in range: %v", err)
	}
}
