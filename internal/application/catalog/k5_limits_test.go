package catalog

import (
	"errors"
	"fmt"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

// K5: ändliga tak per mall/sak. Taken ligger långt över legitimt bruk
// (seed max: 8 egenskaper, 3 recept, 2 parametrar) och nekas med 400.
func k5IDs(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	return out
}

func TestK5PublishRejectsTooManyProperties(t *testing.T) {
	mk := func(n int) TemplateSpec {
		return TemplateSpec{
			Template: things.Template{
				ID: "big", Version: "v1", Required: k5IDs("p", n), Category: "big",
			},
		}
	}
	if err := New().PutTemplate(mk(maxPropertiesPerThing + 1)); !errors.Is(err, things.ErrValidation) {
		t.Fatalf("65 properties must fail validation, got %v", err)
	}
	if err := New().PutTemplate(mk(maxPropertiesPerThing)); err != nil {
		t.Fatalf("64 properties must publish, got %v", err)
	}
}

func TestK5PublishRejectsTooManyRecipes(t *testing.T) {
	spec := TemplateSpec{
		Template: things.Template{
			ID: "many", Version: "v1", Required: []string{"p0"}, Category: "many",
		},
	}
	for i := 0; i < maxRecipesPerTemplate+1; i++ {
		spec.Recipes = append(spec.Recipes, Recipe{
			Name: fmt.Sprintf("r%02d", i), Operator: "passthrough", Version: "v1",
			Inputs: []string{"p0"}, Outputs: []string{"p0"},
		})
	}
	if err := New().PutTemplate(spec); !errors.Is(err, things.ErrValidation) {
		t.Fatalf("33 recipes must fail validation, got %v", err)
	}
}

func TestK5PublishRejectsTooManyParams(t *testing.T) {
	spec := TemplateSpec{
		Template: things.Template{
			ID: "par", Version: "v1", Required: []string{"p0"}, Category: "par",
		},
		ParamDefaults: map[string]float64{},
	}
	for i := 0; i < maxParamsPerTemplate+1; i++ {
		spec.ParamDefaults[fmt.Sprintf("par%02d", i)] = float64(i)
	}
	if err := New().PutTemplate(spec); !errors.Is(err, things.ErrValidation) {
		t.Fatalf("65 params must fail validation, got %v", err)
	}
}

func TestK5CompileRejectsTooManyBindings(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	bindings := make([]things.Binding, maxBindingsPerThing+1)
	for i := range bindings {
		bindings[i] = things.Binding{DeviceID: "s", Object: urnDistance, Resource: "5700", Input: "distance"}
	}
	_, err := c.Compile(ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Bindings: bindings,
	})
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("65 bindings must fail validation, got %v", err)
	}
}

func TestK5CompileRejectsTooManyRelations(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	rels := make([]RelationRef, maxRelationsPerThing+1)
	for i := range rels {
		rels[i] = RelationRef{Name: "partOf", TargetThingID: fmt.Sprintf("parent-%d", i)}
	}
	_, err := c.Compile(ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
		Relations: rels,
	})
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("17 relations must fail validation, got %v", err)
	}
}

func TestK5CompileRejectsTooManyProperties(t *testing.T) {
	c := New()
	ids := k5IDs("p", maxPropertiesPerThing+1)
	props := make(map[string]things.Property, len(ids))
	for _, id := range ids {
		props[id] = things.Property{}
	}
	// Pre-K5-mall förbi publicering: kompilering nekar ändå.
	c.templates[versionKey("big", "v1")] = TemplateSpec{
		Template: things.Template{
			ID: "big", Version: "v1", Required: ids, Category: "big",
		},
	}
	_, err := c.Compile(ObjectSpec{
		ThingID: "big-1", Tenant: "t", Name: "Stor",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "big", TemplateVersion: "v1",
		Properties: props,
	})
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("65 properties must fail validation, got %v", err)
	}
}
