package catalog

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

// TV-08: stora geometrier/strängar nekas vid kompilering.
func TestCompileRejectsOversizeGeometryAndName(t *testing.T) {
	c := New()
	if err := c.PutTemplate(TemplateSpec{Template: things.Template{
		ID: "t", Version: "v1", Category: "t",
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Stor polygon (över 64KB koordinater).
	bigCoords := "[" + strings.Repeat("[18.0,59.3],", 6000) + "[18.0,59.3]]"
	obj := ObjectSpec{
		ThingID: "x", Tenant: "t", Name: "n",
		Location:   &things.Location{Type: "Polygon", Coordinates: json.RawMessage(bigCoords)},
		TemplateID: "t", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}
	if _, err := c.Compile(obj); err == nil {
		t.Fatal("oversize polygon must be rejected")
	}
	// Långt namn.
	obj2 := ObjectSpec{
		ThingID: "y", Tenant: "t", Name: strings.Repeat("n", 300),
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.0,59.3]`)},
		TemplateID: "t", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}
	if _, err := c.Compile(obj2); err == nil {
		t.Fatal("oversize name must be rejected")
	}
}

// TV-08: äldre mallar över recept-/parametertak nekas vid kompilering,
// inte bara vid publicering.
func TestCompileRejectsLegacyOverCapTemplate(t *testing.T) {
	c := New()
	recipes := make([]Recipe, 0, 33)
	for i := range 33 {
		recipes = append(recipes, Recipe{
			Name:     "r" + strings.Repeat("x", 3) + string(rune('a'+i%26)),
			Operator: "noop", Inputs: []string{"p"}, Outputs: []string{"q"},
		})
	}
	spec := TemplateSpec{
		Template: things.Template{ID: "legacy", Version: "v1", Category: "c", Required: []string{"p"}, Optional: []string{"q"}},
		Recipes:  recipes,
	}
	c.mu.Lock()
	if c.templates == nil {
		c.templates = make(map[string]TemplateSpec)
	}
	c.templates[versionKey("legacy", "v1")] = spec
	c.mu.Unlock()

	obj := ObjectSpec{
		ThingID: "z", Tenant: "t", Name: "n",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.0,59.3]`)},
		TemplateID: "legacy", TemplateVersion: "v1",
		Properties: map[string]things.Property{"p": {}},
	}
	if _, err := c.Compile(obj); err == nil {
		t.Fatal("legacy template over recipe cap must be rejected at compile")
	}
}
