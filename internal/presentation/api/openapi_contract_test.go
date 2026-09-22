package api

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application"
	"gopkg.in/yaml.v3"
)

// TV-06: PUT för sensorkopplingar ska dokumenteras under
// /things/{id}/bindings (där koden har den), inte under
// /things/{id}/relations. Jämför dokumenterade metoder/sökvägar med
// registrerade routes så att GUI/genererade klienter får rätt endpoint.
func TestOpenAPIBindingsPutContract(t *testing.T) {
	raw, err := os.ReadFile("../../../assets/docs/openapi.yaml")
	if err != nil {
		t.Fatalf("openapi read: %v", err)
	}
	var doc struct {
		Paths      map[string]map[string]any `yaml:"paths"`
		Components struct {
			Schemas map[string]struct {
				Required   []string       `yaml:"required"`
				Properties map[string]any `yaml:"properties"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("openapi parse: %v", err)
	}

	bindings, ok := doc.Paths["/things/{id}/bindings"]
	if !ok {
		t.Fatal("openapi must document /things/{id}/bindings")
	}
	if _, ok := bindings["get"]; !ok {
		t.Fatal("bindings must document GET")
	}
	if _, ok := bindings["put"]; !ok {
		t.Fatal("bindings must document PUT (swapBindings lives there)")
	}
	relations, ok := doc.Paths["/things/{id}/relations"]
	if !ok {
		t.Fatal("openapi must document /things/{id}/relations")
	}
	if _, ok := relations["get"]; !ok {
		t.Fatal("relations must document GET")
	}
	if _, ok := relations["put"]; ok {
		t.Fatal("relations must NOT document PUT (bindings PUT was misplaced here)")
	}

	resolve, ok := doc.Components.Schemas["RelationResolve"]
	if !ok {
		t.Fatal("openapi must define RelationResolve")
	}
	need := map[string]bool{"name": false, "targetThingId": false, "target": false}
	for _, r := range resolve.Required {
		need[r] = true
	}
	for k, v := range need {
		if !v {
			t.Fatalf("RelationResolve.required must include %q (nullable but present)", k)
		}
	}
	if _, ok := resolve.Properties["target"]; !ok {
		t.Fatal("RelationResolve must define target property")
	}

	// Kodens routes ska matcha docs: PUT bindings registrerad,
	// PUT relations inte. Jämför registrerade routes (register.go) med
	// dokumenterade metoder så att felet aldrig återinförs.
	code, err := os.ReadFile("register.go")
	if err != nil {
		t.Fatalf("read register.go: %v", err)
	}
	if !strings.Contains(string(code), `r.Put("{id}/bindings"`) {
		t.Fatal("code must register PUT {id}/bindings (swapBindings)")
	}
	if strings.Contains(string(code), `r.Put("{id}/relations"`) {
		t.Fatal("code must NOT register PUT {id}/relations (was misplaced bindings PUT)")
	}
}

// TV-06 andra halvan: target ska vara obligatoriskt men nullable —
// saknat mål ger "target":null, aldrig saknad nyckel.
func TestRelationResolveTargetAlwaysPresent(t *testing.T) {
	res := application.RelationResolve{Name: "partOf", TargetThingID: "missing"}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	v, ok := m["target"]
	if !ok {
		t.Fatalf("target key must always be present, got %s", string(raw))
	}
	if v != nil {
		t.Fatalf("nil target must encode as null, got %s", string(raw))
	}
}
