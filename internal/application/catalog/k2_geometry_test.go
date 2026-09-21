package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

// K2: nivåoperatorn (distanceToLevel/level v1) stödjer endast
// vertical-down-geometri. Vinkelparametrar och extra receptparametrar
// nekas vid publicering/kompilering i stället för att tyst ignoreras.
func TestK2PublishRejectsAngle(t *testing.T) {
	spec := wastebinTemplate()
	spec.ParamDefaults["angle"] = 15.0
	err := New().PutTemplate(spec)
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("angle with level recipe must fail validation, got %v", err)
	}
	if !strings.Contains(err.Error(), "vertical-down") {
		t.Fatalf("error should name the supported geometry, got %q", err)
	}
}

func TestK2PublishRejectsExtraLevelParams(t *testing.T) {
	spec := wastebinTemplate()
	spec.ParamDefaults["usableHeight"] = 1.0
	spec.Recipes[0].Params = []string{"sensorToBottom", "usableHeight"}
	err := New().PutTemplate(spec)
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("level recipe with 2 params must fail validation, got %v", err)
	}
}

func TestK2PublishAcceptsVerticalDown(t *testing.T) {
	if err := New().PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("vertical-down template must publish, got %v", err)
	}
}

// Mallar publicerade före K2 kan bära angle i beständig katalog;
// kompilering nekar dem på alla objektskrivvägar.
func TestK2CompileRejectsPreK2AngleTemplate(t *testing.T) {
	c := New()
	spec := wastebinTemplate()
	spec.ParamDefaults["angle"] = 15.0
	c.templates[versionKey("wastebin", "v1")] = spec
	_, err := c.Compile(ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	})
	if !errors.Is(err, things.ErrValidation) {
		t.Fatalf("compile of pre-K2 angle template must fail validation, got %v", err)
	}
}
