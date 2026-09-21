package catalog

import (
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/things"
)

// K1.1/K1.2 code review: alternativa namn/subtyp är additiva,
// explicita värden vinner, metadata är fallback, "/" i subType nekas.
func TestK1CompileAlternativeNameSubType(t *testing.T) {
	c := New()
	if err := c.PutTemplate(wastebinTemplate()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	base := ObjectSpec{
		ThingID: "bin-42", Tenant: "t", Name: "Soptunna 42",
		AlternativeName: "BinA", SubType: "WasteContainer",
		Location: testLocation(), Metadata: testMetadata(),
		TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"distance": {}, "level": {}, "fillRate": {},
		},
	}
	cfg, err := c.Compile(base)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if cfg.AlternativeName != "BinA" || cfg.SubType != "WasteContainer" {
		t.Fatalf("cfg=%+v, want BinA/WasteContainer", cfg)
	}

	// Metadata-fallback för saker materialiserade före K1.
	legacy := base
	legacy.AlternativeName, legacy.SubType = "", ""
	legacy.Metadata = map[string]string{"alternativeName": "Legacy", "subType": "WasteContainer"}
	cfg, err = c.Compile(legacy)
	if err != nil {
		t.Fatalf("legacy compile: %v", err)
	}
	if cfg.AlternativeName != "Legacy" || cfg.SubType != "WasteContainer" {
		t.Fatalf("legacy fallback=%+v", cfg)
	}

	// Explicit vinner över metadata.
	clash := base
	clash.Metadata = map[string]string{"alternativeName": "Other", "subType": "Other"}
	cfg, err = c.Compile(clash)
	if err != nil {
		t.Fatalf("clash compile: %v", err)
	}
	if cfg.AlternativeName != "BinA" || cfg.SubType != "WasteContainer" {
		t.Fatalf("explicit must win: %+v", cfg)
	}

	// "/" i subType nekas (URN-segment).
	bad := base
	bad.SubType = "a/b"
	if _, err := c.Compile(bad); err == nil {
		t.Fatal("subType with / must fail")
	}
}
