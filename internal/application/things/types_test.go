package things

import (
	"encoding/json"
	"errors"
	"testing"
)

// B1: closed template denies unknown properties.
func TestClosedTemplateRejectsUnknownProperty(t *testing.T) {
	tmpl := Template{
		ID:           "room",
		Version:      "v1",
		Required:     []string{"temperature.window"},
		Optional:     []string{"humidity"},
		RepeatGroups: []string{"temperature."},
	}

	for _, id := range []string{"temperature.window", "humidity", "temperature.door"} {
		if err := tmpl.ValidatePropertyID(id); err != nil {
			t.Fatalf("ValidatePropertyID(%q) = %v, want nil", id, err)
		}
	}

	if err := tmpl.ValidatePropertyID("co2"); err == nil {
		t.Fatalf("ValidatePropertyID(co2) = nil, want validation error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidatePropertyID(co2) error = %v, want ErrValidation", err)
	}

	bad := Binding{
		DeviceID: "sensor-1",
		Object:   "urn:oma:lwm2m:ext:3303",
		Resource: "5700",
		Input:    "co2",
	}
	if err := ValidateBindingForTemplate(tmpl, bad); err == nil {
		t.Fatalf("ValidateBindingForTemplate with unknown input = nil, want error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidateBindingForTemplate error = %v, want ErrValidation", err)
	}
}

// B2: stable measurement-point ID separate from display name.
func TestStablePropertyIDVsDisplayName(t *testing.T) {
	tmpl := Template{
		ID:       "room",
		Version:  "v1",
		Required: []string{"temperature.window"},
	}

	prop := Property{
		ID:          "temperature.window",
		DisplayName: "Lufttemperatur vid fönstret",
		DataType:    "number",
		Quantity:    "temperature",
		Unit:        "Cel",
	}

	// Renaming the display name must not change the identity.
	renamed := prop
	renamed.DisplayName = "Window"
	if renamed.ID != prop.ID {
		t.Fatalf("renamed ID = %q, want %q", renamed.ID, prop.ID)
	}

	// Template validation uses the stable ID, never the display name.
	if err := tmpl.ValidatePropertyID(prop.ID); err != nil {
		t.Fatalf("ValidatePropertyID(%q) = %v, want nil", prop.ID, err)
	}
	if err := tmpl.ValidatePropertyID(prop.DisplayName); err == nil {
		t.Fatalf("ValidatePropertyID(display name %q) = nil, want error", prop.DisplayName)
	}

	// A binding keeps pointing at the stable ID after a rename.
	b := Binding{
		DeviceID: "sensor-1",
		Object:   "urn:oma:lwm2m:ext:3303",
		Resource: "5700",
		Input:    renamed.ID,
	}
	if err := ValidateBindingForTemplate(tmpl, b); err != nil {
		t.Fatalf("ValidateBindingForTemplate after rename = %v, want nil", err)
	}
}

// B11: stream identity device/[channel]/object/resource;
// empty channel in binding means all channels.
func TestBindingWildcardChannel(t *testing.T) {
	b := Binding{
		DeviceID: "device-1",
		Channel:  "",
		Object:   "urn:oma:lwm2m:ext:3303",
		Resource: "5700",
		Input:    "temperature.window",
	}

	withChannel := ParseSignalID("device-1", "0", "urn:oma:lwm2m:ext:3303", "5700")
	withoutChannel := ParseSignalID("device-1", "", "urn:oma:lwm2m:ext:3303", "5700")

	if !b.Matches(withChannel) {
		t.Fatalf("wildcard binding should match signal with channel")
	}
	if !b.Matches(withoutChannel) {
		t.Fatalf("wildcard binding should match signal without channel")
	}

	if got, want := withoutChannel.ID(), "device-1/3303/5700"; got != want {
		t.Fatalf("Signal.ID() without channel = %q, want %q", got, want)
	}
	if got, want := withChannel.ID(), "device-1/0/3303/5700"; got != want {
		t.Fatalf("Signal.ID() with channel = %q, want %q", got, want)
	}

	// Exact channel only matches that channel.
	exact := b
	exact.Channel = "0"
	if !exact.Matches(withChannel) {
		t.Fatalf("exact channel binding should match same channel")
	}
	if exact.Matches(withoutChannel) {
		t.Fatalf("exact channel binding must not match another channel")
	}

	// Other fields are exact: other device/object/resource must not match.
	other := ParseSignalID("device-2", "0", "urn:oma:lwm2m:ext:3303", "5700")
	if b.Matches(other) {
		t.Fatalf("binding must not match another device")
	}
	other = ParseSignalID("device-1", "0", "urn:oma:lwm2m:ext:3304", "5700")
	if b.Matches(other) {
		t.Fatalf("binding must not match another object")
	}
	other = ParseSignalID("device-1", "0", "urn:oma:lwm2m:ext:3303", "5701")
	if b.Matches(other) {
		t.Fatalf("binding must not match another resource")
	}
}

// B10: thing IDs are slash-free so stream/thing addressing stays stable
// within tenant-isolated things.
func TestValidateThingIDRejectsSlash(t *testing.T) {
	if err := ValidateThingID("kontorslandskap-a"); err != nil {
		t.Fatalf("ValidateThingID(valid) = %v, want nil", err)
	}
	if err := ValidateThingID(""); err == nil {
		t.Fatalf("ValidateThingID(empty) = nil, want error")
	}
	if err := ValidateThingID("a/b"); err == nil {
		t.Fatalf("ValidateThingID(a/b) = nil, want error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidateThingID(a/b) error = %v, want ErrValidation", err)
	}
}

// B17: strict intra-tenant relations deny self references.
func TestValidateNoSelfRef(t *testing.T) {
	ok := Relation{ID: "r1", Tenant: "t1", Type: "partOf", FromID: "room-a", ToID: "building-a"}
	if err := ValidateNoSelfRef(ok); err != nil {
		t.Fatalf("ValidateNoSelfRef(distinct) = %v, want nil", err)
	}

	self := Relation{ID: "r2", Tenant: "t1", Type: "partOf", FromID: "room-a", ToID: "room-a"}
	if err := ValidateNoSelfRef(self); err == nil {
		t.Fatalf("ValidateNoSelfRef(self) = nil, want error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidateNoSelfRef(self) error = %v, want ErrValidation", err)
	}
}

// B1/T2: the bare repeat-group prefix itself is not a measurement point;
// a non-empty suffix is required.
func TestClosedTemplateRejectsBarePrefix(t *testing.T) {
	tmpl := Template{
		ID:           "room",
		Version:      "v1",
		RepeatGroups: []string{"temperature."},
	}

	if err := tmpl.ValidatePropertyID("temperature.window"); err != nil {
		t.Fatalf("ValidatePropertyID(temperature.window) = %v, want nil", err)
	}
	if err := tmpl.ValidatePropertyID("temperature."); err == nil {
		t.Fatalf("ValidatePropertyID(bare prefix) = nil, want validation error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidatePropertyID(bare prefix) error = %v, want ErrValidation", err)
	}
}

// B11: a binding selects one specific stream, so DeviceID is required.
func TestValidateBindingRequiresDevice(t *testing.T) {
	tmpl := Template{
		ID:       "room",
		Version:  "v1",
		Required: []string{"temperature.window"},
	}

	ok := Binding{
		DeviceID: "sensor-1",
		Object:   "urn:oma:lwm2m:ext:3303",
		Resource: "5700",
		Input:    "temperature.window",
	}
	if err := ValidateBindingForTemplate(tmpl, ok); err != nil {
		t.Fatalf("ValidateBindingForTemplate(valid) = %v, want nil", err)
	}

	missing := ok
	missing.DeviceID = ""
	if err := ValidateBindingForTemplate(tmpl, missing); err == nil {
		t.Fatalf("ValidateBindingForTemplate(empty device) = nil, want error")
	} else if !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidateBindingForTemplate(empty device) error = %v, want ErrValidation", err)
	}
}

// Regression: parameterprecedens template -> variant -> thing (thing vinner).
func TestEffectiveParamPrecedence(t *testing.T) {
	tmplDefaults := map[string]float64{"usableHeight": 1.0, "onlyTemplate": 9}
	variant := Variant{
		ID:              "wastebin-B",
		Version:         "v1",
		TemplateID:      "wastebin",
		TemplateVersion: "v1",
		ParamValues:     map[string]float64{"usableHeight": 1.2, "onlyVariant": 8},
	}
	thing := Thing{
		ID:          "bin-42",
		Tenant:      "t1",
		ParamValues: map[string]float64{"usableHeight": 1.5},
	}

	if v, src, ok := thing.EffectiveParam("usableHeight", tmplDefaults, variant); !ok || v != 1.5 || src != ParamSourceThing {
		t.Fatalf("thing-override = (%v, %q, %v), want (1.5, thing, true)", v, src, ok)
	}
	bare := Thing{ID: "x", Tenant: "t1"}
	if v, src, ok := bare.EffectiveParam("usableHeight", tmplDefaults, variant); !ok || v != 1.2 || src != ParamSourceVariant {
		t.Fatalf("variant = (%v, %q, %v), want (1.2, variant, true)", v, src, ok)
	}
	emptyVariant := Variant{}
	if v, src, ok := bare.EffectiveParam("usableHeight", tmplDefaults, emptyVariant); !ok || v != 1.0 || src != ParamSourceTemplate {
		t.Fatalf("template-default = (%v, %q, %v), want (1.0, template, true)", v, src, ok)
	}
	if _, _, ok := bare.EffectiveParam("missing", tmplDefaults, variant); ok {
		t.Fatal("saknad nyckel ska ge ok=false")
	}
	if v, src, ok := bare.EffectiveParam("onlyVariant", tmplDefaults, variant); !ok || v != 8 || src != ParamSourceVariant {
		t.Fatalf("onlyVariant = (%v, %q, %v), want (8, variant, true)", v, src, ok)
	}
	if v, src, ok := bare.EffectiveParam("onlyTemplate", tmplDefaults, variant); !ok || v != 9 || src != ParamSourceTemplate {
		t.Fatalf("onlyTemplate = (%v, %q, %v), want (9, template, true)", v, src, ok)
	}
}

func locPoint(coords string) Location {
	return Location{Type: "Point", Coordinates: json.RawMessage(coords)}
}

func TestLocationPoint(t *testing.T) {
	if err := locPoint(`[18.06,59.33]`).Validate(); err != nil {
		t.Fatalf("valid point: %v", err)
	}
	if err := locPoint(`[18.06,59.33,12.5]`).Validate(); err != nil {
		t.Fatalf("point with alt: %v", err)
	}
	for _, coords := range []string{`[181,0]`, `[0,91]`, `[18]`, `[18,59,1,2]`, `"x"`} {
		if err := locPoint(coords).Validate(); err == nil {
			t.Fatalf("point %s must fail", coords)
		}
	}
}

func TestLocationPolygon(t *testing.T) {
	closed := Location{Type: "Polygon", Coordinates: json.RawMessage(
		`[[[18.0,59.3],[18.1,59.3],[18.1,59.4],[18.0,59.4],[18.0,59.3]]]`)}
	if err := closed.Validate(); err != nil {
		t.Fatalf("valid polygon: %v", err)
	}
	open := Location{Type: "Polygon", Coordinates: json.RawMessage(
		`[[[18.0,59.3],[18.1,59.3],[18.1,59.4],[18.0,59.4]]]`)}
	if err := open.Validate(); err == nil {
		t.Fatal("open ring must fail")
	}
	if err := (Location{Type: "Circle", Coordinates: json.RawMessage(`[18,59]`)}).Validate(); err == nil {
		t.Fatal("unknown type must fail")
	}
}

func TestLocationIntersectsBBox(t *testing.T) {
	inside := BBox{MinLon: 17, MinLat: 59, MaxLon: 19, MaxLat: 60}
	if !locPoint(`[18.06,59.33]`).IntersectsBBox(inside) {
		t.Fatal("point inside must intersect")
	}
	outside := BBox{MinLon: 0, MinLat: 0, MaxLon: 1, MaxLat: 1}
	if locPoint(`[18.06,59.33]`).IntersectsBBox(outside) {
		t.Fatal("point outside must not intersect")
	}
	poly := Location{Type: "Polygon", Coordinates: json.RawMessage(
		`[[[18.0,59.3],[20.0,59.3],[20.0,60.0],[18.0,60.0],[18.0,59.3]]]`)}
	overlap := BBox{MinLon: 19, MinLat: 59.5, MaxLon: 21, MaxLat: 61}
	if !poly.IntersectsBBox(overlap) {
		t.Fatal("overlapping polygon must intersect")
	}
}
