// Package things defines the core domain types for iot-things-v2.
//
// The model follows THNGS_NEW.md chapters 0-3: Thing (sak) and Property
// are normative names in code, stream identity is
// device/[channel]/object/resource where an empty channel in a binding
// means all channels, templates are closed definitions of allowed
// properties, and parameter precedence is template -> variant -> thing.
package things

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Quality is the strict observation quality enum.
type Quality string

const (
	QualityOK        Quality = "ok"
	QualityUncertain Quality = "uncertain"
	QualityInvalid   Quality = "invalid"
)

// Signal identifies one stream source: device/[channel]/object/resource.
// Object is a URN such as "urn:oma:lwm2m:ext:3303" and Resource such as
// "5700". Channel is usually empty; several sensors of the same type on
// the same device are distinguished by channel.
type Signal struct {
	DeviceID string
	Channel  string
	Object   string
	Resource string
}

// shortObject returns the last ":" segment of an object URN,
// e.g. "3303" for "urn:oma:lwm2m:ext:3303".
func shortObject(objectURN string) string {
	if i := strings.LastIndex(objectURN, ":"); i >= 0 {
		return objectURN[i+1:]
	}
	return objectURN
}

// ID returns the stream identity device/[channel]/object/resource,
// e.g. "device-1/3303/5700" without channel or
// "device-1/0/3303/5700" with channel "0".
func (s Signal) ID() string {
	short := shortObject(s.Object)
	if s.Channel == "" {
		return s.DeviceID + "/" + short + "/" + s.Resource
	}
	return s.DeviceID + "/" + s.Channel + "/" + short + "/" + s.Resource
}

// ParseSignalID builds a Signal from its parts.
func ParseSignalID(device, channel, objectURN, resource string) Signal {
	return Signal{
		DeviceID: device,
		Channel:  channel,
		Object:   objectURN,
		Resource: resource,
	}
}

// Binding couples a specific stream to a property or processor input.
// An empty Channel matches all channels (wildcard); all other fields
// match exactly. Input is the stable property ID,
// e.g. "temperature.window".
type Binding struct {
	DeviceID string
	Channel  string
	Object   string
	Resource string
	Input    string
}

// Matches reports whether the binding selects signal s.
func (b Binding) Matches(s Signal) bool {
	if b.DeviceID != s.DeviceID {
		return false
	}
	if b.Channel != "" && b.Channel != s.Channel {
		return false
	}
	if b.Object != s.Object {
		return false
	}
	if b.Resource != s.Resource {
		return false
	}
	return true
}

// Property is a named property of a thing with a stable identity.
// ID is stable (e.g. "temperature.window"); DisplayName is a separate
// mutable field and never breaks references or history.
type Property struct {
	ID          string
	DisplayName string
	DataType    string // "number", "boolean" or "string"
	Quantity    string
	Unit        string
	Description string
}

// Observation is one typed value with measurement time, reception time,
// identity, quality and origin.
type Observation struct {
	Signal     Signal
	Value      *float64
	BoolValue  *bool
	Unit       string
	ObservedAt time.Time
	ReceivedAt time.Time
	Quality    Quality
	Source     string
	MessageID  string
}

// RelationSpec describes one allowed named relation of a template.
type RelationSpec struct {
	Name           string
	AllowedTargets []string
	Min            int
	Max            int
}

// Template is a versioned closed definition of allowed properties.
// Required and Optional hold exact property IDs. RepeatGroups holds
// repeatable measurement-point prefixes such as "temperature.".
type Template struct {
	ID                string
	Version           string
	Required          []string
	Optional          []string
	RepeatGroups      []string
	AllowedQuantities []string
	Relations         []RelationSpec
}

// ValidatePropertyID reports whether id is allowed by the closed
// template: it must be listed in Required/Optional or match a
// RepeatGroups prefix such as "temperature." with a non-empty suffix
// (the bare prefix itself, e.g. "temperature.", is not a measurement
// point and is rejected).
func (t Template) ValidatePropertyID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty property id", ErrValidation)
	}
	for _, r := range t.Required {
		if id == r {
			return nil
		}
	}
	for _, o := range t.Optional {
		if id == o {
			return nil
		}
	}
	for _, prefix := range t.RepeatGroups {
		if prefix != "" && len(id) > len(prefix) && strings.HasPrefix(id, prefix) {
			return nil
		}
	}
	return fmt.Errorf("%w: property %q not allowed by template %s/%s", ErrValidation, id, t.ID, t.Version)
}

// Variant is a named, versioned configuration of exactly one template
// version, mainly parameter values.
type Variant struct {
	ID              string
	Version         string
	TemplateID      string
	TemplateVersion string
	ParamValues     map[string]float64
}

// ParamSource describes the origin of an effective parameter value.
type ParamSource string

const (
	ParamSourceTemplate ParamSource = "template"
	ParamSourceVariant  ParamSource = "variant"
	ParamSourceThing    ParamSource = "thing"
)

// Thing is a persistent business object (sak) created from a template.
// Both version references and the effective configuration are stored so
// the configuration can be reproduced without reading a later template
// version.
type Thing struct {
	ID              string
	Tenant          string
	Name            string
	TemplateID      string
	TemplateVersion string
	VariantID       string
	VariantVersion  string
	Revision        int64
	Properties      map[string]Property
	Bindings        []Binding
	ParamValues     map[string]float64
	ParamSources    map[string]ParamSource
}

// EffectiveParam resolves key with precedence
// template default -> variant value -> thing override,
// reporting the effective value together with its origin.
func (t Thing) EffectiveParam(key string, tmplDefaults map[string]float64, variant Variant) (float64, ParamSource, bool) {
	if t.ParamValues != nil {
		if v, ok := t.ParamValues[key]; ok {
			return v, ParamSourceThing, true
		}
	}
	if variant.ParamValues != nil {
		if v, ok := variant.ParamValues[key]; ok {
			return v, ParamSourceVariant, true
		}
	}
	if tmplDefaults != nil {
		if v, ok := tmplDefaults[key]; ok {
			return v, ParamSourceTemplate, true
		}
	}
	return 0, "", false
}

// Relation is a persistent, typed, directed link between independent
// things within the same tenant.
type Relation struct {
	ID     string
	Tenant string
	Type   string
	FromID string
	ToID   string
}

var (
	// ErrValidation signals that input violates domain rules.
	ErrValidation = errors.New("validation error")
	// ErrConflict signals optimistic-locking or uniqueness conflicts.
	ErrConflict = errors.New("conflict")
	// ErrNotFound signals a missing entity.
	ErrNotFound = errors.New("not found")
)

// ValidateThingID validates a thing identity: non-empty and without "/".
func ValidateThingID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: thing id is empty", ErrValidation)
	}
	if strings.Contains(id, "/") {
		return fmt.Errorf("%w: thing id %q must not contain %q", ErrValidation, id, "/")
	}
	return nil
}

// ValidateBindingForTemplate validates a binding against a template:
// Input must be allowed via ValidatePropertyID and Device/Object/Resource
// must be non-empty (a binding selects one specific stream).
func ValidateBindingForTemplate(tmpl Template, b Binding) error {
	if err := tmpl.ValidatePropertyID(b.Input); err != nil {
		return fmt.Errorf("binding input %q: %w", b.Input, err)
	}
	if b.DeviceID == "" {
		return fmt.Errorf("%w: binding device must not be empty", ErrValidation)
	}
	if b.Object == "" {
		return fmt.Errorf("%w: binding object must not be empty", ErrValidation)
	}
	if b.Resource == "" {
		return fmt.Errorf("%w: binding resource must not be empty", ErrValidation)
	}
	return nil
}

// ValidateNoSelfRef denies self references in relations.
func ValidateNoSelfRef(rel Relation) error {
	if rel.FromID != "" && rel.FromID == rel.ToID {
		return fmt.Errorf("%w: relation %q must not reference itself", ErrValidation, rel.ID)
	}
	return nil
}
