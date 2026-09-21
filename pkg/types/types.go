package types

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

// TopicThingValues carries versioned thing property values.
const TopicThingValues = "things.v1.values"

// TopicRelationChanges carries versioned thing relation changes.
const TopicRelationChanges = "things.v1.relations"

// TopicLifecycleChanges carries thing lifecycle events (created/updated/deleted).
const TopicLifecycleChanges = "things.v1.lifecycle"

// SchemaV1 is the payload schema version for the v1 topics.
// Old topics/payloads (e.g. thing.updated) are left untouched (B20).
const SchemaV1 = "v1"

// ContentTypeV1 is the versioned payload content type shared by v1 topics.
const ContentTypeV1 = "application/vnd.diwise.things.v1+json"

// GeoLocationV1 is a GeoJSON Point or Polygon (lon/lat, WGS84),
// mirroring the thing location for consumers without API access.
type GeoLocationV1 struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// ThingDescriptorV1 describes the thing an event belongs to, valid for the
// event's config revision. It carries what consumers need without an API
// join: identity for the entity URN, category for rule matching, location
// for GeoProperties and metadata for the remaining attributes.
//
// K1 schema: thingId/tenant/configRevision are required; name, category,
// alternativeName, subType, location, metadata, variantId/variantVersion
// are optional and additive (omitempty). AlternativeName feeds NameOrID
// (alternativeName > name > thingId); SubType feeds TypeName
// (subType > thingType). Description/tags stay in metadata, never as new
// top-level fields in v1.
type ThingDescriptorV1 struct {
	ThingID         string            `json:"thingId"`
	Name            string            `json:"name,omitempty"`
	AlternativeName string            `json:"alternativeName,omitempty"`
	SubType         string            `json:"subType,omitempty"`
	Category        string            `json:"category,omitempty"`
	Location        *GeoLocationV1    `json:"location,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	VariantID       string            `json:"variantId,omitempty"`
	VariantVersion  string            `json:"variantVersion,omitempty"`
}

// nonSafeURI matches the shared URN-segment normalisation: everything
// outside the allowed set becomes ":".
var nonSafeURI = regexp.MustCompile(`[^\w\-~:/?#\[\]@!$&'()*+,;=%.]+`)

// SafeURISegment normalises s for use as a URI segment. ":" and "/" are
// kept by convention (percent-encoding would also encode them), so the
// shared rule is ":"-substitution, not percent-encoding.
func SafeURISegment(s string) string {
	return nonSafeURI.ReplaceAllString(strings.TrimSpace(s), ":")
}

// NameOrID returns alternativeName > name > thingId, raw (unsanitised).
// Use SafeURISegment on the result for URN construction.
func (d ThingDescriptorV1) NameOrID() string {
	if d.AlternativeName != "" {
		return d.AlternativeName
	}
	if d.Name != "" {
		return d.Name
	}
	return d.ThingID
}

// TypeName returns subType when set, otherwise the event's thingType
// (template ID). The caller supplies thingType since the descriptor does
// not repeat it.
func (d ThingDescriptorV1) TypeName(thingType string) string {
	if d.SubType != "" {
		return d.SubType
	}
	return thingType
}

// UUID returns the trailing ://-segment of an ID.
func UUID(id string) string {
	if i := strings.LastIndex(id, "/"); i >= 0 {
		return id[i+1:]
	}
	return id
}

// ThingValueV1 is a single versioned thing property observation.
// ObservedAt/ReceivedAt/ComputedAt are kept separate per C5 so consumers
// can reproduce recency (B3) and tie-breaks (B13): later ObservedAt wins,
// then later ReceivedAt, then source ordering. PublishedAt is the outbox
// publish time. Quality is the strict enum ok/uncertain/invalid.
type ThingValueV1 struct {
	Schema         string    `json:"schema,omitempty"`
	ThingID        string    `json:"thingId"`
	Tenant         string    `json:"tenant"`
	PropertyID     string    `json:"propertyId"`
	Value          *float64  `json:"value"`
	Quality        string    `json:"quality,omitempty"`
	Unit           string    `json:"unit,omitempty"`
	Source         string    `json:"source,omitempty"`
	MessageID      string    `json:"messageId,omitempty"`
	ObservedAt     time.Time `json:"observedAt"`
	ReceivedAt     time.Time `json:"receivedAt"`
	ComputedAt     time.Time `json:"computedAt"`
	PublishedAt    time.Time `json:"publishedAt"`
	ConfigRevision int64     `json:"configRevision"`
	EventID        string    `json:"eventId,omitempty"`
}

// Validate checks required identity fields and the strict quality enum.
func (v ThingValueV1) Validate() error {
	if v.ThingID == "" {
		return fmt.Errorf("types: missing thingId")
	}
	if v.Tenant == "" {
		return fmt.Errorf("types: missing tenant")
	}
	if v.PropertyID == "" {
		return fmt.Errorf("types: missing propertyId")
	}
	if v.ObservedAt.IsZero() {
		return fmt.Errorf("types: missing observedAt")
	}
	switch v.Quality {
	case "ok", "uncertain", "invalid":
	default:
		return fmt.Errorf("types: invalid quality %q (want ok/uncertain/invalid)", v.Quality)
	}
	if v.Value != nil && (math.IsNaN(*v.Value) || math.IsInf(*v.Value, 0)) {
		return fmt.Errorf("types: invalid value (NaN/Inf not allowed)")
	}
	return nil
}

// ThingValuesEventV1 groups property values (and related relation changes)
// for one thing at one config revision. ThingType carries the template ID
// (e.g. "wastebin") so consumers can route without a catalog join.
type ThingValuesEventV1 struct {
	Schema           string             `json:"schema,omitempty"`
	EventID          string             `json:"eventId"`
	Tenant           string             `json:"tenant"`
	ThingID          string             `json:"thingId"`
	ThingType        string             `json:"thingType,omitempty"`
	ThingTypeVersion string             `json:"thingTypeVersion,omitempty"`
	ConfigRevision   int64              `json:"configRevision"`
	PublishedAt      time.Time          `json:"publishedAt"`
	Thing            *ThingDescriptorV1 `json:"thing,omitempty"`
	Values           []ThingValueV1     `json:"values"`
	Relations        []RelationChangeV1 `json:"relations,omitempty"`
}

// RelationChangeV1 describes a single directed relation change.
// Removed marks a detached link; ToID then carries the last known target
// (which link disappeared), otherwise the new one.
type RelationChangeV1 struct {
	RelationID string    `json:"relationId"`
	Type       string    `json:"type"`
	FromID     string    `json:"fromId"`
	ToID       string    `json:"toId,omitempty"`
	Removed    bool      `json:"removed,omitempty"`
	Tenant     string    `json:"tenant"`
	Revision   int64     `json:"revision"`
	ChangedAt  time.Time `json:"changedAt"`
}

// TopicName reports the publish topic (B20: isolated v1 topics).
func (e RelationChangeV1) TopicName() string { return TopicRelationChanges }

// ContentType reports the versioned payload content type.
func (e RelationChangeV1) ContentType() string { return ContentTypeV1 }

// Body serializes the event for transport.
func (e RelationChangeV1) Body() []byte {
	b, _ := json.Marshal(e)
	return b
}

// ThingLifecycleKind describes what happened to a thing.
type ThingLifecycleKind string

const (
	ThingCreated ThingLifecycleKind = "created"
	ThingUpdated ThingLifecycleKind = "updated"
	ThingDeleted ThingLifecycleKind = "deleted"
)

// ThingLifecycleV1 announces thing creation, configuration change or
// deletion. Consumers that materialize things subscribe to stay in sync,
// including deletes. The snapshot carries the thing descriptor at
// ConfigRevision.
type ThingLifecycleV1 struct {
	Schema           string             `json:"schema,omitempty"`
	EventID          string             `json:"eventId"`
	Tenant           string             `json:"tenant"`
	ThingID          string             `json:"thingId"`
	Kind             ThingLifecycleKind `json:"kind"`
	ThingType        string             `json:"thingType,omitempty"`
	ThingTypeVersion string             `json:"thingTypeVersion,omitempty"`
	ConfigRevision   int64              `json:"configRevision"`
	ChangedAt        time.Time          `json:"changedAt"`
	Thing            *ThingDescriptorV1 `json:"thing,omitempty"`
}

// TopicName reports the publish topic (B20: isolated v1 topics).
func (e ThingLifecycleV1) TopicName() string { return TopicLifecycleChanges }

// ContentType reports the versioned payload content type.
func (e ThingLifecycleV1) ContentType() string { return ContentTypeV1 }

// Body serializes the event for transport.
func (e ThingLifecycleV1) Body() []byte {
	b, _ := json.Marshal(e)
	return b
}

// Validate checks required identity fields and the kind enum.
func (e ThingLifecycleV1) Validate() error {
	if e.ThingID == "" {
		return fmt.Errorf("types: missing thingId")
	}
	if e.Tenant == "" {
		return fmt.Errorf("types: missing tenant")
	}
	switch e.Kind {
	case ThingCreated, ThingUpdated, ThingDeleted:
	default:
		return fmt.Errorf("types: invalid kind %q (want created/updated/deleted)", e.Kind)
	}
	return nil
}

// EventTimestamp är konsumentens tid för ett värdesevent (K1.3):
// max(values[].observedAt) — mättid, aldrig transporttid (B3).
// Rekommendationen i INTEGRATION.md är därmed kontrakt.
func (e ThingValuesEventV1) EventTimestamp() time.Time {
	var latest time.Time
	for _, v := range e.Values {
		if v.ObservedAt.After(latest) {
			latest = v.ObservedAt
		}
	}
	return latest
}

// ValueByProperty returnerar värdet för en egenskap eller nil när den
// saknas i eventet. K1.4: konsumentregler adresserar egenskaper via
// values.<propertyId>.{value,observedAt,quality,unit,source} —
// ersättaren för gamla field:-vägar (temperature.v, measured.level,
// percent, refDevices[0].deviceID). Full proveniens (antal
// tillgängliga/förväntade per ingång) stannar i historik/outbox;
// kontraktet bär kommaseparerad source
// (device/[channel]/object/resource, tom channel = alla).
func (e ThingValuesEventV1) ValueByProperty(propertyID string) *ThingValueV1 {
	for i := range e.Values {
		if e.Values[i].PropertyID == propertyID {
			return &e.Values[i]
		}
	}
	return nil
}

// ShouldEmitProperty är konsumentregeln för kvalitet (K1.5): invalid
// hoppar alltid över propertyn (ogiltig ersätter aldrig aktuellt),
// ok/uncertain skrivs där uncertain flaggas vidare av konsumenten.
// Okänd/tom kvalitet behandlas som invalid.
func ShouldEmitProperty(quality string) bool {
	return quality == "ok" || quality == "uncertain"
}

// ShouldEmitEntity avgör om en entitet får skrivas: alla required
// egenskaper måste finnas med emitterbar kvalitet, annars hoppas hela
// entiteten över (ingen icke-konform partiell entitet). Valfria
// egenskaper utelämnas individuellt (when:present default).
func (e ThingValuesEventV1) ShouldEmitEntity(required []string) bool {
	for _, id := range required {
		v := e.ValueByProperty(id)
		if v == nil || !ShouldEmitProperty(v.Quality) {
			return false
		}
	}
	return true
}

// TopicName reports the publish topic (B20: isolated v1 topics).
func (e ThingValuesEventV1) TopicName() string { return TopicThingValues }

// ContentType reports the versioned payload content type.
func (e ThingValuesEventV1) ContentType() string { return ContentTypeV1 }

// Body serializes the event for transport.
func (e ThingValuesEventV1) Body() []byte {
	b, _ := json.Marshal(e)
	return b
}
