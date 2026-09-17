package types

import (
	"encoding/json"
	"fmt"
	"math"
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
// event's config revision. It carries what consumers (e.g. FIWARE mapping
// to Smart Data Models) need without an API join: identity for the entity
// URN, category for rule matching, location for GeoProperties and metadata
// for the remaining attributes.
type ThingDescriptorV1 struct {
	ThingID        string            `json:"thingId"`
	Name           string            `json:"name,omitempty"`
	Category       string            `json:"category,omitempty"`
	Location       *GeoLocationV1    `json:"location,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	VariantID      string            `json:"variantId,omitempty"`
	VariantVersion string            `json:"variantVersion,omitempty"`
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
// (e.g. "wastebin") so thing.updated-consumers (FIWARE mapping needs the
// thing type) can route without a catalog join.
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
// deletion. Consumers that materialize things (e.g. FIWARE entities)
// subscribe to stay in sync, including deletes. The snapshot carries the
// thing descriptor at ConfigRevision.
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

// TopicName reports the publish topic (B20: isolated v1 topics).
func (e ThingValuesEventV1) TopicName() string { return TopicThingValues }

// ContentType reports the versioned payload content type.
func (e ThingValuesEventV1) ContentType() string { return ContentTypeV1 }

// Body serializes the event for transport.
func (e ThingValuesEventV1) Body() []byte {
	b, _ := json.Marshal(e)
	return b
}
