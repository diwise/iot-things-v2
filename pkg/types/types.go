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

// SchemaV1 is the payload schema version for the v1 topics.
// Old topics/payloads (e.g. thing.updated) are left untouched (B20).
const SchemaV1 = "v1"

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
// for one thing at one config revision.
type ThingValuesEventV1 struct {
	Schema         string             `json:"schema,omitempty"`
	EventID        string             `json:"eventId"`
	Tenant         string             `json:"tenant"`
	ThingID        string             `json:"thingId"`
	ConfigRevision int64              `json:"configRevision"`
	PublishedAt    time.Time          `json:"publishedAt"`
	Values         []ThingValueV1     `json:"values"`
	Relations      []RelationChangeV1 `json:"relations,omitempty"`
}

// RelationChangeV1 describes a single directed relation change.
type RelationChangeV1 struct {
	RelationID string    `json:"relationId"`
	Type       string    `json:"type"`
	FromID     string    `json:"fromId"`
	ToID       string    `json:"toId"`
	Tenant     string    `json:"tenant"`
	Revision   int64     `json:"revision"`
	ChangedAt  time.Time `json:"changedAt"`
}

// TopicName reports the publish topic (B20: isolated v1 topics).
func (e ThingValuesEventV1) TopicName() string { return TopicThingValues }

// ContentType reports the versioned payload content type.
func (e ThingValuesEventV1) ContentType() string { return "application/vnd.diwise.things.v1+json" }

// Body serializes the event for transport.
func (e ThingValuesEventV1) Body() []byte {
	b, _ := json.Marshal(e)
	return b
}
