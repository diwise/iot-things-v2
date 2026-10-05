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

// ThingSnapshotValueV1 is one current (snapshot) property value: the
// producer's current-state for the property at the event's config revision,
// not a merge of historical observations. Unit is frozen per configRevision
// (conversion only via explicit convert rule); value is finite when present
// (NaN/Inf never become current); quality is the strict enum;
// observedAt/receivedAt/source/messageId/configRevision preserve the value's
// own time, unit, quality and provenance across config changes and restarts.
// Sub-second precision (RFC3339Nano, microsecond dedup) is preserved;
// zero/false are preserved (nil means missing, not zero).
type ThingSnapshotValueV1 struct {
	PropertyID     string    `json:"propertyId"`
	Value          *float64  `json:"value"`
	Quality        string    `json:"quality"`
	Unit           string    `json:"unit,omitempty"`
	ObservedAt     time.Time `json:"observedAt"`
	ReceivedAt     time.Time `json:"receivedAt"`
	Source         string    `json:"source,omitempty"`
	MessageID      string    `json:"messageId,omitempty"`
	ConfigRevision int64     `json:"configRevision"`
}

// Validate checks the snapshot value: strict quality enum and finite value.
func (v ThingSnapshotValueV1) Validate() error {
	if v.PropertyID == "" {
		return fmt.Errorf("types: missing propertyId")
	}
	switch v.Quality {
	case "ok", "uncertain", "invalid":
	default:
		return fmt.Errorf("types: invalid quality %q (want ok/uncertain/invalid)", v.Quality)
	}
	if v.Value != nil && (math.IsNaN(*v.Value) || math.IsInf(*v.Value, 0)) {
		return fmt.Errorf("types: invalid value (NaN/Inf not allowed)")
	}
	if v.ObservedAt.IsZero() {
		return fmt.Errorf("types: missing observedAt")
	}
	return nil
}

// ThingValuesEventV1 groups delta (what updated now) and snapshot (all
// current values) for one thing at one frozen config revision, plus the
// frozen descriptor and version references. ThingType carries the template
// ID (e.g. "wastebin") so consumers can route without a catalog join.
//
// EventID is "v1/<tenant>/<thing>/<message>". ConfigRevision, Thing
// descriptor, ThingType/Version and every snapshot unit are frozen at
// mutation/computation: queued outbox entries are never re-enriched from
// current config at drain, and restart republishes without recomputation,
// including an old revision after the thing got a new config.
//
// Five-mode semantics (PLAN1050 A2):
//   - missing current (snapshot lacks property; required -> error),
//   - new invalid obs (invalid/null/NaN/Inf/unit mismatch -> delta carries
//     an invalid entry, snapshot keeps the previous valid - never replaced),
//   - previous-valid-after-invalid (preview shows current vs latest delta),
//   - removed property at config change (pruned -> snapshot missing +
//     removed[]),
//   - explicit clearing (only via removeAttributes/lifecycle rule).
//
// Zero/false are preserved; sub-second precision (RFC3339Nano,
// microsecond dedup); unit frozen per configRevision.
//
// Values is the deprecated alias for Delta, kept one release: producers set
// both to the same slice; consumers read Delta with fallback to Values.
type ThingValuesEventV1 struct {
	Schema           string                 `json:"schema,omitempty"`
	EventID          string                 `json:"eventId"`
	Tenant           string                 `json:"tenant"`
	ThingID          string                 `json:"thingId"`
	ThingType        string                 `json:"thingType,omitempty"`
	ThingTypeVersion string                 `json:"thingTypeVersion,omitempty"`
	ConfigRevision   int64                  `json:"configRevision"`
	PublishedAt      time.Time              `json:"publishedAt"`
	Thing            *ThingDescriptorV1     `json:"thing,omitempty"`
	Delta            []ThingValueV1         `json:"delta,omitempty"`
	Snapshot         []ThingSnapshotValueV1 `json:"snapshot,omitempty"`
	Removed          []string               `json:"removed,omitempty"`
	Relations        []RelationChangeV1     `json:"relations,omitempty"`
	// Values is the deprecated alias for Delta (one release): always equal
	// to Delta when produced; consumed as fallback when Delta is absent.
	Values []ThingValueV1 `json:"values"`
}

// RelationChangeV1 describes a single directed relation change for any
// named slot (not only partOf). Removed marks a detached link; ToID then
// carries the last known target (which link disappeared), otherwise the new
// one. PreviousToId carries the previous target on move: a move is an
// atomic remove+set at the same revision (same RelationID, set entry carries
// previousToId, removed entry is not emitted separately for moves).
//
// RelationID is "v1/<tenant>/<from>/relations/<type>/<to>/<revision>/<set|removed>"
// (To ingår för unicitet vid flera mål samma revision; paritet med F:s
// BuildRelationID). Legacy "partof-<child>" IDs remain readable (opaque string,
// no validation) but new events use the versioned form. F:s legacy v1 without
// To (seven segments) remains readably parseable (To empty, no To check);
// C1 tolerates all forms opaquely while F validates strict v1 on write.
//
// FromThingType/FromThingTypeVersion/From and
// ToThingType/ToThingTypeVersion/To let consumers route and render without
// a catalog join: typeName is SubType over thing type and nameOrID is
// alternativeName over name over thing ID (same convention as the thing's
// own rules). All are additive and omitempty. To (descriptor) is null when
// the target side is unknown; readers tolerate null To (broken reference
// stays visible as target:null instead of silently vanishing).
//
// Tenant boundary is strict intra-tenant; duplicates
// (tenant,relation,from,to) are idempotent upserts. partOf keeps its
// hierarchy rules (max one parent, CTE cycle check); generic names have no
// cycle gate.
type RelationChangeV1 struct {
	RelationID   string    `json:"relationId"`
	Type         string    `json:"type"`
	FromID       string    `json:"fromId"`
	ToID         string    `json:"toId,omitempty"`
	Removed      bool      `json:"removed,omitempty"`
	PreviousToID string    `json:"previousToId,omitempty"`
	Tenant       string    `json:"tenant"`
	Revision     int64     `json:"revision"`
	ChangedAt    time.Time `json:"changedAt"`
	// FromThingType is the source's template ID; FromThingTypeVersion its
	// version; From is its frozen descriptor at Revision.
	FromThingType        string             `json:"fromThingType,omitempty"`
	FromThingTypeVersion string             `json:"fromThingTypeVersion,omitempty"`
	From                 *ThingDescriptorV1 `json:"from,omitempty"`
	// ToThingType is the target's template ID; ToThingTypeVersion its
	// version; To is its frozen descriptor at Revision.
	// Absent (null) only when the target side is unknown.
	ToThingType        string             `json:"toThingType,omitempty"`
	ToThingTypeVersion string             `json:"toThingTypeVersion,omitempty"`
	To                 *ThingDescriptorV1 `json:"to,omitempty"`
}

// Validate checks required identity fields for a relation change.
func (e RelationChangeV1) Validate() error {
	if e.RelationID == "" {
		return fmt.Errorf("types: missing relationId")
	}
	if e.Type == "" {
		return fmt.Errorf("types: missing type")
	}
	if e.FromID == "" {
		return fmt.Errorf("types: missing fromId")
	}
	if e.Tenant == "" {
		return fmt.Errorf("types: missing tenant")
	}
	return nil
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
// including deletes. The snapshot carries the frozen thing descriptor at
// ConfigRevision. Bootstrap marks bootstrap/resync emissions (complete
// underlay without new measurements) so descriptive things without
// measurements can be created/updated; it is additive and omitempty.
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
	Bootstrap        bool               `json:"bootstrap,omitempty"`
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
// max(delta[].observedAt) — mättid, aldrig transporttid (B3).
// Faller tillbaka på values[] (deprecated alias) när delta saknas.
// Snapshot-tid ersätter aldrig dessa tider: varje snapshotvärde bär egen
// observationstid, enhet, kvalitet och proveniens.
// Rekommendationen i INTEGRATION.md är därmed kontrakt.
func (e ThingValuesEventV1) EventTimestamp() time.Time {
	var latest time.Time
	vals := e.Delta
	if len(vals) == 0 {
		vals = e.Values
	}
	for _, v := range vals {
		if v.ObservedAt.After(latest) {
			latest = v.ObservedAt
		}
	}
	return latest
}

// deltaValues returnerar delta med fallback på values (deprecated alias).
func (e ThingValuesEventV1) deltaValues() []ThingValueV1 {
	if len(e.Delta) > 0 {
		return e.Delta
	}
	return e.Values
}

// ValueByProperty returnerar deltavärdet för en egenskap eller nil när den
// saknas i eventet. Läser Delta med fallback på Values (deprecated alias).
// K1.4: konsumentregler adresserar egenskaper via
// values.<propertyId>.{value,observedAt,quality,unit,source} —
// ersättaren för gamla field:-vägar (temperature.v, measured.level,
// percent, refDevices[0].deviceID). Full proveniens (antal
// tillgängliga/förväntade per ingång) stannar i historik/outbox;
// kontraktet bär kommaseparerad source
// (device/[channel]/object/resource, tom channel = alla).
func (e ThingValuesEventV1) ValueByProperty(propertyID string) *ThingValueV1 {
	for i := range e.Delta {
		if e.Delta[i].PropertyID == propertyID {
			return &e.Delta[i]
		}
	}
	for i := range e.Values {
		if e.Values[i].PropertyID == propertyID {
			return &e.Values[i]
		}
	}
	return nil
}

// SnapshotByProperty returnerar snapshotvärdet (current) för en egenskap
// eller nil när snapshot saknar den (saknat current; required -> fel).
// Skiljer saknat current från ny ogiltig observation (delta har
// invalid-post, snapshot behåller tidigare giltigt) och från borttagen
// egenskap (snapshot saknas + removed[]).
func (e ThingValuesEventV1) SnapshotByProperty(propertyID string) *ThingSnapshotValueV1 {
	for i := range e.Snapshot {
		if e.Snapshot[i].PropertyID == propertyID {
			return &e.Snapshot[i]
		}
	}
	return nil
}

// DeltaOrValues returnerar delta (eller deprecated values-aliaset) för
// konsumenter som ännu inte migrerat till delta-fältet. Snapshot läses via
// SnapshotByProperty; removed via Removed.
func (e ThingValuesEventV1) DeltaOrValues() []ThingValueV1 {
	return e.deltaValues()
}

// Validate checks required identity fields and nested values.
func (e ThingValuesEventV1) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("types: missing eventId")
	}
	if e.ThingID == "" {
		return fmt.Errorf("types: missing thingId")
	}
	if e.Tenant == "" {
		return fmt.Errorf("types: missing tenant")
	}
	for _, v := range e.Delta {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	for _, v := range e.Values {
		if err := v.Validate(); err != nil {
			return err
		}
	}
	for _, v := range e.Snapshot {
		if err := v.Validate(); err != nil {
			return err
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
// När snapshot finns (delta+snapshot-kontraktet) prövas snapshot
// (producentens current-state, inte senaste ogiltiga delta): saknat
// current eller ogiltigt current för required hoppar entiteten, även om
// delta bär en ogiltig post (ogiltig ersätter aldrig aktuellt). Utan
// snapshot faller den tillbaka på delta/values (deprecated beteende).
func (e ThingValuesEventV1) ShouldEmitEntity(required []string) bool {
	if len(e.Snapshot) > 0 {
		for _, id := range required {
			v := e.SnapshotByProperty(id)
			if v == nil || !ShouldEmitProperty(v.Quality) {
				return false
			}
		}
		return true
	}
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
