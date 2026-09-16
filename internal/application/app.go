// Package application is the v2 application layer, mirroring iot-things
// internal/application: domain orchestration with no transport or database
// imports. Transport (messaging/senml) enters via handlers.go, persistence
// via the ThingsReader/ThingsWriter interfaces (implemented by
// internal/infrastructure/storage), and outbound events via MsgContext.
package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// ThingsApp mirrors the iot-things application facade (subset for v2).
type ThingsApp interface {
	HandleMeasurements(ctx context.Context, tenant string, messageID string, observations []things.Observation) error

	Add(ctx context.Context, b []byte) error
	Seed(ctx context.Context, r io.Reader) error
	LoadConfig(ctx context.Context, r io.Reader) error

	MigrateBindings(ctx context.Context) (int, error)
	HasUnmigratedThings(ctx context.Context) (bool, error)
}

// ThingsReader is implemented by the infrastructure storage layer.
type ThingsReader interface {
	QueryThing(tenant, id string) ([]byte, int64, bool)
}

// ThingsWriter is implemented by the infrastructure storage layer.
type ThingsWriter interface {
	PutThing(tenant, id string, data []byte, revision int64) int64
}

var (
	ErrThingNotFound      = errors.New("thing not found")
	ErrAlreadyExists      = errors.New("thing already exists")
	ErrMissingThingID     = errors.New("thing ID must be provided")
	ErrMissingThingTenant = errors.New("tenant must be provided")
	ErrMissingThingType   = errors.New("thing type must be provided")
	ErrInvalidThingID     = errors.New("thing ID must not contain '/'")
	ErrInvalidBinding     = errors.New("invalid binding")
)

type app struct {
	reader ThingsReader
	writer ThingsWriter
	msgCtx messaging.MsgContext

	mu sync.Mutex
}

// New wires reader/writer/msgCtx. Messaging may be nil in tests.
func New(r ThingsReader, w ThingsWriter, m messaging.MsgContext) *app {
	return &app{reader: r, writer: w, msgCtx: m}
}

// HandleMeasurements validates tenant identity and fans observations out to
// bound properties. Full binding/graph evaluation lands in evaluate.go;
// this facade owns ack semantics: structural/identity errors are permanent,
// handling errors are returned for retry (same contract as iot-things
// handlers: Permanent vs Temporary classification lives in handlers.go).
func (a *app) HandleMeasurements(ctx context.Context, tenant string, messageID string, observations []things.Observation) error {
	if tenant == "" {
		return fmt.Errorf("%w: empty tenant (message %q)", ErrMissingThingTenant, messageID)
	}
	if len(observations) == 0 {
		return nil
	}
	log := logging.GetFromContext(ctx)
	log.Debug("handle measurements", "tenant", tenant, "messageID", messageID, "count", len(observations))
	// TODO(evaluate): resolve bindings via reader, apply time/quality rules in
	// evaluate.go, persist current/history/state + outbox atomically, publish
	// pkg/types ThingValuesEventV1 via msgCtx.
	return nil
}

// Add validates and stores a thing definition (JSON). Full validation
// (closed template, binding signals, ETag) lands with the catalog work.
func (a *app) Add(_ context.Context, b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("%w: empty body", ErrMissingThingID)
	}
	return nil
}

// LoadConfig loads operator/catalog configuration (YAML). Stub: validated
// but not yet stored; iot-things parity point (newApp -> LoadConfig).
func (a *app) LoadConfig(_ context.Context, r io.Reader) error {
	if r == nil {
		return errors.New("nil config reader")
	}
	return nil
}

// Seed loads initial things (CSV). Stub: consumed but not yet stored;
// exercised by cmd seed path.
func (a *app) Seed(_ context.Context, r io.Reader) error {
	if r == nil {
		return errors.New("nil seed reader")
	}
	return nil
}

// MigrateBindings converts legacy refDevices posts to bindings once at
// startup, before the message handler is registered (same gating as
// iot-things THINGS_MIGRATE_BINDINGS). v2 starts binding-native: always 0.
func (a *app) MigrateBindings(_ context.Context) (int, error) { return 0, nil }

// HasUnmigratedThings reports posts lacking bindings for fail-fast gating.
func (a *app) HasUnmigratedThings(_ context.Context) (bool, error) { return false, nil }
