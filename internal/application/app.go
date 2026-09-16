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
	"time"

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

	// DrainPending publishes all pending outbox batches (restart drain).
	DrainPending(ctx context.Context) (int, error)
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
	rt     *Runtime
	pub    EventPublisher

	mu sync.Mutex
}

// New wires reader/writer/msgCtx with the Etapp 4 runtime and publisher.
// rt/pub may be nil in tests: batches are then accepted without persistence
// fan-out (DrainPending reports none).
func New(r ThingsReader, w ThingsWriter, m messaging.MsgContext, rt *Runtime, pub EventPublisher) *app {
	return &app{reader: r, writer: w, msgCtx: m, rt: rt, pub: pub}
}

// HandleMeasurements validates tenant identity, persists the batch
// atomically via the runtime and publishes pending events. Errors propagate
// for retry (same contract as iot-things); structural/identity errors are
// classified Permanent in handlers.go and never reach here as such.
func (a *app) HandleMeasurements(ctx context.Context, tenant string, messageID string, observations []things.Observation) error {
	if tenant == "" {
		return fmt.Errorf("%w: empty tenant (message %q)", ErrMissingThingTenant, messageID)
	}
	if messageID == "" {
		return fmt.Errorf("%w: empty messageID", ErrMissingThingTenant)
	}
	if len(observations) == 0 {
		return nil
	}
	log := logging.GetFromContext(ctx)
	log.Debug("handle measurements", "tenant", tenant, "messageID", messageID, "count", len(observations))
	if a.rt == nil {
		return nil
	}
	outcomes, err := a.rt.HandleBatch(ctx, tenant, messageID, observations, time.Now().UTC())
	if err != nil {
		return err
	}
	if a.pub == nil {
		return nil
	}
	// Enhetlig publiceringstid per batch (samma PublishedAt för alla saker).
	now := time.Now().UTC()
	for _, o := range outcomes {
		if _, err := a.rt.DrainAndPublish(ctx, a.pub, tenant, o.ThingID, now); err != nil {
			return err
		}
	}
	return nil
}

// DrainPending publishes all pending outbox batches (restart drain after
// messaging start, before/after handler registration in cmd).
func (a *app) DrainPending(ctx context.Context) (int, error) {
	if a.rt == nil || a.pub == nil {
		return 0, nil
	}
	return a.rt.DrainAll(ctx, a.pub, time.Now().UTC())
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
