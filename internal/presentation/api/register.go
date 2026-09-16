package api

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/diwise/iot-things-v2/internal/presentation/api/auth"
	"github.com/diwise/service-chassis/pkg/infrastructure/net/http/router"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// Scopes mirror iot-things internal/presentation/api so policies and
// frontend roles transfer unchanged.
const (
	CreateThings auth.Scope = "things.create"
	ReadThings   auth.Scope = "things.read"
	UpdateThings auth.Scope = "things.update"
	DeleteThings auth.Scope = "things.delete"
)

type registerOptions struct {
	authOptions []auth.Option
}

// RegisterOption configures API registration.
type RegisterOption func(*registerOptions)

// WithAccessObjectAuthorization toggles the OPA access-object result model,
// same seam as iot-things.
func WithAccessObjectAuthorization(enabled bool) RegisterOption {
	return func(o *registerOptions) {
		o.authOptions = append(o.authOptions, auth.WithAccessObjectAuthorization(enabled))
	}
}

// ThingsApp is the minimal application surface the HTTP layer needs. It is
// deliberately narrow so presentation stays decoupled from the application
// implementation (same direction as iot-things: presentation -> application).
type ThingsApp interface {
	Add(ctx context.Context, b []byte) error
}

// RegisterHandlers wires the public API onto mux with the same structure as
// iot-things: router with /api/v0 prefix, OPA auth groups per scope, tracing
// via service-chassis. The v1 memory routes (NewMux) stay available for the
// scaffold and its tests; the authenticated v0 surface below is where the
// application wiring lands next (handlers currently return 501).
func RegisterHandlers(ctx context.Context, mux *http.ServeMux, app ThingsApp, policies io.Reader, opts ...RegisterOption) error {
	const apiPrefix string = "/api/v0"

	log := logging.GetFromContext(ctx)
	_ = log

	registerOpts := registerOptions{}
	for _, apply := range opts {
		apply(&registerOpts)
	}

	authz, err := auth.NewAuthenticator(ctx, policies, registerOpts.authOptions...)
	if err != nil {
		return fmt.Errorf("failed to create api authenticator: %w", err)
	}

	r := router.New(mux, router.WithPrefix(apiPrefix))

	notImplemented := func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "not implemented (TODO: wire application.ThingsApp)"})
	}

	r.Route("things", func(r router.ServeMux) {
		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(ReadThings))
			r.Get("", notImplemented)
			r.Get("{id}", notImplemented)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(CreateThings))
			r.Post("", notImplemented)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(UpdateThings))
			r.Put("{id}", notImplemented)
			r.Patch("{id}", notImplemented)
		})

		r.Group(func(r router.ServeMux) {
			r.Use(authz.RequireAccess(DeleteThings))
			r.Delete("{id}", notImplemented)
		})
	})

	return nil
}
