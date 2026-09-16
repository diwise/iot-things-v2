package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diwise/iot-things-v2/internal/api"
)

// TODO(store): wire PostgreSQL via pgx for things/history/relations.
// TODO(messaging): publish thing values/relation changes via messaging-golang.

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	listenAddr := getenv("LISTEN_ADDRESS", "0.0.0.0")
	servicePort := getenv("SERVICE_PORT", "8080")
	controlPort := getenv("CONTROL_PORT", "8000")

	publicAddr := listenAddr + ":" + servicePort
	controlAddr := listenAddr + ":" + controlPort

	store := api.NewServer()
	publicMux := api.NewMux(store)

	controlMux := http.NewServeMux()
	controlMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	controlMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	publicSrv := &http.Server{
		Addr:              publicAddr,
		Handler:           publicMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	controlSrv := &http.Server{
		Addr:              controlAddr,
		Handler:           controlMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		logger.Info("public server listening", "addr", publicAddr)
		if err := publicSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		logger.Info("control server listening", "addr", controlAddr)
		if err := controlSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		logger.Error("server failed", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := publicSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("public server shutdown failed", "error", err)
	}
	if err := controlSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("control server shutdown failed", "error", err)
	}
	logger.Info("shutdown complete")
}
