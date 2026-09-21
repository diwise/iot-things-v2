package main

import (
	"context"
	"os"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application"
	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/infrastructure/storage"
)

// K4: HISTORY_RETENTION_DAYS tolkning — endast positivt heltal aktiverar
// radering, allt annat betyder obegränsat (B6-default).
func TestRetentionDays(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{"30", 30},
		{"1", 1},
		{"0", 0},
		{"", 0},
		{"-5", 0},
		{"abc", 0},
		{"1.5", 0},
		{" 30", 0},
	} {
		if got := retentionDays(flagMap{historyRetentionDays: tc.in}); got != tc.want {
			t.Fatalf("retentionDays(%q)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

// TV-01: trasig lagring ger fel (med Error-status i span + fellogg) i
// stället för ett normalt "0 raderade"-resultat. Managern byggs över en
// stängd pg-pool (kräver nåbar DB för konstruktion, skippar annars).
func TestRunRetentionPurgeFailureSurfaces(t *testing.T) {
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	store, err := storage.NewPostgres(context.Background(), storage.NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	store.Close()
	mgr := application.NewManager(catalog.New(), application.NewRuntime(store), store)
	if err := runRetentionPurge(context.Background(), mgr, 30); err == nil {
		t.Fatal("runRetentionPurge on broken storage must fail")
	}
}
