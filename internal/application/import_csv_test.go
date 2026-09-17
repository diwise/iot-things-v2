package application

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

const importTestCSV = `id;type;subType;name;decsription;location;tenant;tags;bindings;args
a6921a85-1f40-4e43-9719-4dabdb349075;Container;WasteContainer;GATA_Soptunnor_punkt.203;Kapacitet 160 liter. ObjektID: 203;62.387232,17.316830;default;160L;milesight:203;{'maxd':0.940000,'maxl':0.790000}
room-1;Room;;Rum A;;59.33,18.06;default;;s-r1||urn:oma:lwm2m:ext:3303|5700|temperature;
desk-1;Desk;;Skrivbord;;;default;;s-desk;
pass-1;Passage;;Entré;;;default;;s-pass;
bad-type;Nope;;;59,18;default;;;;
angle-bin;Container;;;;59,18;default;;milesight:9;{'maxd':0.94,'maxl':0.79,'angle':15}
bad-loc;Desk;;D;;not-a-place;default;;s-d;
no-tenant;Desk;;D;;59,18;;;s-d;
`

func importSeededManager(t *testing.T) *Manager {
	t.Helper()
	raw, err := os.ReadFile("../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := catalog.ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	mgr, _, _, _ := mgmtSetup(t)
	if _, _, err := mgr.SeedCatalog(context.Background(), seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return mgr
}

func TestImportCSV(t *testing.T) {
	ctx := context.Background()
	mgr := importSeededManager(t)

	added, skipped, failures, err := mgr.ImportCSV(ctx, strings.NewReader(importTestCSV))
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if added != 4 || skipped != 0 {
		t.Fatalf("added=%d skipped=%d, want 4/0", added, skipped)
	}
	if len(failures) != 4 {
		t.Fatalf("failures=%+v, want 4", failures)
	}
	rows := map[int]bool{}
	for _, f := range failures {
		rows[f.Row] = true
	}
	for _, want := range []int{6, 7, 8, 9} {
		if !rows[want] {
			t.Fatalf("failures=%+v, want row %d rejected", failures, want)
		}
	}

	// Idempotent omläsning: allt redan skapat.
	added, skipped, failures, err = mgr.ImportCSV(ctx, strings.NewReader(importTestCSV))
	if err != nil {
		t.Fatalf("reimport: %v", err)
	}
	if added != 0 || skipped != 4 || len(failures) != 4 {
		t.Fatalf("reimport added=%d skipped=%d failures=%d, want 0/4/4", added, skipped, len(failures))
	}

	// Tunnan bär subtyp/taggar i metadata och får mått som overrides.
	sum, err := mgr.GetObject(ctx, "default", "a6921a85-1f40-4e43-9719-4dabdb349075")
	if err != nil {
		t.Fatalf("get bin: %v", err)
	}
	if sum.Metadata["tags"] != "160L" || sum.Metadata["subType"] != "WasteContainer" {
		t.Fatalf("metadata=%+v, want tags+subType", sum.Metadata)
	}
	if sum.ParamSources["usableHeight"] != things.ParamSourceThing ||
		sum.ParamSources["sensorToBottom"] != things.ParamSourceThing {
		t.Fatalf("param sources=%+v, want thing overrides", sum.ParamSources)
	}
}

func TestImportCSVBadStructure(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	_, _, _, err := mgr.ImportCSV(context.Background(), strings.NewReader("id;type\nonly-two-cols\n"))
	if err == nil {
		t.Fatal("short rows must be fatal")
	}
}

func TestImportCSVSeedRoom(t *testing.T) {
	// Rum med legacy temperature-punkt från seed-mallen.
	raw := `id;type;subType;name;decsription;location;tenant;tags;bindings;args
room-9;Room;;Rum Nio;;59.3,18.0;default;;s-r9||urn:oma:lwm2m:ext:3303|5700|temperature;
`
	ctx := context.Background()
	mgr := importSeededManager(t)
	added, _, failures, err := mgr.ImportCSV(ctx, strings.NewReader(raw))
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if added != 1 || len(failures) != 0 {
		t.Fatalf("added=%d failures=%+v, want 1/0", added, failures)
	}
}
