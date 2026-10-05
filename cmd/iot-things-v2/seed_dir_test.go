package main

import (
	"os"
	"path/filepath"
	"testing"
)

// loadSeed ska läsa både enstaka seedfil och en katalog med seedfiler
// (t.ex. en monterad test/things-katalog). Saknad sökväg är inte ett fel
// här (seedCatalog varnar och fortsätter); corrupt innehåll ska fela.
func TestLoadSeedFileAndDir(t *testing.T) {
	doc := `{"description":"test","templates":[{"template":{"id":"room","version":"v1"}}]}`

	file := filepath.Join(t.TempDir(), "seed.json")
	if err := os.WriteFile(file, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	seed, err := loadSeed(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(seed.Templates) != 1 || seed.Templates[0].Template.ID != "room" {
		t.Fatalf("file seed not loaded: %+v", seed.Templates)
	}

	dir := t.TempDir()
	for _, name := range []string{"b-room.json", "a-wastebin.json"} {
		id := "room"
		if name[2] == 'w' {
			id = "wastebin"
		}
		content := `{"templates":[{"template":{"id":"` + id + `","version":"v1"}}]}`
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignoreras"), 0o644); err != nil {
		t.Fatal(err)
	}
	merged, err := loadSeed(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Templates) != 2 {
		t.Fatalf("want 2 merged templates, got %d", len(merged.Templates))
	}

	if _, err := loadSeed(filepath.Join(t.TempDir(), "missing")); err == nil || !os.IsNotExist(err) {
		t.Fatalf("missing path should give not-exist error, got %v", err)
	}

	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("{trasig"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSeed(bad); err == nil {
		t.Fatal("corrupt seed should fail")
	}
}
