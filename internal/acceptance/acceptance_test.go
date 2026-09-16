package acceptance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type binding struct {
	Device   string `json:"device"`
	Channel  string `json:"channel"`
	Object   string `json:"object"`
	Resource string `json:"resource"`
	Input    string `json:"input"`
}

type templateDef struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Version   string   `json:"version"`
	Recipe    string   `json:"recipe"`
	Required  []string `json:"required"`
	Optional  []string `json:"optional"`
	Params    []string `json:"params"`
	Repeat    string   `json:"repeat"`
	Operators []string `json:"operators"`
	Operator  string   `json:"operator"`
	Inputs    []string `json:"inputs"`
	Outputs   []string `json:"outputs"`
}

type variantDef struct {
	ID       string             `json:"id"`
	Template string             `json:"template"`
	Recipe   string             `json:"recipe"`
	Params   map[string]float64 `json:"params"`
}

type objectDef struct {
	ID       string    `json:"id"`
	Template string    `json:"template"`
	Variant  string    `json:"variant"`
	PartOf   string    `json:"partOf"`
	Children []string  `json:"children"`
	Inputs   []string  `json:"inputs"`
	Outputs  []string  `json:"outputs"`
	Bindings []binding `json:"bindings"`
}

type fixtureFile struct {
	Description string        `json:"description"`
	Templates   []templateDef `json:"templates"`
	Variants    []variantDef  `json:"variants"`
	Objects     []objectDef   `json:"objects"`
}

func fixturePath(name string) string {
	return filepath.Join("..", "..", "testdata", "fixtures", name)
}

func loadFixture(t *testing.T, name string) fixtureFile {
	t.Helper()

	path := fixturePath(name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("saknar fixture %s (sökväg %s): %v", name, path, err)
	}

	var f fixtureFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("ogiltig JSON i fixture %s (sökväg %s): %v", name, path, err)
	}

	return f
}

func findTemplate(t *testing.T, f fixtureFile, id string) templateDef {
	t.Helper()

	for _, tmpl := range f.Templates {
		if tmpl.ID == id {
			return tmpl
		}
	}

	t.Fatalf("saknar mall %q i fixture", id)
	return templateDef{}
}

func findObject(t *testing.T, f fixtureFile, id string) objectDef {
	t.Helper()

	for _, o := range f.Objects {
		if o.ID == id {
			return o
		}
	}

	t.Fatalf("saknar objekt %q i fixture", id)
	return objectDef{}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func countPrefix(items []string, prefix string) int {
	n := 0
	for _, item := range items {
		if strings.HasPrefix(item, prefix) {
			n++
		}
	}
	return n
}

// A1: 4 varianter delar samma recept-namn (wastebin-level) med olika usableHeight.
func TestA1_BinVariantsShareRecipe(t *testing.T) {
	f := loadFixture(t, "bins.json")

	tmpl := findTemplate(t, f, "wastebin/v1")
	for _, want := range []string{"distance", "level", "fillRate"} {
		if !contains(tmpl.Required, want) {
			t.Fatalf("wastebin/v1 saknar required %q (har %v)", want, tmpl.Required)
		}
	}
	for _, want := range []string{"usableHeight", "sensorToBottom"} {
		if !contains(tmpl.Params, want) {
			t.Fatalf("wastebin/v1 saknar param %q (har %v)", want, tmpl.Params)
		}
	}

	if len(f.Variants) != 4 {
		t.Fatalf("väntat 4 varianter, har %d", len(f.Variants))
	}

	wantHeights := map[string]float64{
		"wastebin-A": 0.8,
		"wastebin-B": 1.2,
		"wastebin-C": 1.5,
		"wastebin-D": 1.8,
	}
	for _, v := range f.Variants {
		if v.Template != "wastebin/v1" {
			t.Fatalf("variant %s pekar på %q, väntat wastebin/v1", v.ID, v.Template)
		}
		if v.Recipe == "" {
			t.Fatalf("variant %s saknar recept-namn", v.ID)
		}
		if v.Recipe != tmpl.Recipe {
			t.Fatalf("variant %s har recept %q, mallen har %q", v.ID, v.Recipe, tmpl.Recipe)
		}
		want, ok := wantHeights[v.ID]
		if !ok {
			t.Fatalf("oväntad variant %q", v.ID)
		}
		if v.Params["usableHeight"] != want {
			t.Fatalf("variant %s har usableHeight %v, väntat %v", v.ID, v.Params["usableHeight"], want)
		}
	}

	bin := findObject(t, f, "bin-42")
	if bin.Template != "wastebin/v1" {
		t.Fatalf("bin-42 har mall %q, väntat wastebin/v1", bin.Template)
	}
	if bin.Variant != "wastebin-B" {
		t.Fatalf("bin-42 har variant %q, väntat wastebin-B", bin.Variant)
	}
	if len(bin.Bindings) != 1 {
		t.Fatalf("bin-42 ska ha exakt 1 bindning, har %d", len(bin.Bindings))
	}
	b := bin.Bindings[0]
	if b.Device != "milesight:214" || b.Channel != "" || b.Object != "urn:oma:lwm2m:ext:3330" || b.Resource != "5700" || b.Input != "distance" {
		t.Fatalf("bin-42 har fel bindning: %+v", b)
	}
}

// A3: room-a har 3 temp-ingångar + average.
func TestA3_RoomA_ThreeTempInputsPlusAverage(t *testing.T) {
	f := loadFixture(t, "rooms.json")

	room := findObject(t, f, "room-a")
	if room.Template != "room/v1" {
		t.Fatalf("room-a har mall %q, väntat room/v1", room.Template)
	}
	if n := countPrefix(room.Inputs, "temperature."); n != 3 {
		t.Fatalf("room-a ska ha 3 temperature.*-ingångar, har %v", room.Inputs)
	}
	if !contains(room.Outputs, "average") {
		t.Fatalf("room-a saknar average i outputs (har %v)", room.Outputs)
	}
}

// A4: room-b har fukt+ljus utan ny mall.
func TestA4_RoomB_HumidityLightWithoutNewTemplate(t *testing.T) {
	f := loadFixture(t, "rooms.json")

	roomA := findObject(t, f, "room-a")
	roomB := findObject(t, f, "room-b")

	if roomB.Template != "room/v1" {
		t.Fatalf("room-b har mall %q, väntat room/v1", roomB.Template)
	}
	if roomB.Template != roomA.Template {
		t.Fatalf("room-b (%q) delar inte mall med room-a (%q)", roomB.Template, roomA.Template)
	}
	if !contains(roomB.Inputs, "humidity") {
		t.Fatalf("room-b saknar humidity (har %v)", roomB.Inputs)
	}
	if !contains(roomB.Inputs, "illuminance") {
		t.Fatalf("room-b saknar illuminance (har %v)", roomB.Inputs)
	}

	roomTemplates := 0
	for _, tmpl := range f.Templates {
		if tmpl.ID == "room/v1" || tmpl.Name == "room" || strings.HasPrefix(tmpl.ID, "room/") {
			roomTemplates++
		}
	}
	if roomTemplates != 1 {
		t.Fatalf("väntat exakt 1 room-mall, har %d", roomTemplates)
	}
}

// A5: building-a har 2 barn (partOf från båda rummen).
func TestA5_BuildingA_TwoChildren(t *testing.T) {
	f := loadFixture(t, "rooms.json")

	building := findObject(t, f, "building-a")
	if len(building.Children) != 2 {
		t.Fatalf("building-a ska ha 2 barn, har %v", building.Children)
	}
	if !contains(building.Children, "room-a") || !contains(building.Children, "room-b") {
		t.Fatalf("building-a ska ha barnen room-a + room-b (har %v)", building.Children)
	}

	for _, id := range []string{"room-a", "room-b"} {
		room := findObject(t, f, id)
		if room.PartOf != "building-a" {
			t.Fatalf("%s saknar partOf building-a (har %q)", id, room.PartOf)
		}
	}
}

// A6: greenhouse-a har 2 bänkar.
func TestA6_GreenhouseA_TwoBenches(t *testing.T) {
	f := loadFixture(t, "greenhouse.json")

	greenhouse := findObject(t, f, "greenhouse-a")
	if len(greenhouse.Children) != 2 {
		t.Fatalf("greenhouse-a ska ha 2 bänkar, har %v", greenhouse.Children)
	}
	if !contains(greenhouse.Children, "bench-1") || !contains(greenhouse.Children, "bench-2") {
		t.Fatalf("greenhouse-a ska ha bench-1 + bench-2 (har %v)", greenhouse.Children)
	}

	for _, id := range []string{"bench-1", "bench-2"} {
		bench := findObject(t, f, id)
		if bench.Template != "bench/v1" {
			t.Fatalf("%s har mall %q, väntat bench/v1", id, bench.Template)
		}
		if n := countPrefix(bench.Inputs, "soilMoisture."); n < 2 {
			t.Fatalf("%s ska repetera soilMoisture.* (>=2), har %v", id, bench.Inputs)
		}
		if bench.PartOf != "greenhouse-a" {
			t.Fatalf("%s saknar partOf greenhouse-a (har %q)", id, bench.PartOf)
		}
	}
}

// A7: sewer-1 har distance + overflowTime (bräddningstid via activeTime).
func TestA7_Sewer1_DistancePlusOverflowTime(t *testing.T) {
	f := loadFixture(t, "sewer.json")

	tmpl := findTemplate(t, f, "sewer/v1")
	if !contains(tmpl.Required, "distance") || !contains(tmpl.Required, "overflowTime") {
		t.Fatalf("sewer/v1 ska kräva distance + overflowTime (har %v)", tmpl.Required)
	}
	operators := append([]string{}, tmpl.Operators...)
	if tmpl.Operator != "" {
		operators = append(operators, tmpl.Operator)
	}
	if !contains(operators, "activeTime") {
		t.Fatalf("sewer/v1 ska använda activeTime (har operators=%v operator=%q)", tmpl.Operators, tmpl.Operator)
	}

	sewer := findObject(t, f, "sewer-1")
	if !contains(sewer.Inputs, "distance") {
		t.Fatalf("sewer-1 saknar distance i inputs (har %v)", sewer.Inputs)
	}
	if !contains(sewer.Outputs, "overflowTime") {
		t.Fatalf("sewer-1 saknar overflowTime i outputs (har %v)", sewer.Outputs)
	}
}
