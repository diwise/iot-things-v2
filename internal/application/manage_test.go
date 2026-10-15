package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	pkgtypes "github.com/diwise/iot-things-v2/pkg/types"
)

const (
	mgHumURN = "urn:oma:lwm2m:ext:3304"
	mgLuxURN = "urn:oma:lwm2m:ext:3301"
)

func mgmtSetup(t *testing.T) (*Manager, *catalog.Catalog, *fakeStore, *Runtime) {
	t.Helper()
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := catalog.New()
	mgr := NewManager(cat, rt, fs)
	return mgr, cat, fs, rt
}

func mgmtWastebin(t *testing.T, mgr *Manager) {
	t.Helper()
	ctx := context.Background()
	err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required: []string{"distance", "level", "fillRate"},
			Category: "wastebin",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
			{Name: "fill-rate", Operator: "fillRate", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"fillRate"},
				Params: []string{"usableHeight"}},
		},
	})
	if err != nil {
		t.Fatalf("publish wastebin: %v", err)
	}
	err = mgr.PublishVariant(ctx, catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 1.2},
	}})
	if err != nil {
		t.Fatalf("publish variant: %v", err)
	}
	// Dubblettpublicering nekas (oföränderlighet).
	if err := mgr.PublishVariant(ctx, catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
	}}); err == nil {
		t.Fatal("duplicate variant publish must conflict")
	}
}

func mgmtBinObject() catalog.ObjectSpec {
	obj := rtBinObject()
	obj.Name = "Soptunna 42"
	return obj
}

// Soptunneflödet: publicera -> förhandsgranska -> skapa -> mata -> läs.
func TestManageBinFlow(t *testing.T) {
	mgr, _, fs, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)

	if len(mgr.ListTemplates(ctx, "")) != 1 || len(mgr.ListVariants(ctx)) != 1 {
		t.Fatal("catalog list must show published versions")
	}
	preview, err := mgr.PreviewObject(ctx, "t", mgmtBinObject())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if preview.ParamValues["usableHeight"] != 1.2 || preview.ParamSources["usableHeight"] != things.ParamSourceVariant {
		t.Fatalf("preview params=%+v", preview)
	}
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err == nil {
		t.Fatal("duplicate create must conflict")
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Name != "Soptunna 42" || sum.Revision != 1 {
		t.Fatalf("summary=%+v", sum)
	}
	fill := sum.Values["fillRate"]
	if fill.Value == nil || *fill.Value < 93.32 || *fill.Value > 93.34 {
		t.Fatalf("fillRate=%+v", fill)
	}
	if fill.Quality != "ok" {
		t.Fatalf("quality=%q", fill.Quality)
	}
	if sum.ParamSources["sensorToBottom"] != things.ParamSourceThing {
		t.Fatalf("param sources=%+v", sum.ParamSources)
	}
	hist, err := mgr.History(ctx, "t", "bin-42", "fillRate", time.Time{}, time.Time{}, 10, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("history=%d, want 1", len(hist))
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("stored history=%d, want 3", n)
	}
}

// Sensorbyte: samma mätpunkt och historik består, nya observationer visar
// den nya källan; gamla sensorn ignoreras.
func TestManageSensorSwap(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	swapped := []things.Binding{
		{DeviceID: "milesight:215", Object: rtDistURN, Resource: "5700", Input: "distance"},
	}
	if _, err := mgr.SwapBindings(ctx, "t", "bin-42", swapped, 1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Revision != 2 {
		t.Fatalf("revision=%d, want 2 after swap", sum.Revision)
	}
	at := base.Add(time.Minute)
	// Gamla sensorn: ingen effekt. Nya sensorn: ny nivå.
	if _, err := rt.HandleBatch(ctx, "t", "m2", []things.Observation{
		rtDistObs("milesight:214", 0.10, at),
		rtDistObs("milesight:215", 0.50, at),
	}, at); err != nil {
		t.Fatalf("batch: %v", err)
	}
	sum, _ = mgr.GetObject(ctx, "t", "bin-42")
	// Nivå = 1.42 - 0.50 = 0.92 (nya sensorn), inte 1.32 (gamla).
	if got := *sum.Values["level"].Value; got < 0.919 || got > 0.921 {
		t.Fatalf("level=%v, want 0.92 from new sensor", got)
	}
	if got := sum.Values["distance"].Source; got != "milesight:215/3330/5700" {
		t.Fatalf("source=%q, want new sensor", got)
	}
}

// Rumsflödet: två rum, samma mall, olika konfiguration; valfri medeltemperatur.
func mgmtRooms(t *testing.T, mgr *Manager) {
	t.Helper()
	ctx := context.Background()
	err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "room", Version: "v1",
			RepeatGroups: []string{"temperature."},
			Optional:     []string{"humidity", "illuminance", "temperature.average"},
			Category:     "room",
			Relations: []things.RelationSpec{
				{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
			},
		},
		Recipes: []catalog.Recipe{
			{Name: "mean-temp", Operator: "mean", Version: "v1",
				Inputs:  []string{"temperature.window", "temperature.center", "temperature.door"},
				Outputs: []string{"temperature.average"}},
		},
	})
	if err != nil {
		t.Fatalf("publish room: %v", err)
	}
	err = mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1", Category: "building",
	}})
	if err != nil {
		t.Fatalf("publish building: %v", err)
	}
}

func TestManageRoomVariants(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	roomA := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Kontorslandskap A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window":  {DisplayName: "Vid fönstret"},
			"temperature.center":  {DisplayName: "Mitt i rummet"},
			"temperature.door":    {DisplayName: "Vid dörren"},
			"temperature.average": {DisplayName: "Medeltemperatur"},
		},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s2", Object: rtTempURN, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "s3", Object: rtTempURN, Resource: "5700", Input: "temperature.door"},
		},
	}
	roomB := catalog.ObjectSpec{
		ThingID: "room-b", Tenant: "t", Name: "Kontorslandskap B",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {},
			"temperature.center": {},
			"humidity":           {},
			"illuminance":        {},
		},
		Bindings: []things.Binding{
			{DeviceID: "s4", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "s5", Object: rtTempURN, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "s6", Object: mgHumURN, Resource: "5700", Input: "humidity"},
			{DeviceID: "s6", Object: mgLuxURN, Resource: "5700", Input: "illuminance"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", roomA); err != nil {
		t.Fatalf("create room-a: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", roomB); err != nil {
		t.Fatalf("create room-b without new template: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	_, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{
		tempObs("s1", 18, base), tempObs("s2", 21, base), tempObs("s3", 24, base),
	}, base)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "room-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := *sum.Values["temperature.average"].Value; got != 21 {
		t.Fatalf("average=%v, want 21", got)
	}
	if sum.Values["temperature.window"].DisplayName != "Vid fönstret" {
		t.Fatalf("display name lost: %+v", sum.Values["temperature.window"])
	}
	// Valfri medeltemperatur kan aktiveras för rum B utan att bryta direktvärden.
	got, err := mgr.GetObject(ctx, "t", "room-b")
	if err != nil {
		t.Fatalf("get room-b: %v", err)
	}
	if _, ok := got.Values["humidity"]; !ok {
		t.Fatal("room-b humidity missing")
	}
}

// Byggnadsöversikt: rum kopplas till byggnad, värden syns tillsammans.
func TestManageBuildingOverview(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Byggnad A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create building: %v", err)
	}
	mkRoom := func(id, parent string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: id,
			Location: rtLoc(), Metadata: rtMeta(),
			TemplateID: "room", TemplateVersion: "v1",
			Properties: map[string]things.Property{"temperature.window": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-" + id, Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			},
			Relations: []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}},
		}
	}
	if _, err := mgr.CreateObject(ctx, "t", mkRoom("room-a", "building-a")); err != nil {
		t.Fatalf("create room-a: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", mkRoom("room-b", "building-a")); err != nil {
		t.Fatalf("create room-b: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkObs := func(device string, v float64) things.Observation {
		return things.Observation{
			Signal: things.ParseSignalID(device, "", rtTempURN, "5700"),
			Value:  &v, Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base,
		}
	}
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{
		mkObs("s-room-a", 20), mkObs("s-room-b", 22),
	}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	ov, err := mgr.Overview(ctx, "t", "building-a")
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if len(ov.Children) != 2 {
		t.Fatalf("children=%d, want 2", len(ov.Children))
	}
	for _, child := range ov.Children {
		if child.Values["temperature.window"].Value == nil {
			t.Fatalf("child %q lacks current value", child.ThingID)
		}
	}
	// Flytt via manager bevarar värden.
	if err := mgr.MoveObject(ctx, "t", "room-b", "building-a", 1); err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "building-b", Tenant: "t", Name: "Byggnad B",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create building-b: %v", err)
	}
	if err := mgr.MoveObject(ctx, "t", "room-b", "building-b", 2); err != nil {
		t.Fatalf("move: %v", err)
	}
	ov, _ = mgr.Overview(ctx, "t", "building-b")
	if len(ov.Children) != 1 || *ov.Children[0].Values["temperature.window"].Value != 22 {
		t.Fatalf("moved overview=%+v", ov)
	}
	// Delete med barn blockeras.
	if err := mgr.DeleteObject(ctx, "t", "building-b"); err == nil {
		t.Fatal("delete with children must conflict")
	}
}

// Växthusöversikt: bänkar med egna mätpunkter + växthusets egna punkter.
func TestManageGreenhouseOverview(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "greenhouse", Version: "v1",
		RepeatGroups: []string{"airTemp."},
		Category:     "greenhouse",
	}})
	if err != nil {
		t.Fatalf("publish greenhouse: %v", err)
	}
	err = mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "bench", Version: "v1",
		RepeatGroups: []string{"soilMoisture."},
		Optional:     []string{"airTemp.bench"},
		Category:     "bench",
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"greenhouse"}, Min: 0, Max: 1},
		},
	}})
	if err != nil {
		t.Fatalf("publish bench: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "greenhouse-a", Tenant: "t", Name: "Växthus A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "greenhouse", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"airTemp.common": {DisplayName: "Gemensam lufttemperatur"},
		},
		Bindings: []things.Binding{
			{DeviceID: "gh", Object: rtTempURN, Resource: "5700", Input: "airTemp.common"},
		},
	}); err != nil {
		t.Fatalf("create greenhouse: %v", err)
	}
	for _, bench := range []string{"bench-1", "bench-2"} {
		obj := catalog.ObjectSpec{
			ThingID: bench, Tenant: "t", Name: bench,
			Location: rtLoc(), Metadata: rtMeta(),
			TemplateID: "bench", TemplateVersion: "v1",
			Properties: map[string]things.Property{
				"soilMoisture.1": {}, "airTemp.bench": {},
			},
			Bindings: []things.Binding{
				{DeviceID: bench, Object: mgHumURN, Resource: "5700", Input: "soilMoisture.1"},
				{DeviceID: bench, Object: rtTempURN, Resource: "5700", Input: "airTemp.bench"},
			},
			Relations: []catalog.RelationRef{{Name: "partOf", TargetThingID: "greenhouse-a", TargetTemplateID: "greenhouse"}},
		}
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", bench, err)
		}
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	mkObs := func(device, urn string, v float64) things.Observation {
		return things.Observation{
			Signal: things.ParseSignalID(device, "", urn, "5700"),
			Value:  &v, Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base,
		}
	}
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{
		mkObs("gh", rtTempURN, 19),
		mkObs("bench-1", mgHumURN, 42),
		mkObs("bench-1", rtTempURN, 20),
		mkObs("bench-2", mgHumURN, 47),
		mkObs("bench-2", rtTempURN, 21),
	}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	ov, err := mgr.Overview(ctx, "t", "greenhouse-a")
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if got := *ov.Thing.Values["airTemp.common"].Value; got != 19 {
		t.Fatalf("greenhouse own value=%v, want 19", got)
	}
	if len(ov.Children) != 2 {
		t.Fatalf("children=%d, want 2", len(ov.Children))
	}
	for _, child := range ov.Children {
		if child.Values["soilMoisture.1"].Value == nil || child.Values["airTemp.bench"].Value == nil {
			t.Fatalf("bench %q lacks values: %+v", child.ThingID, child.Values)
		}
	}
}

// Uppgradering: preview visar diff, konflikter blockerar utan force.
func TestManageUpgradeBlockedThenForced(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	obj := mgmtBinObject()
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	v2 := catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v2",
			Required: []string{"distance", "level", "fillRate"},
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v2",
				Inputs: []string{"distance"}, Outputs: []string{"level"}},
		},
	}
	if err := mgr.PublishTemplate(ctx, v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	diff, conflicts, err := mgr.PreviewUpgrade(ctx, "t", "bin-42", "wastebin", "v2", "", "")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(conflicts) == 0 {
		t.Fatal("removed sensorToBottom override must conflict")
	}
	if _, _, err := mgr.UpgradeObject(ctx, "t", "bin-42", "wastebin", "v2", "", "", false, 1); err == nil {
		t.Fatal("upgrade with conflicts must be blocked without force")
	}
	diff2, _, err := mgr.UpgradeObject(ctx, "t", "bin-42", "wastebin", "v2", "", "", true, 1)
	if err != nil {
		t.Fatalf("forced upgrade: %v", err)
	}
	if len(diff2.RemovedParams) == 0 && len(diff.RemovedParams) == 0 {
		t.Fatal("diff must show removed param")
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.TemplateVersion != "v2" || sum.Revision != 2 {
		t.Fatalf("summary=%+v, want v2/rev 2", sum)
	}
}

// Sök + historikgränser + tenantisolering.
func TestManageSearchHistoryTenants(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	mgmtRooms(t, mgr)
	obj := mgmtBinObject()
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create bin: %v", err)
	}
	roomObj := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Rum A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", roomObj); err != nil {
		t.Fatalf("create room: %v", err)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{}); len(got) != 2 {
		t.Fatalf("search all=%d, want 2", len(got))
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{TemplateID: "wastebin"}); len(got) != 1 {
		t.Fatalf("search wastebin=%d, want 1", len(got))
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Limit: 1}); len(got) != 1 {
		t.Fatalf("search limit=%d, want 1", len(got))
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		at := base.Add(time.Duration(i) * time.Minute)
		id := fmt.Sprintf("h%d", i)
		if _, err := rt.HandleBatch(ctx, "t", id, []things.Observation{rtDistObs("milesight:214", 0.3+float64(i)*0.1, at)}, at); err != nil {
			t.Fatalf("batch: %v", err)
		}
	}
	// Tre rundor -> tre distansrader (olika mättid/värde).
	gotHist, err := mgr.History(ctx, "t", "bin-42", "distance", time.Time{}, time.Time{}, 0, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(gotHist) != 3 {
		t.Fatalf("distance history=%d, want 3", len(gotHist))
	}
	// Tidsfönster över två rundor x tre egenskaper = 6 rader.
	gotHist, err = mgr.History(ctx, "t", "bin-42", "", base.Add(time.Minute), base.Add(2*time.Minute), 0, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(gotHist) != 6 {
		t.Fatalf("ranged history=%d, want 6", len(gotHist))
	}
	gotHist, err = mgr.History(ctx, "t", "bin-42", "", time.Time{}, time.Time{}, 2, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(gotHist) != 2 {
		t.Fatalf("limited history=%d, want 2", len(gotHist))
	}
	if _, err := mgr.GetObject(ctx, "other", "bin-42"); err == nil {
		t.Fatal("cross-tenant read must fail")
	}
	if _, err := mgr.PreviewObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "bad", TemplateID: "wastebin", TemplateVersion: "v1",
		Properties: map[string]things.Property{"nope": {}},
	}); err == nil {
		t.Fatal("preview of invalid config must fail")
	}
}

// Regression P5: historik för okänt thing ska ge NotFound (404), inte tom 200-lista.
func TestManageHistoryUnknownThingIsNotFound(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	if _, err := mgr.History(ctx, "t", "missing", "", time.Time{}, time.Time{}, 0, 0); err == nil {
		t.Fatal("history for unknown thing must fail")
	} else if !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("history unknown err=%v, want not found", err)
	}
}

// Regression B13/P5: sen observation ska synas i historik med Late=true
// utan att bli aktuellt värde.
func TestManageLateHistoryFlag(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	late := base.Add(-time.Hour)
	if _, err := rt.HandleBatch(ctx, "t", "m2", []things.Observation{rtDistObs("milesight:214", 0.10, late)}, base.Add(time.Minute)); err != nil {
		t.Fatalf("late batch: %v", err)
	}
	hist, err := mgr.History(ctx, "t", "bin-42", "distance", time.Time{}, time.Time{}, 0, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("distance history=%d, want 2", len(hist))
	}
	lateCount := 0
	for _, h := range hist {
		if h.Late {
			lateCount++
		}
	}
	if lateCount != 1 {
		t.Fatalf("late rows=%d, want 1 (%+v)", lateCount, hist)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := *sum.Values["distance"].Value; got != 0.30 {
		t.Fatalf("current distance=%v, want 0.30 (late must not win)", got)
	}
}

// Regression P7/B19: flytt kräver aktuell revision, bumpar revision och
// synkar lagrad config så att efterföljande swap/upgrade inte återställer
// den gamla föräldern.
func TestManageMoveRevisionAndConfigSync(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	for _, id := range []string{"building-a", "building-b"} {
		if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: id,
			Location: rtLoc(), Metadata: rtMeta(),
			TemplateID: "building", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	// Fristående rum (ingen partOf) flyttas till building-a med revision 1.
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "room-x", Tenant: "t", Name: "Rum X",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {DisplayName: "Fönster"}},
		Bindings: []things.Binding{
			{DeviceID: "sx", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}); err != nil {
		t.Fatalf("create room-x: %v", err)
	}
	if err := mgr.MoveObject(ctx, "t", "room-x", "building-a", 99); err == nil {
		t.Fatal("stale move revision must conflict")
	}
	if err := mgr.MoveObject(ctx, "t", "room-x", "building-a", 1); err != nil {
		t.Fatalf("move parentless: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "room-x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Revision != 2 {
		t.Fatalf("revision=%d, want 2 after move", sum.Revision)
	}
	ov, err := mgr.Overview(ctx, "t", "building-a")
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if len(ov.Children) != 1 || ov.Children[0].ThingID != "room-x" {
		t.Fatalf("overview=%+v, want room-x under building-a", ov)
	}
	// Swap efter flytt får inte tappa föräldern eller visningsnamnet.
	swapped := []things.Binding{
		{DeviceID: "sx2", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
	}
	if _, err := mgr.SwapBindings(ctx, "t", "room-x", swapped, 2); err != nil {
		t.Fatalf("swap after move: %v", err)
	}
	ov, _ = mgr.Overview(ctx, "t", "building-a")
	if len(ov.Children) != 1 || ov.Children[0].ThingID != "room-x" {
		t.Fatalf("parent lost after swap: %+v", ov)
	}
	sum, _ = mgr.GetObject(ctx, "t", "room-x")
	if sum.Values["temperature.window"].DisplayName != "Fönster" {
		t.Fatalf("display name lost after move+swap: %+v", sum.Values["temperature.window"])
	}
	if err := mgr.MoveObject(ctx, "t", "room-x", "missing-parent", 3); err == nil {
		t.Fatal("move to unknown parent must fail")
	}
}

// Regression T8/P8: force-uppgradering bevarar namn, visningsnamn och
// överlevande overrides; borttagna parametrar tappas explicit (synliga i
// preview-konflikter), revision bumpas.
func TestManageUpgradeForceKeepsIdentity(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	obj := mgmtBinObject()
	obj.Properties["distance"] = things.Property{DisplayName: "Avstånd"}
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	v2 := catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v2",
			Required: []string{"distance", "level", "fillRate"},
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v2",
				Inputs: []string{"distance"}, Outputs: []string{"level"}},
		},
	}
	if err := mgr.PublishTemplate(ctx, v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	_, conflicts, err := mgr.PreviewUpgrade(ctx, "t", "bin-42", "wastebin", "v2", "", "")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if len(conflicts) == 0 {
		t.Fatal("removed sensorToBottom override must conflict")
	}
	if _, _, err := mgr.UpgradeObject(ctx, "t", "bin-42", "wastebin", "v2", "", "", true, 1); err != nil {
		t.Fatalf("forced upgrade: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Name != "Soptunna 42" {
		t.Fatalf("name=%q, want preserved", sum.Name)
	}
	if sum.Values["distance"].DisplayName != "Avstånd" {
		t.Fatalf("display name lost: %+v", sum.Values["distance"])
	}
	if sum.TemplateVersion != "v2" || sum.Revision != 2 {
		t.Fatalf("summary=%+v, want v2/rev 2", sum)
	}
	if _, ok := sum.ParamValues["sensorToBottom"]; ok {
		t.Fatalf("removed param must be dropped after force: %+v", sum.ParamValues)
	}
	if sum.ParamValues["usableHeight"] != 1.0 {
		t.Fatalf("variant dropped by explicit empty newVariant: usableHeight=%v, want template default 1.0", sum.ParamValues["usableHeight"])
	}
	if src := sum.ParamSources["usableHeight"]; src != things.ParamSourceTemplate {
		t.Fatalf("usableHeight source=%q, want template after variant removal", src)
	}
}

// Regression P2: preview muterar inget (skapar inget thing, bumpar ingen revision).
func TestManagePreviewNoMutate(t *testing.T) {
	mgr, _, fs, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	before := len(fs.ListThingIDs(context.Background(), "t"))
	if _, err := mgr.PreviewObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("preview: %v", err)
	}
	if got := len(fs.ListThingIDs(context.Background(), "t")); got != before {
		t.Fatalf("preview created things: %d -> %d", before, got)
	}
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	sumBefore, _ := mgr.GetObject(ctx, "t", "bin-42")
	up := mgmtBinObject()
	up.Name = "Ändrat namn"
	if _, err := mgr.PreviewObject(ctx, "t", up); err != nil {
		t.Fatalf("preview update: %v", err)
	}
	sumAfter, _ := mgr.GetObject(ctx, "t", "bin-42")
	if sumAfter.Revision != sumBefore.Revision || sumAfter.Name != sumBefore.Name {
		t.Fatalf("preview mutated: %+v -> %+v", sumBefore, sumAfter)
	}
}

// Regression JSON: värdet 0.0 får inte tappas av omitempty (pekare, ej nollvärde).
func TestManageZeroValuePreserved(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.0, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	d := sum.Values["distance"]
	if d.Value == nil || *d.Value != 0.0 {
		t.Fatalf("distance=%+v, want non-nil 0.0", d)
	}
	raw, err := json.Marshal(sum)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	vals, _ := decoded["values"].(map[string]any)
	dist, _ := vals["distance"].(map[string]any)
	v, ok := dist["value"].(float64)
	if !ok || v != 0.0 {
		t.Fatalf("JSON drops value 0.0: %s", raw)
	}
	if !strings.Contains(string(raw), `"propertyId":"distance"`) {
		t.Fatalf("JSON missing propertyId: %s", raw)
	}
}

// Regression B10/H9: overview i annan tenant ska ge NotFound, aldrig förälder+barn.
func TestManageOverviewCrossTenantIsNotFound(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "building-a", Tenant: "t", Name: "Byggnad A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mgr.Overview(ctx, "other", "building-a"); err == nil {
		t.Fatal("cross-tenant overview must fail")
	}
}

// memCatalogStore är ett testbart catalog.Store i minnet.
type memCatalogStore struct {
	templates []catalog.TemplateSpec
	variants  []catalog.VariantSpec
}

func (s *memCatalogStore) SaveTemplate(spec catalog.TemplateSpec) error {
	for _, prev := range s.templates {
		if prev.Template.ID == spec.Template.ID && prev.Template.Version == spec.Template.Version {
			return fmt.Errorf("%w: template %s/%s already published",
				things.ErrConflict, spec.Template.ID, spec.Template.Version)
		}
	}
	s.templates = append(s.templates, spec)
	return nil
}

func (s *memCatalogStore) SaveVariant(spec catalog.VariantSpec) error {
	for _, prev := range s.variants {
		if prev.Variant.ID == spec.Variant.ID && prev.Variant.Version == spec.Variant.Version {
			return fmt.Errorf("%w: variant %s/%s already published",
				things.ErrConflict, spec.Variant.ID, spec.Variant.Version)
		}
	}
	s.variants = append(s.variants, spec)
	return nil
}

func (s *memCatalogStore) LoadTemplates() []catalog.TemplateSpec {
	return append([]catalog.TemplateSpec(nil), s.templates...)
}

func (s *memCatalogStore) LoadVariants() []catalog.VariantSpec {
	return append([]catalog.VariantSpec(nil), s.variants...)
}

// Beständig katalog: publicering skriver igenom till storet och en ny
// katalog återställd från storet kan skapa saker utan ompublicering
// (omstartsprotokollet i cmd/newApp).
func TestManageCatalogPersistence(t *testing.T) {
	ctx := context.Background()
	cs := &memCatalogStore{}
	fs := newFakeStore()
	mgr := NewManager(catalog.New(), NewRuntime(fs), fs, WithCatalogStore(cs))

	tmpl := catalog.TemplateSpec{
		Template:      things.Template{ID: "wastebin", Version: "v1", Required: []string{"distance"}, Category: "wastebin"},
		ParamDefaults: map[string]float64{"usableHeight": 1.0},
	}
	if err := mgr.PublishTemplate(ctx, tmpl); err != nil {
		t.Fatalf("publish: %v", err)
	}
	variant := catalog.VariantSpec{Variant: things.Variant{
		ID: "wastebin-B", Version: "v1",
		TemplateID: "wastebin", TemplateVersion: "v1",
		ParamValues: map[string]float64{"usableHeight": 1.2},
	}}
	if err := mgr.PublishVariant(ctx, variant); err != nil {
		t.Fatalf("publish variant: %v", err)
	}
	if len(cs.templates) != 1 || len(cs.variants) != 1 {
		t.Fatalf("write-through templates=%d variants=%d, want 1/1", len(cs.templates), len(cs.variants))
	}

	restored := catalog.New()
	restored.Restore(cs.LoadTemplates(), cs.LoadVariants())
	mgr2 := NewManager(restored, NewRuntime(fs), fs, WithCatalogStore(cs))
	if _, err := mgr2.GetTemplate(ctx, "wastebin", "v1"); err != nil {
		t.Fatalf("restored template: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "bin-42", TemplateID: "wastebin", TemplateVersion: "v1",
		Name: "Soptunna 42", Location: rtLoc(), Metadata: rtMeta(),
		VariantID: "wastebin-B", VariantVersion: "v1",
		Properties: map[string]things.Property{"distance": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "distance"},
		},
	}
	if _, err := mgr2.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create from restored catalog: %v", err)
	}
}

// Historik-paging: offset hoppar över de äldsta raderna (äldst först).
func TestManageHistoryOffset(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		at := base.Add(time.Duration(i) * time.Minute)
		if _, err := rt.HandleBatch(ctx, "t", fmt.Sprintf("m%d", i),
			[]things.Observation{rtDistObs("milesight:214", 0.3+float64(i)*0.1, at)}, at); err != nil {
			t.Fatalf("batch: %v", err)
		}
	}
	all, err := mgr.History(ctx, "t", "bin-42", "distance", time.Time{}, time.Time{}, 0, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("history=%d, want 3", len(all))
	}
	page, err := mgr.History(ctx, "t", "bin-42", "distance", time.Time{}, time.Time{}, 1, 1)
	if err != nil {
		t.Fatalf("history page: %v", err)
	}
	if len(page) != 1 || page[0].Value == nil || *page[0].Value < 0.399 || *page[0].Value > 0.401 {
		t.Fatalf("paged history=%+v, want middle row 0.4", page)
	}
	if got, err := mgr.History(ctx, "t", "bin-42", "distance", time.Time{}, time.Time{}, 0, 9); err != nil || len(got) != 0 {
		t.Fatalf("offset past end=%+v err=%v, want empty", got, err)
	}
}

// Saklistning: filter på mall/version/variant/namn + limit/offset.
func TestManageSearchFilterAndPaging(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	mgmtRooms(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("create bin: %v", err)
	}
	roomObj := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Kontorslandskap A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", roomObj); err != nil {
		t.Fatalf("create room: %v", err)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{NameContains: "SOPTUNNA"}); len(got) != 1 || got[0].ThingID != "bin-42" {
		t.Fatalf("name filter=%+v, want bin-42", got)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{VariantID: "wastebin-B"}); len(got) != 1 || got[0].ThingID != "bin-42" {
		t.Fatalf("variant filter=%+v, want bin-42", got)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{TemplateVersion: "v1"}); len(got) != 2 {
		t.Fatalf("version filter=%d, want 2", len(got))
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{TemplateID: "room", NameContains: "kontors"}); len(got) != 1 {
		t.Fatalf("combined filter=%d, want 1", len(got))
	}
	first := mgr.SearchObjects(ctx, "t", SearchFilter{Limit: 1})
	second := mgr.SearchObjects(ctx, "t", SearchFilter{Limit: 1, Offset: 1})
	if len(first) != 1 || len(second) != 1 || first[0].ThingID == second[0].ThingID {
		t.Fatalf("paging first=%+v second=%+v, want distinct pages", first, second)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Offset: 9}); len(got) != 0 {
		t.Fatalf("offset past end=%d, want 0", len(got))
	}
	taggedBin := func(id, tags string) catalog.ObjectSpec {
		obj := mgmtBinObject()
		obj.ThingID = id
		md := map[string]string{"test": "true"}
		if tags != "" {
			md["tags"] = tags
		}
		obj.Metadata = md
		return obj
	}
	for _, obj := range []catalog.ObjectSpec{
		taggedBin("bin-tag-1", "park, centrum"),
		taggedBin("bin-tag-2", "parker"),
	} {
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Tag: "park"}); len(got) != 1 || got[0].ThingID != "bin-tag-1" {
		t.Fatalf("tag park=%v, want [bin-tag-1]", got)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Tag: "centrum"}); len(got) != 1 {
		t.Fatalf("tag centrum=%d, want 1", len(got))
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Tag: "parkering"}); len(got) != 0 {
		t.Fatalf("tag parkering=%d, want 0", len(got))
	}
}

// ListTags: distincta dynamiska kategoriseringstaggar ur metadata["tags"]
// (importformat: komma-separerat). Duplikat/blanksteg/tomma slås ihop,
// sorteras och avgränsas per tenant.
func TestManageListTags(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)

	tagged := func(id, tenant, tags string) catalog.ObjectSpec {
		obj := mgmtBinObject()
		obj.ThingID = id
		obj.Tenant = tenant
		md := map[string]string{"test": "true"}
		if tags != "" {
			md["tags"] = tags
		}
		obj.Metadata = md
		return obj
	}

	for _, obj := range []catalog.ObjectSpec{
		tagged("bin-1", "t", "park, centrum"),
		tagged("bin-2", "t", "centrum ,hamn"),
		tagged("bin-3", "t", ""),
		tagged("bin-x", "other", "utanför"),
	} {
		if _, err := mgr.CreateObject(ctx, obj.Tenant, obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	got := mgr.ListTags(ctx, "t")
	want := []string{"centrum", "hamn", "park"}
	if len(got) != len(want) {
		t.Fatalf("tags=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tags=%v, want %v", got, want)
		}
	}

	if got := mgr.ListTags(ctx, "other"); len(got) != 1 || got[0] != "utanför" {
		t.Fatalf("other tags=%v, want [utanför]", got)
	}
	if got := mgr.ListTags(ctx, "empty"); len(got) != 0 {
		t.Fatalf("empty tags=%v, want []", got)
	}
}

// ExportObjects: återskapbara specs + rundtur. Exporten av två skapade
// saker ska bära mallreferenser, bindningar och saknivå-overrides (men
// inte variantärvda värden), och ska gå att skapa på en ny manager med
// samma katalog (samma export igen).
func TestManageExportObjectsRoundTrip(t *testing.T) {
	mkMgr := func(t *testing.T) *Manager {
		t.Helper()
		mgr, _, _, _ := mgmtSetup(t)
		mgmtWastebin(t, mgr)
		return mgr
	}
	mgr := mkMgr(t)
	ctx := context.Background()

	obj2 := mgmtBinObject()
	obj2.ThingID = "bin-43"
	obj2.Name = "Soptunna 43"
	for _, obj := range []catalog.ObjectSpec{mgmtBinObject(), obj2} {
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	specs := mgr.ExportObjects(ctx, "t")
	if len(specs) != 2 || specs[0].ThingID != "bin-42" || specs[1].ThingID != "bin-43" {
		t.Fatalf("exported=%v, want [bin-42 bin-43]", specs)
	}
	first := specs[0]
	if first.TemplateID != "wastebin" || first.TemplateVersion != "v1" {
		t.Fatalf("template ref=%v, want wastebin/v1", first)
	}
	if first.VariantID != "wastebin-B" || first.ParamOverrides["sensorToBottom"] != 1.42 {
		t.Fatalf("overrides=%v, want sensorToBottom=1.42", first.ParamOverrides)
	}
	if _, ok := first.ParamOverrides["usableHeight"]; ok {
		t.Fatalf("overrides=%v, variant-arvda usableHeight får inte exporteras", first.ParamOverrides)
	}
	if len(first.Bindings) != 1 || first.Bindings[0].Input != "distance" {
		t.Fatalf("bindings=%v, want distance-bindning", first.Bindings)
	}

	mgr2 := mkMgr(t)
	for _, spec := range specs {
		if _, err := mgr2.CreateObject(ctx, spec.Tenant, spec); err != nil {
			t.Fatalf("recreate %s: %v", spec.ThingID, err)
		}
	}
	again := mgr2.ExportObjects(ctx, "t")
	if len(again) != 2 || again[0].ThingID != "bin-42" || again[1].ThingID != "bin-43" {
		t.Fatalf("re-exported=%v, want [bin-42 bin-43]", again)
	}

	if got := mgr.ExportObjects(ctx, "other"); len(got) != 0 {
		t.Fatalf("other export=%v, want []", got)
	}
}

// Katalogläsning: enstaka mall/variant, okänd version ger NotFound.
func TestManageGetTemplateVariant(t *testing.T) {

	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	tmpl, err := mgr.GetTemplate(ctx, "wastebin", "v1")
	if err != nil {
		t.Fatalf("get template: %v", err)
	}
	if tmpl.Template.ID != "wastebin" || len(tmpl.Recipes) != 2 {
		t.Fatalf("template=%+v", tmpl)
	}
	variant, err := mgr.GetVariant(ctx, "wastebin-B", "v1")
	if err != nil {
		t.Fatalf("get variant: %v", err)
	}
	if variant.Variant.TemplateID != "wastebin" {
		t.Fatalf("variant=%+v", variant)
	}
	if _, err := mgr.GetTemplate(ctx, "wastebin", "v9"); !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("unknown template err=%v, want not found", err)
	}
	if _, err := mgr.GetVariant(ctx, "nope", "v1"); !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("unknown variant err=%v, want not found", err)
	}
}

// Plats/metadata/klassificering: lagras, returneras, filtreras och
// överlever uppdatering (reconstructSpec tappar dem inte).
func TestManageLocationMetadataSearch(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtWastebin(t, mgr)
	mgmtRooms(t, mgr)

	bin := mgmtBinObject()
	bin.Location = &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)}
	bin.Metadata = map[string]string{"district": "centrum"}
	bin.Category = "wastebin"
	if _, err := mgr.CreateObject(ctx, "t", bin); err != nil {
		t.Fatalf("create bin: %v", err)
	}
	roomObj := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Kontorslandskap A",
		TemplateID: "room", TemplateVersion: "v1",
		Category:   "room",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[20.20,67.90]`)},
		Metadata:   map[string]string{"district": "söder"},
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", roomObj); err != nil {
		t.Fatalf("create room: %v", err)
	}

	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Location == nil || sum.Location.Type != "Point" {
		t.Fatalf("location=%+v, want point", sum.Location)
	}
	if sum.Metadata["district"] != "centrum" || sum.Category != "wastebin" {
		t.Fatalf("summary=%+v", sum)
	}

	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Category: "room"}); len(got) != 1 || got[0].ThingID != "room-a" {
		t.Fatalf("category filter=%+v, want room-a", got)
	}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Metadata: map[string]string{"district": "centrum"}}); len(got) != 1 || got[0].ThingID != "bin-42" {
		t.Fatalf("metadata filter=%+v, want bin-42", got)
	}
	stockholm := things.BBox{MinLon: 17, MinLat: 59, MaxLon: 19, MaxLat: 60}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{BBox: &stockholm}); len(got) != 1 || got[0].ThingID != "bin-42" {
		t.Fatalf("bbox filter=%+v, want bin-42", got)
	}
	farNorth := things.BBox{MinLon: 17, MinLat: 65, MaxLon: 19, MaxLat: 68}
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{BBox: &farNorth}); len(got) != 0 {
		t.Fatalf("far bbox=%d, want 0", len(got))
	}

	// Uppdatering med nytt namn tappar inte plats/metadata.
	upd := bin
	upd.Name = "Soptunna 42 flyttad"
	if _, err := mgr.UpdateObject(ctx, "t", upd, 1); err != nil {
		t.Fatalf("update: %v", err)
	}
	sum, err = mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if sum.Location == nil || sum.Metadata["district"] != "centrum" {
		t.Fatalf("update dropped location/metadata: %+v", sum)
	}
}

// Underträd: depth gränsar barnnivåer (0 = bara saken), för djupt nekas.
func TestManageSubtree(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "probe", Version: "v1",
			Category: "probe",
			Relations: []things.RelationSpec{
				{Name: "partOf", AllowedTargets: []string{"room"}, Min: 0, Max: 1},
			},
		},
	}); err != nil {
		t.Fatalf("publish probe: %v", err)
	}
	mkRoom := func(id, parent string) catalog.ObjectSpec {
		obj := catalog.ObjectSpec{
			ThingID: id, TemplateID: "room", TemplateVersion: "v1",
			Name: id, Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{"temperature.window": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-" + id, Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			},
		}
		if parent != "" {
			obj.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		}
		return obj
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "building-a", TemplateID: "building", TemplateVersion: "v1",
		Name: "Byggnad A", Location: rtLoc(), Metadata: rtMeta(),
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create building: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", mkRoom("room-a", "building-a")); err != nil {
		t.Fatalf("create room: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "probe-1", TemplateID: "probe", TemplateVersion: "v1",
		Name: "Prob 1", Location: rtLoc(), Metadata: rtMeta(),
		Properties: map[string]things.Property{},
		Relations:  []catalog.RelationRef{{Name: "partOf", TargetThingID: "room-a", TargetTemplateID: "room"}},
	}); err != nil {
		t.Fatalf("create probe: %v", err)
	}

	node, err := mgr.Subtree(ctx, "t", "building-a", 0)
	if err != nil {
		t.Fatalf("depth 0: %v", err)
	}
	if node.Thing.ThingID != "building-a" || len(node.Children) != 0 {
		t.Fatalf("depth 0=%+v, want bare building", node)
	}
	node, err = mgr.Subtree(ctx, "t", "building-a", 1)
	if err != nil {
		t.Fatalf("depth 1: %v", err)
	}
	if len(node.Children) != 1 || len(node.Children[0].Children) != 0 {
		t.Fatalf("depth 1=%+v, want room without probe", node)
	}
	node, err = mgr.Subtree(ctx, "t", "building-a", 2)
	if err != nil {
		t.Fatalf("depth 2: %v", err)
	}
	if len(node.Children) != 1 || len(node.Children[0].Children) != 1 ||
		node.Children[0].Children[0].Thing.ThingID != "probe-1" {
		t.Fatalf("depth 2=%+v, want room with probe", node)
	}
	if _, err := mgr.Subtree(ctx, "t", "building-a", 99); err == nil {
		t.Fatal("excessive depth must fail")
	}
	if _, err := mgr.Subtree(ctx, "t", "missing", 1); !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("unknown subtree err=%v, want not found", err)
	}
	if _, err := mgr.Subtree(ctx, "other", "building-a", 1); !errors.Is(err, things.ErrNotFound) {
		t.Fatal("cross-tenant subtree must fail")
	}
}

// Bortkoppling: förälder försvinner, barnet (ID/bindningar/historik) består,
// revision bumpas; andra gången ger NotFound.
func TestManageUnlink(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	mgmtRooms(t, mgr)
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "building-a", TemplateID: "building", TemplateVersion: "v1",
		Name: "Byggnad A", Location: rtLoc(), Metadata: rtMeta(),
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create building: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "room-a", TemplateID: "room", TemplateVersion: "v1",
		Name: "Rum A", Location: rtLoc(), Metadata: rtMeta(),
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
		Relations: []catalog.RelationRef{{Name: "partOf", TargetThingID: "building-a", TargetTemplateID: "building"}},
	}); err != nil {
		t.Fatalf("create room: %v", err)
	}
	if err := mgr.UnlinkObject(ctx, "t", "room-a", 99); !errors.Is(err, things.ErrConflict) {
		t.Fatalf("stale unlink err=%v, want conflict", err)
	}
	if err := mgr.UnlinkObject(ctx, "t", "room-a", 1); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "room-a")
	if err != nil {
		t.Fatalf("get after unlink: %v", err)
	}
	if sum.Revision != 2 {
		t.Fatalf("revision=%d, want 2", sum.Revision)
	}
	ov, err := mgr.Overview(ctx, "t", "building-a")
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if len(ov.Children) != 0 {
		t.Fatalf("children=%d, want 0 after unlink", len(ov.Children))
	}
	if err := mgr.UnlinkObject(ctx, "t", "room-a", 2); !errors.Is(err, things.ErrNotFound) {
		t.Fatalf("second unlink err=%v, want not found", err)
	}
}

// Bool-typerna som ren konfiguration: desk/lifebuoy (presence),
// passage (räknare + dörr), pumpingstation (bool -> aktiv tid),
// watermeter (volym + flaggor) och sewer (nivå + bräddningstid).
func TestManageBoolTypes(t *testing.T) {
	const (
		dinURN = "urn:oma:lwm2m:ext:3200"
		wmURN  = "urn:oma:lwm2m:ext:3424"
	)
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	publish := func(spec catalog.TemplateSpec) {
		t.Helper()
		if err := mgr.PublishTemplate(ctx, spec); err != nil {
			t.Fatalf("publish %s: %v", spec.Template.ID, err)
		}
	}
	publish(catalog.TemplateSpec{Template: things.Template{
		ID: "desk", Version: "v1", Required: []string{"presence"}, Category: "desk",
	}})
	publish(catalog.TemplateSpec{Template: things.Template{
		ID: "lifebuoy", Version: "v1", Required: []string{"presence"}, Category: "lifebuoy",
	}})
	publish(catalog.TemplateSpec{
		Template: things.Template{ID: "passage", Version: "v1", Required: []string{"door", "passages"}, Category: "passage"},
		Recipes: []catalog.Recipe{{Name: "count", Operator: "counter", Version: "v1",
			Inputs: []string{"door"}, Outputs: []string{"passages"}}},
	})
	publish(catalog.TemplateSpec{
		Template: things.Template{ID: "pumpingstation", Version: "v1", Required: []string{"run", "runTime"}, Category: "pumpingstation"},
		Recipes: []catalog.Recipe{{Name: "run-time", Operator: "activeTime", Version: "v1",
			Inputs: []string{"run"}, Outputs: []string{"runTime"}}},
	})
	publish(catalog.TemplateSpec{Template: things.Template{
		ID: "watermeter", Version: "v1", Required: []string{"volume", "leakage", "backflow", "fraud"}, Category: "watermeter",
	}})
	publish(catalog.TemplateSpec{
		Template: things.Template{ID: "sewer", Version: "v1", Required: []string{"distance", "overflow", "overflowTime"}, Category: "sewer"},
		Recipes: []catalog.Recipe{{Name: "overflow-time", Operator: "activeTime", Version: "v1",
			Inputs: []string{"overflow"}, Outputs: []string{"overflowTime"}}},
	})

	mkBoolThing := func(id, tmpl, prop, device string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, TemplateID: tmpl, TemplateVersion: "v1",
			Name: id, Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{prop: {}},
			Bindings: []things.Binding{
				{DeviceID: device, Object: dinURN, Resource: "5500", Input: prop},
			},
		}
	}
	for _, obj := range []catalog.ObjectSpec{
		mkBoolThing("desk-1", "desk", "presence", "s-desk"),
		mkBoolThing("lifebuoy-1", "lifebuoy", "presence", "s-lifebuoy"),
		{
			ThingID: "passage-1", TemplateID: "passage", TemplateVersion: "v1",
			Name: "Passage 1", Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{"door": {}, "passages": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-pass", Object: dinURN, Resource: "5500", Input: "door"},
			},
		},
		{
			ThingID: "pump-1", TemplateID: "pumpingstation", TemplateVersion: "v1",
			Name: "Pump 1", Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{"run": {}, "runTime": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-pump", Object: dinURN, Resource: "5500", Input: "run"},
			},
		},
		{
			ThingID: "water-1", TemplateID: "watermeter", TemplateVersion: "v1",
			Name: "Mätare 1", Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{
				"volume": {}, "leakage": {}, "backflow": {}, "fraud": {},
			},
			Bindings: []things.Binding{
				{DeviceID: "s-wm", Object: wmURN, Resource: "1", Input: "volume"},
				{DeviceID: "s-wm", Object: wmURN, Resource: "10", Input: "leakage"},
				{DeviceID: "s-wm", Object: wmURN, Resource: "11", Input: "backflow"},
				{DeviceID: "s-wm", Object: wmURN, Resource: "13", Input: "fraud"},
			},
		},
		{
			ThingID: "sewer-1", TemplateID: "sewer", TemplateVersion: "v1",
			Name: "Avlopp 1", Location: rtLoc(), Metadata: rtMeta(),
			Properties: map[string]things.Property{
				"distance": {}, "overflow": {}, "overflowTime": {},
			},
			Bindings: []things.Binding{
				{DeviceID: "s-sewer", Object: rtDistURN, Resource: "5700", Input: "distance"},
				{DeviceID: "s-sewer", Object: dinURN, Resource: "5500", Input: "overflow"},
			},
		},
	} {
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	batch := func(id string, obs []things.Observation, at time.Time) {
		t.Helper()
		if _, err := rt.HandleBatch(ctx, "t", id, obs, at); err != nil {
			t.Fatalf("batch %s: %v", id, err)
		}
	}
	val := func(thing, prop string) float64 {
		t.Helper()
		sum, err := mgr.GetObject(ctx, "t", thing)
		if err != nil {
			t.Fatalf("get %s: %v", thing, err)
		}
		pv, ok := sum.Values[prop]
		if !ok || pv.Value == nil {
			t.Fatalf("%s.%s missing: %+v", thing, prop, sum.Values)
		}
		return *pv.Value
	}

	// Desk + lifebuoy: presence 1/0.
	batch("m1", []things.Observation{rtBoolObs("s-desk", dinURN, "5500", true, base)}, base)
	batch("m2", []things.Observation{rtBoolObs("s-lifebuoy", dinURN, "5500", false, base)}, base)
	if got := val("desk-1", "presence"); got != 1 {
		t.Fatalf("desk presence=%v, want 1", got)
	}
	if got := val("lifebuoy-1", "presence"); got != 0 {
		t.Fatalf("lifebuoy presence=%v, want 0", got)
	}

	// Passage: true, true, false, true -> 1, 1, 1, 2.
	batch("p1", []things.Observation{rtBoolObs("s-pass", dinURN, "5500", true, base)}, base)
	batch("p2", []things.Observation{rtBoolObs("s-pass", dinURN, "5500", true, base.Add(time.Minute))}, base.Add(time.Minute))
	batch("p3", []things.Observation{rtBoolObs("s-pass", dinURN, "5500", false, base.Add(2*time.Minute))}, base.Add(2*time.Minute))
	batch("p4", []things.Observation{rtBoolObs("s-pass", dinURN, "5500", true, base.Add(3*time.Minute))}, base.Add(3*time.Minute))
	if got := val("passage-1", "passages"); got != 2 {
		t.Fatalf("passages=%v, want 2", got)
	}
	if got := val("passage-1", "door"); got != 1 {
		t.Fatalf("door=%v, want 1", got)
	}

	// Pump: 90 s drift.
	batch("pu1", []things.Observation{rtBoolObs("s-pump", dinURN, "5500", true, base)}, base)
	batch("pu2", []things.Observation{rtBoolObs("s-pump", dinURN, "5500", false, base.Add(90*time.Second))}, base.Add(90*time.Second))
	if got := val("pump-1", "runTime"); got != 90 {
		t.Fatalf("runTime=%v, want 90", got)
	}

	// Watermeter: volym + läckflagga i samma rapport.
	vol := 12.5
	batch("w1", []things.Observation{
		{
			Signal: things.ParseSignalID("s-wm", "", wmURN, "1"), Value: &vol,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base,
		},
		rtBoolObs("s-wm", wmURN, "10", true, base),
		rtBoolObs("s-wm", wmURN, "11", false, base),
		rtBoolObs("s-wm", wmURN, "13", false, base),
	}, base)
	if got := val("water-1", "volume"); got != 12.5 {
		t.Fatalf("volume=%v, want 12.5", got)
	}
	if got := val("water-1", "leakage"); got != 1 {
		t.Fatalf("leakage=%v, want 1", got)
	}

	// Sewer: bräddning 60 s via bool-ingest -> activeTime.
	batch("s1", []things.Observation{rtBoolObs("s-sewer", dinURN, "5500", true, base)}, base)
	batch("s2", []things.Observation{rtBoolObs("s-sewer", dinURN, "5500", false, base.Add(time.Minute))}, base.Add(time.Minute))
	if got := val("sewer-1", "overflowTime"); got != 60 {
		t.Fatalf("overflowTime=%v, want 60", got)
	}
}

// Startkatalogen end-to-end: seed -> en sak per mall -> beräkningar.
// Motbevisar att seed-mallarna går att skapa saker från och att deras
// recept är korrekt wired (soptunna fyllnadsgrad, passage-räknare).
func TestManageSeedCatalogEndToEnd(t *testing.T) {
	const (
		seedDinURN  = "urn:oma:lwm2m:ext:3200"
		seedDistURN = "urn:oma:lwm2m:ext:3330"
		seedTempURN = "urn:oma:lwm2m:ext:3303"
	)
	raw, err := os.ReadFile("../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := catalog.ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	mgr, _, fs, rt := mgmtSetup(t)
	ctx := context.Background()
	added, skipped, err := mgr.SeedCatalog(ctx, seed)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if added != 17 || skipped != 0 {
		t.Fatalf("seed added=%d skipped=%d, want 17/0", added, skipped)
	}
	// Idempotent: andra seedningen skippar allt.
	added, skipped, err = mgr.SeedCatalog(ctx, seed)
	if err != nil {
		t.Fatalf("reseed: %v", err)
	}
	if added != 0 || skipped != 17 {
		t.Fatalf("reseed added=%d skipped=%d, want 0/17", added, skipped)
	}

	bind := func(device, object, resource, input string) things.Binding {
		return things.Binding{DeviceID: device, Object: object, Resource: resource, Input: input}
	}
	props := func(ids ...string) map[string]things.Property {
		out := make(map[string]things.Property, len(ids))
		for _, id := range ids {
			out[id] = things.Property{}
		}
		return out
	}
	objects := []catalog.ObjectSpec{
		{
			ThingID: "bin-1", TemplateID: "wastebin", TemplateVersion: "v2",
			VariantID: "wastebin-B", VariantVersion: "v2",
			Properties:     props("distance", "level", "fillRate"),
			Bindings:       []things.Binding{bind("s-bin", seedDistURN, "5700", "distance")},
			ParamOverrides: map[string]float64{"sensorToBottom": 1.5},
		},
		{
			ThingID: "room-1", TemplateID: "room", TemplateVersion: "v2",
			Properties: props("temperature.window", "temperature.center", "temperature.door", "temperature.average"),
			Bindings: []things.Binding{
				bind("s-r1", seedTempURN, "5700", "temperature.window"),
				bind("s-r2", seedTempURN, "5700", "temperature.center"),
				bind("s-r3", seedTempURN, "5700", "temperature.door"),
			},
		},
		{
			ThingID: "building-1", TemplateID: "building", TemplateVersion: "v3",
			Properties: props("energy", "power", "temperature"),
			Bindings: []things.Binding{
				bind("s-b", seedTempURN, "5700", "temperature"),
			},
		},
		{
			ThingID: "poi-1", TemplateID: "pointofinterest", TemplateVersion: "v3",
			Properties: props("temperature"),
			Bindings:   []things.Binding{bind("s-poi", seedTempURN, "5700", "temperature")},
		},
		{
			ThingID: "lifebuoy-1", TemplateID: "lifebuoy", TemplateVersion: "v2",
			Properties: props("presence"),
			Bindings:   []things.Binding{bind("s-life", seedDinURN, "5500", "presence")},
		},
		{
			ThingID: "passage-1", TemplateID: "passage", TemplateVersion: "v2",
			Properties: props("door", "passages"),
			Bindings:   []things.Binding{bind("s-pass", seedDinURN, "5500", "door")},
		},
		{
			ThingID: "pump-1", TemplateID: "pumpingstation", TemplateVersion: "v2",
			Properties: props("run", "runTime"),
			Bindings:   []things.Binding{bind("s-pump", seedDinURN, "5500", "run")},
		},
		{
			ThingID: "sewer-1", TemplateID: "sewer", TemplateVersion: "v2",
			Properties: props("distance", "level", "fillRate", "overflow", "overflowTime"),
			Bindings: []things.Binding{
				bind("s-sewer", seedDistURN, "5700", "distance"),
				bind("s-sewer", seedDinURN, "5500", "overflow"),
			},
			ParamOverrides: map[string]float64{"usableHeight": 2.0, "sensorToBottom": 2.5},
		},
		{
			ThingID: "water-1", TemplateID: "watermeter", TemplateVersion: "v2",
			Properties: props("volume", "leakage", "backflow", "fraud"),
			Bindings: []things.Binding{
				bind("s-wm", "urn:oma:lwm2m:ext:3424", "1", "volume"),
				bind("s-wm", "urn:oma:lwm2m:ext:3424", "10", "leakage"),
				bind("s-wm", "urn:oma:lwm2m:ext:3424", "11", "backflow"),
				bind("s-wm", "urn:oma:lwm2m:ext:3424", "13", "fraud"),
			},
		},
	}
	// Beskrivande fält är obligatoriska men inte under test här.
	for i := range objects {
		if objects[i].Name == "" {
			objects[i].Name = objects[i].ThingID
		}
		if objects[i].Location == nil {
			objects[i].Location = rtLoc()
		}
		if len(objects[i].Metadata) == 0 {
			objects[i].Metadata = rtMeta()
		}
	}
	_ = fs
	for _, obj := range objects {
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	dist := 0.3
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{
		{Signal: things.ParseSignalID("s-bin", "", seedDistURN, "5700"), Value: &dist,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base); err != nil {
		t.Fatalf("bin batch: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "bin-1")
	if err != nil {
		t.Fatalf("get bin: %v", err)
	}
	if got := *sum.Values["fillRate"].Value; got < 99.99 || got > 100.01 {
		t.Fatalf("seed bin fillRate=%v, want 100", got)
	}
	// Mallens egenskapsdefinitioner prefillas (GUI visar namn/enhet utan
	// att skaparen angav dem).
	if got := sum.Values["distance"]; got.DisplayName != "Avstånd" || got.Unit != "m" {
		t.Fatalf("prefill=%+v, want Avstånd/m från seed", got)
	}
	for i, open := range []bool{true, true, false, true} {
		at := base.Add(time.Duration(i) * time.Minute)
		if _, err := rt.HandleBatch(ctx, "t", fmt.Sprintf("seed-p%d", i), []things.Observation{
			{Signal: things.ParseSignalID("s-pass", "", seedDinURN, "5500"), BoolValue: &open,
				Quality: things.QualityOK, ObservedAt: at, ReceivedAt: at},
		}, at); err != nil {
			t.Fatalf("passage batch: %v", err)
		}
	}
	sum, err = mgr.GetObject(ctx, "t", "passage-1")
	if err != nil {
		t.Fatalf("get passage: %v", err)
	}
	if got := *sum.Values["passages"].Value; got != 2 {
		t.Fatalf("seed passage count=%v, want 2", got)
	}
}

// Migrering från v1-seed: tre 160L-tunnor (GATA Soptunnor punkt 203-205).
// Mappning: maxl -> variantens usableHeight (0.79, 100% vid 79 cm sopor),
// maxd -> sensorToBottom-override (0.94), "lat,lon" -> Point [lon,lat],
// tagg 160L -> metadata, device -> distance-bindning.
func TestManageMigrate160LBins(t *testing.T) {
	const distURN = "urn:oma:lwm2m:ext:3330"
	type v1bin struct {
		id, name, latlon, device string
	}
	bins := []v1bin{
		{"a6921a85-1f40-4e43-9719-4dabdb349075", "GATA_Soptunnor_punkt.203", "62.387232,17.316830", "milesight:203"},
		{"41e19a70-aa18-4019-8b47-8b21775b799f", "GATA_Soptunnor_punkt.204", "62.298636,17.378277", "milesight:204"},
		{"2bf440f4-65a9-46f1-8e57-a04825e636c8", "GATA_Soptunnor_punkt.205", "62.298782,17.378005", "milesight:205"},
	}
	raw, err := os.ReadFile("../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := catalog.ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	if _, _, err := mgr.SeedCatalog(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, b := range bins {
		var lat, lon float64
		if _, err := fmt.Sscanf(b.latlon, "%f,%f", &lat, &lon); err != nil {
			t.Fatalf("parse location: %v", err)
		}
		loc, _ := json.Marshal([]float64{lon, lat})
		obj := catalog.ObjectSpec{
			ThingID: b.id, Tenant: "default", Name: b.name,
			TemplateID: "wastebin", TemplateVersion: "v2",
			VariantID: "wastebin-160L", VariantVersion: "v2",
			Location: &things.Location{Type: "Point", Coordinates: loc},
			Metadata: map[string]string{"capacity": "160L"},
			Properties: map[string]things.Property{
				"distance": {}, "level": {}, "fillRate": {},
			},
			Bindings: []things.Binding{
				{DeviceID: b.device, Object: distURN, Resource: "5700", Input: "distance"},
			},
			ParamOverrides: map[string]float64{"sensorToBottom": 0.94},
		}
		if _, err := mgr.CreateObject(ctx, "default", obj); err != nil {
			t.Fatalf("create %s: %v", b.name, err)
		}
	}

	// Plats + metadata + paramursprung bevaras för 203:an.
	sum, err := mgr.GetObject(ctx, "default", bins[0].id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Location == nil || sum.Metadata["capacity"] != "160L" {
		t.Fatalf("summary=%+v, want location + capacity", sum)
	}
	if sum.ParamSources["usableHeight"] != things.ParamSourceVariant ||
		sum.ParamSources["sensorToBottom"] != things.ParamSourceThing {
		t.Fatalf("param sources=%+v, want variant/thing", sum.ParamSources)
	}

	// 15 cm avstånd -> nivå 0.79 -> 100% (full vid 79 cm sopor).
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	full := 0.15
	for i, b := range bins {
		at := base.Add(time.Duration(i) * time.Minute)
		if _, err := rt.HandleBatch(ctx, "default", fmt.Sprintf("m-full-%d", i), []things.Observation{
			{Signal: things.ParseSignalID(b.device, "", distURN, "5700"), Value: &full,
				Quality: things.QualityOK, ObservedAt: at, ReceivedAt: at},
		}, at); err != nil {
			t.Fatalf("batch %s: %v", b.name, err)
		}
	}
	for _, b := range bins {
		sum, err := mgr.GetObject(ctx, "default", b.id)
		if err != nil {
			t.Fatalf("get %s: %v", b.name, err)
		}
		if got := *sum.Values["fillRate"].Value; got < 99.99 || got > 100.01 {
			t.Fatalf("%s fillRate=%v, want 100", b.name, got)
		}
	}

	// Tom tunna: 94 cm avstånd -> nivå 0 -> 0%.
	empty := 0.94
	at := base.Add(time.Hour)
	if _, err := rt.HandleBatch(ctx, "default", "m-empty", []things.Observation{
		{Signal: things.ParseSignalID("milesight:203", "", distURN, "5700"), Value: &empty,
			Quality: things.QualityOK, ObservedAt: at, ReceivedAt: at},
	}, at); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	sum, err = mgr.GetObject(ctx, "default", bins[0].id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := *sum.Values["fillRate"].Value; got != 0 {
		t.Fatalf("empty fillRate=%v, want 0", got)
	}
}

// Livscykel + relationer för konsumenter: create/move/unlink/
// delete ger created/updated/deleted samt partOf set/removed; värdesevent
// bär sakdeskriptor så att entiteter byggs utan API-join.
func TestManageLifecycleAndRelations(t *testing.T) {
	ctx := context.Background()
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := catalog.New()
	pub := &fakePublisher{}
	mgr := NewManager(cat, rt, fs, WithPublisher(pub))

	roomTmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1",
		Category:     "room",
		RepeatGroups: []string{"temperature."},
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
		},
	}}
	if err := mgr.PublishTemplate(ctx, roomTmpl); err != nil {
		t.Fatalf("publish room: %v", err)
	}
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1", Category: "building",
	}}); err != nil {
		t.Fatalf("publish building: %v", err)
	}
	mkRoom := func(id, parent string) catalog.ObjectSpec {
		obj := catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: "Rum " + id,
			Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
			Metadata:   map[string]string{"floor": "2"},
			TemplateID: "room", TemplateVersion: "v1",
			Properties: map[string]things.Property{"temperature.window": {}},
			Bindings: []things.Binding{
				{DeviceID: "s-" + id, Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
			},
		}
		if parent != "" {
			obj.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		}
		return obj
	}
	mkBuilding := func(id string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: "Hus " + id,
			Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.0,59.3]`)},
			Metadata:   map[string]string{"floor": "0"},
			TemplateID: "building", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
		}
	}
	mustCreate := func(obj catalog.ObjectSpec) {
		t.Helper()
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	mustCreate(mkBuilding("b-a"))
	mustCreate(mkBuilding("b-b"))
	mustCreate(mkRoom("room-a", "b-a"))

	sum, err := mgr.GetObject(ctx, "t", "room-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	etag := sum.Revision
	if err := mgr.MoveObject(ctx, "t", "room-a", "b-b", etag); err != nil {
		t.Fatalf("move: %v", err)
	}
	sum, _ = mgr.GetObject(ctx, "t", "room-a")
	if err := mgr.UnlinkObject(ctx, "t", "room-a", sum.Revision); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	sum, _ = mgr.GetObject(ctx, "t", "room-a")
	if err := mgr.DeleteObject(ctx, "t", "room-a"); err != nil {
		t.Fatalf("delete room: %v", err)
	}
	_ = sum
	if err := mgr.DeleteObject(ctx, "t", "b-a"); err != nil {
		t.Fatalf("delete b-a: %v", err)
	}
	// Delete med aktiv förälder ger både deleted och removed.
	roomB := mkRoom("room-b", "b-b")
	if _, err := mgr.CreateObject(ctx, "t", roomB); err != nil {
		t.Fatalf("create room-b: %v", err)
	}
	if err := mgr.DeleteObject(ctx, "t", "room-b"); err != nil {
		t.Fatalf("delete room-b: %v", err)
	}

	lifecycle := pub.rawByTopic(pkgtypes.TopicLifecycleChanges)
	relations := pub.rawByTopic(pkgtypes.TopicRelationChanges)
	kinds := map[string]int{}
	for _, body := range lifecycle {
		var ev pkgtypes.ThingLifecycleV1
		if err := json.Unmarshal(body, &ev); err != nil {
			t.Fatalf("decode lifecycle: %v", err)
		}
		if err := ev.Validate(); err != nil {
			t.Fatalf("validate lifecycle: %v", err)
		}
		kinds[string(ev.Kind)]++
		if ev.ThingID == "room-a" && (ev.Thing == nil || ev.Thing.Name != "Rum room-a" || ev.Thing.Category != "room") {
			t.Fatalf("lifecycle descriptor=%+v, want name+category", ev.Thing)
		}
		if ev.ThingID == "room-a" && (ev.Thing.Location == nil || ev.Thing.Location.Type != "Point") {
			t.Fatalf("lifecycle location=%+v, want point", ev.Thing.Location)
		}
	}
	// created x4 (b-a, b-b, room-a, room-b), updated x2 (move, unlink),
	// deleted x3 (room-a, b-a, room-b).
	if kinds["created"] != 4 || kinds["updated"] != 2 || kinds["deleted"] != 3 {
		t.Fatalf("lifecycle kinds=%v, want created 4/updated 2/deleted 3", kinds)
	}
	sets, removed := 0, 0
	roomA := map[string]int{}
	removedTargets := map[string]string{}
	for _, body := range relations {
		var ev pkgtypes.RelationChangeV1
		if err := json.Unmarshal(body, &ev); err != nil {
			t.Fatalf("decode relation: %v", err)
		}
		if ev.RelationID == "" || ev.Type != "partOf" {
			t.Fatalf("relation=%+v, want partOf with ID", ev)
		}
		// From-sidan bär alltid barnets mall + deskriptor så att konsumenter
		// kan routa och rendera utan kataloguppslag.
		if ev.FromThingType != "room" {
			t.Fatalf("relation=%+v, want fromThingType room", ev)
		}
		if ev.From == nil || ev.From.ThingID != ev.FromID || ev.From.Category != "room" {
			t.Fatalf("relation=%+v, want from descriptor", ev)
		}
		if ev.Removed {
			removed++
			// Removed bär sista kända mål (informativt: vilken länk som försvann).
			removedTargets[ev.FromID] = ev.ToID
			if ev.ToID == "" {
				t.Fatalf("removed relation ToID empty: %+v", ev)
			}
			if ev.To == nil || ev.To.ThingID != ev.ToID {
				t.Fatalf("relation=%+v, want to descriptor", ev)
			}
		} else {
			sets++
			if ev.ToID == "" {
				t.Fatalf("set relation ToID empty: %+v", ev)
			}
			if ev.ToThingType != "building" {
				t.Fatalf("relation=%+v, want toThingType building", ev)
			}
			if ev.To == nil || ev.To.ThingID != ev.ToID {
				t.Fatalf("relation=%+v, want to descriptor", ev)
			}
		}
		if ev.FromID == "room-a" && !ev.Removed {
			roomA[ev.ToID]++
		}
	}
	// room-a: set(b-a) + set(b-b) + removed; room-b: set(b-b) + removed(b-b).
	if sets != 3 || removed != 2 {
		t.Fatalf("relations set=%d removed=%d, want 3/2", sets, removed)
	}
	if roomA["b-a"] != 1 || roomA["b-b"] != 1 {
		t.Fatalf("room-a targets=%v, want b-a then b-b", roomA)
	}
	if removedTargets["room-b"] != "b-b" {
		t.Fatalf("room-b removed target=%v, want b-b", removedTargets)
	}
	if removedTargets["room-a"] != "b-b" {
		t.Fatalf("room-a removed target=%v, want b-b (unlink bär sista kända mål)", removedTargets)
	}
}

// Värdesevent bär sakdeskriptor: namn, kategori, plats och metadata följer
// med utan API-join.
func TestManageValuesCarryDescriptor(t *testing.T) {
	ctx := context.Background()
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := catalog.New()
	pub := &fakePublisher{}
	mgr := NewManager(cat, rt, fs, WithPublisher(pub))
	mgmtRooms(t, mgr)
	obj := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Kontorslandskap A",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata:   map[string]string{"floor": "2"},
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s1", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	v := 21.0
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{
		{Signal: things.ParseSignalID("s1", "", rtTempURN, "5700"), Value: &v,
			Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := rt.DrainAndPublish(ctx, pub, "t", "room-a", base); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("value events=%d, want 1", len(pub.events))
	}
	desc := pub.events[0].Thing
	if desc == nil || desc.Name != "Kontorslandskap A" || desc.Category != "room" {
		t.Fatalf("descriptor=%+v, want name+category", desc)
	}
	if desc.Location == nil || desc.Location.Type != "Point" || desc.Metadata["floor"] != "2" {
		t.Fatalf("descriptor=%+v, want location+metadata", desc)
	}
}

// Växthus med tankar från seed-mallarna: växthuset bär rummets mätpunkter
// (temperatur, luftfuktighet, co2, ljus), varje tank har pH, co2,
// temperatur och vattennivå. Vy visar tankarnas värden samlat.
func TestManageGreenhouseTanks(t *testing.T) {
	const (
		tempURN = "urn:oma:lwm2m:ext:3303"
		humURN  = "urn:oma:lwm2m:ext:3304"
		luxURN  = "urn:oma:lwm2m:ext:3301"
		aqURN   = "urn:oma:lwm2m:ext:3428"
		distURN = "urn:oma:lwm2m:ext:3330"
	)
	raw, err := os.ReadFile("../../assets/config/catalog-seed.json")
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	seed, err := catalog.ParseSeed(raw)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	if _, _, err := mgr.SeedCatalog(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	greenhouse := catalog.ObjectSpec{
		ThingID: "greenhouse-a", Tenant: "t", Name: "Växthus A",
		Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
		Metadata:   map[string]string{"site": "campus"},
		TemplateID: "greenhouse", TemplateVersion: "v2",
		Properties: map[string]things.Property{
			"temperature": {}, "humidity": {}, "co2": {}, "illuminance": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "gh-air", Object: tempURN, Resource: "5700", Input: "temperature"},
			{DeviceID: "gh-air", Object: humURN, Resource: "5700", Input: "humidity"},
			{DeviceID: "gh-air", Object: aqURN, Resource: "17", Input: "co2"},
			{DeviceID: "gh-air", Object: luxURN, Resource: "5700", Input: "illuminance"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", greenhouse); err != nil {
		t.Fatalf("create greenhouse: %v", err)
	}
	mkTank := func(id, device string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: "Tank " + id,
			Location:   &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.06,59.33]`)},
			Metadata:   map[string]string{"site": "campus"},
			TemplateID: "tank", TemplateVersion: "v6",
			Properties: map[string]things.Property{
				"ph": {}, "co2": {}, "temperature": {}, "waterLevel": {},
			},
			Bindings: []things.Binding{
				{DeviceID: device, Object: tempURN, Resource: "5700", Input: "temperature"},
				{DeviceID: device, Object: aqURN, Resource: "17", Input: "co2"},
				{DeviceID: device, Object: distURN, Resource: "5700", Input: "waterLevel"},
				{DeviceID: device, Object: "urn:oma:lwm2m:ext:3327", Resource: "5700", Input: "ph"},
			},
			Relations: []catalog.RelationRef{{Name: "partOf", TargetThingID: "greenhouse-a", TargetTemplateID: "greenhouse"}},
		}
	}
	for _, obj := range []catalog.ObjectSpec{mkTank("tank-1", "tank-sensor-1"), mkTank("tank-2", "tank-sensor-2")} {
		if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
			t.Fatalf("create %s: %v", obj.ThingID, err)
		}
	}

	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	f := func(v float64) *float64 { return &v }
	feed := func(id string, obs []things.Observation, at time.Time) {
		t.Helper()
		if _, err := rt.HandleBatch(ctx, "t", id, obs, at); err != nil {
			t.Fatalf("batch %s: %v", id, err)
		}
	}
	feed("gh1", []things.Observation{
		{Signal: things.ParseSignalID("gh-air", "", tempURN, "5700"), Value: f(21.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("gh-air", "", humURN, "5700"), Value: f(55.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("gh-air", "", aqURN, "17"), Value: f(450.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("gh-air", "", luxURN, "5700"), Value: f(320.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base)
	feed("t1", []things.Observation{
		{Signal: things.ParseSignalID("tank-sensor-1", "", "urn:oma:lwm2m:ext:3327", "5700"), Value: f(6.2), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-1", "", aqURN, "17"), Value: f(800.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-1", "", tempURN, "5700"), Value: f(18.5), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-1", "", distURN, "5700"), Value: f(0.8), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base)
	feed("t2", []things.Observation{
		{Signal: things.ParseSignalID("tank-sensor-2", "", "urn:oma:lwm2m:ext:3327", "5700"), Value: f(7.1), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-2", "", aqURN, "17"), Value: f(820.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-2", "", tempURN, "5700"), Value: f(19.0), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
		{Signal: things.ParseSignalID("tank-sensor-2", "", distURN, "5700"), Value: f(0.5), Quality: things.QualityOK, ObservedAt: base, ReceivedAt: base},
	}, base)

	val := func(thing, prop string, wantV float64) {
		t.Helper()
		sum, err := mgr.GetObject(ctx, "t", thing)
		if err != nil {
			t.Fatalf("get %s: %v", thing, err)
		}
		pv, ok := sum.Values[prop]
		if !ok || pv.Value == nil || *pv.Value != wantV {
			t.Fatalf("%s.%s=%+v, want %v", thing, prop, sum.Values[prop], wantV)
		}
	}
	val("tank-1", "ph", 6.2)
	val("tank-1", "waterLevel", 0.8)
	val("greenhouse-a", "temperature", 21.0)
	val("greenhouse-a", "co2", 450.0)

	// Gemensam översikt: växthuset + båda tankarna med värden.
	ov, err := mgr.Overview(ctx, "t", "greenhouse-a")
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if len(ov.Children) != 2 {
		t.Fatalf("children=%d, want 2 tanks", len(ov.Children))
	}
	for _, child := range ov.Children {
		ph, ok := child.Values["ph"]
		if !ok || ph.Value == nil {
			t.Fatalf("tank %s saknar ph: %+v", child.ThingID, child.Values)
		}
	}
	// Kategorisökning hittar båda tankarna.
	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Category: "tank"}); len(got) != 2 {
		t.Fatalf("tank search=%d, want 2", len(got))
	}
}

// Primary: mallens primaryProperty upplöst mot aktuella värden (GUI:ns
// huvuddata). Nil utan primaryProperty eller utan aktiverad egenskap,
// följer nya observationer.
func TestManagePrimaryResolved(t *testing.T) {
	mgr, _, _, rt := mgmtSetup(t)
	ctx := context.Background()
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "wastebin", Version: "v1",
			Required:        []string{"distance", "level", "fillRate"},
			Category:        "wastebin",
			PrimaryProperty: "fillRate",
		},
		ParamDefaults: map[string]float64{"usableHeight": 1.0, "sensorToBottom": 1.5},
		Overridable:   []string{"sensorToBottom"},
		Recipes: []catalog.Recipe{
			{Name: "wastebin-level", Operator: "distanceToLevel", Version: "v1",
				Inputs: []string{"distance"}, Outputs: []string{"level"},
				Params: []string{"sensorToBottom"}},
			{Name: "fill-rate", Operator: "fillRate", Version: "v1",
				Inputs: []string{"level"}, Outputs: []string{"fillRate"},
				Params: []string{"usableHeight"}},
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	obj := mgmtBinObject()
	obj.TemplateID = "wastebin"
	obj.VariantID, obj.VariantVersion = "", ""
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Före första mätningen: primary pekar på egenskapen utan värde.
	sum, err := mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Primary == nil || sum.Primary.PropertyID != "fillRate" || sum.Primary.Value != nil {
		t.Fatalf("primary before data=%+v, want fillRate without value", sum.Primary)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(ctx, "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	sum, err = mgr.GetObject(ctx, "t", "bin-42")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Utan variant: usableHeight 1.0 ger 112 % flaggat uncertain.
	if sum.Primary == nil || sum.Primary.Value == nil || *sum.Primary.Value < 111.9 || *sum.Primary.Value > 112.1 {
		t.Fatalf("primary=%+v, want fillRate ~112", sum.Primary)
	}
	if sum.Primary.Unit != sum.Values["fillRate"].Unit || sum.Primary.Quality != "uncertain" {
		t.Fatalf("primary=%+v, want unit+quality from value", sum.Primary)
	}
}

// Primary är nil utan primaryProperty i mallen (GUI-fallback gäller).
func TestManagePrimaryNilWithoutTemplateProperty(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "sink", Version: "v1",
			Optional: []string{"temperature"},
			Category: "sink",
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "sink-1", Tenant: "t", Name: "Vask",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "sink", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature": {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: rtTempURN, Resource: "5700", Input: "temperature"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "sink-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Primary != nil {
		t.Fatalf("primary=%+v, want nil without template primaryProperty", sum.Primary)
	}
}

// Primary är nil när egenskapen inte är aktiverad på saken (t.ex. rum
// utan medeltemperatur), även om mallen pekar ut den.
func TestManagePrimaryNilWhenNotActivated(t *testing.T) {
	mgr, _, _, _ := mgmtSetup(t)
	ctx := context.Background()
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{
		Template: things.Template{
			ID: "room2", Version: "v1",
			RepeatGroups:    []string{"temperature."},
			Optional:        []string{"temperature.average"},
			Category:        "room",
			PrimaryProperty: "temperature.average",
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	obj := catalog.ObjectSpec{
		ThingID: "room-x", Tenant: "t", Name: "Rum X",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room2", TemplateVersion: "v1",
		Properties: map[string]things.Property{"temperature.window": {}},
		Bindings: []things.Binding{
			{DeviceID: "s", Object: rtTempURN, Resource: "5700", Input: "temperature.window"},
		},
	}
	if _, err := mgr.CreateObject(ctx, "t", obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	sum, err := mgr.GetObject(ctx, "t", "room-x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sum.Primary != nil {
		t.Fatalf("primary=%+v, want nil when property not activated", sum.Primary)
	}
}

// TV-05: endast saknat mål (ErrNotFound) får bli target:null. Korrupt
// målblobb (JSON-fel) och lagringsfel ska propageras, inte döljas som
// trasig referens.
func TestRelationsCorruptTargetPropagates(t *testing.T) {
	ctx := context.Background()
	mgr, _, fs, _ := mgmtSetup(t)

	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "building", Version: "v1", Category: "building",
	}}); err != nil {
		t.Fatalf("publish building: %v", err)
	}
	if err := mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1", Category: "room",
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"building"}, Min: 0, Max: 1},
		},
	}}); err != nil {
		t.Fatalf("publish room: %v", err)
	}
	mk := func(id, parent string) catalog.ObjectSpec {
		obj := catalog.ObjectSpec{
			ThingID: id, Tenant: "t", Name: id,
			Location: &things.Location{Type: "Point", Coordinates: json.RawMessage(`[18.0,59.3]`)},
			Metadata: map[string]string{}, TemplateID: "room", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
		}
		if parent != "" {
			obj.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: parent, TargetTemplateID: "building"}}
		} else {
			obj.TemplateID = "building"
			obj.Relations = nil
		}
		return obj
	}
	if _, err := mgr.CreateObject(ctx, "t", mk("b-a", "")); err != nil {
		t.Fatalf("create b-a: %v", err)
	}
	room := mk("room-a", "")
	room.TemplateID = "room"
	room.Relations = []catalog.RelationRef{{Name: "partOf", TargetThingID: "b-a", TargetTemplateID: "building"}}
	if _, err := mgr.CreateObject(ctx, "t", room); err != nil {
		t.Fatalf("create room-a: %v", err)
	}

	// Sanity: intakt mål löses.
	rels, err := mgr.Relations(ctx, "t", "room-a")
	if err != nil {
		t.Fatalf("relations: %v", err)
	}
	if len(rels) != 1 || rels[0].Target == nil {
		t.Fatalf("relations=%+v, want resolved target", rels)
	}

	// Korrumpera målets blob: Relations måste fela, inte ge target:null.
	fs.mu.Lock()
	key := fkey("t", "b-a")
	st := fs.things[key]
	st.ConfigJSON = []byte(`{invalid`)
	fs.things[key] = st
	fs.mu.Unlock()

	rels, err = mgr.Relations(ctx, "t", "room-a")
	if err == nil {
		t.Fatalf("corrupt target must propagate error, got null-target %+v", rels)
	}
	if !errors.Is(err, things.ErrNotFound) && rels != nil {
		// Förväntat: annat fel än NotFound, ingen tyst null-rad.
	}
}
