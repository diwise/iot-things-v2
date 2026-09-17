package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
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

	if len(mgr.ListTemplates(ctx)) != 1 || len(mgr.ListVariants(ctx)) != 1 {
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
		ID: "building", Version: "v1",
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
		TemplateID: "building", TemplateVersion: "v1",
		Properties: map[string]things.Property{},
	}); err != nil {
		t.Fatalf("create building: %v", err)
	}
	mkRoom := func(id, parent string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t",
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
		ThingID: "building-b", Tenant: "t",
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
	}})
	if err != nil {
		t.Fatalf("publish greenhouse: %v", err)
	}
	err = mgr.PublishTemplate(ctx, catalog.TemplateSpec{Template: things.Template{
		ID: "bench", Version: "v1",
		RepeatGroups: []string{"soilMoisture."},
		Optional:     []string{"airTemp.bench"},
		Relations: []things.RelationSpec{
			{Name: "partOf", AllowedTargets: []string{"greenhouse"}, Min: 0, Max: 1},
		},
	}})
	if err != nil {
		t.Fatalf("publish bench: %v", err)
	}
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "greenhouse-a", Tenant: "t", Name: "Växthus A",
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
			ThingID: bench, Tenant: "t",
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
		ThingID: "room-a", Tenant: "t",
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
			ThingID: id, Tenant: "t",
			TemplateID: "building", TemplateVersion: "v1",
			Properties: map[string]things.Property{},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	// Fristående rum (ingen partOf) flyttas till building-a med revision 1.
	if _, err := mgr.CreateObject(ctx, "t", catalog.ObjectSpec{
		ThingID: "room-x", Tenant: "t",
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
	before := len(fs.ListThingIDs("t"))
	if _, err := mgr.PreviewObject(ctx, "t", mgmtBinObject()); err != nil {
		t.Fatalf("preview: %v", err)
	}
	if got := len(fs.ListThingIDs("t")); got != before {
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
		ThingID: "building-a", Tenant: "t",
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
		Template:      things.Template{ID: "wastebin", Version: "v1", Required: []string{"distance"}},
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
	bin.Classification = "wastebin"
	if _, err := mgr.CreateObject(ctx, "t", bin); err != nil {
		t.Fatalf("create bin: %v", err)
	}
	roomObj := catalog.ObjectSpec{
		ThingID: "room-a", Tenant: "t", Name: "Kontorslandskap A",
		TemplateID: "room", TemplateVersion: "v1",
		Classification: "room",
		Metadata:       map[string]string{"district": "söder"},
		Properties:     map[string]things.Property{"temperature.window": {}},
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
	if sum.Metadata["district"] != "centrum" || sum.Classification != "wastebin" {
		t.Fatalf("summary=%+v", sum)
	}

	if got := mgr.SearchObjects(ctx, "t", SearchFilter{Classification: "room"}); len(got) != 1 || got[0].ThingID != "room-a" {
		t.Fatalf("classification filter=%+v, want room-a", got)
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
