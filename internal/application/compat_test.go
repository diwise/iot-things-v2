// Kompatibilitet sensor -> agent -> core -> v2 (THNGS_NEW.md Etapp 7-förarbete).
//
// Testen matar convPack med verklig agent-trådform (kompakt bn/bt, samma
// form som iot-agent marshalar i pkg/lwm2m) och bevisar att v2-signaler,
// bindningar och flöde hanterar dem korrekt: en observation per resurs med
// observationens egen URN, kanal som mellansegment, identiska resursnummer
// särskiljda via URN, samt full kedja pack -> bindning -> thing+property.
package application

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
)

const (
	compatTempURN = "urn:oma:lwm2m:ext:3303"
	compatHumURN  = "urn:oma:lwm2m:ext:3304"
	compatLuxURN  = "urn:oma:lwm2m:ext:3301"
	compatDistURN = "urn:oma:lwm2m:ext:3330"
	compatAQURN   = "urn:oma:lwm2m:ext:3428"
	compatDinURN  = "urn:oma:lwm2m:ext:3200"
)

func parseAgentPack(t *testing.T, payload string, ref time.Time) []things.Observation {
	t.Helper()
	var pack senml.Pack
	if err := json.Unmarshal([]byte(payload), &pack); err != nil {
		t.Fatalf("unmarshal agent pack: %v", err)
	}
	parsed, err := diwisepkg.Parse(pack, ref)
	if err != nil {
		t.Fatalf("diwise.Parse: %v", err)
	}
	return convPack(parsed, ref, "compat-m1")
}

func findObs(obs []things.Observation, device, channel, objectURN, resource string) *things.Observation {
	for i := range obs {
		s := obs[i].Signal
		if s.DeviceID == device && s.Channel == channel && s.Object == objectURN && s.Resource == resource {
			return &obs[i]
		}
	}
	return nil
}

func obsValue(t *testing.T, o *things.Observation) float64 {
	t.Helper()
	if o == nil || o.Value == nil {
		t.Fatalf("observation saknar numeriskt värde: %+v", o)
	}
	return *o.Value
}

// Agentens enobjekts-temp (senlabT-form): ett objekt, resurs 5700 i Cel.
func TestCompatSingleTemp(t *testing.T) {
	ref := time.Date(2024, 3, 11, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"senlabT/3303/","bt":1710151647,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":6.625}
	]`, ref)
	if len(obs) != 1 {
		t.Fatalf("observations=%d, want 1 (header n:0 skipped)", len(obs))
	}
	s := obs[0].Signal
	if s.DeviceID != "senlabT" || s.Channel != "" || s.Object != compatTempURN || s.Resource != "5700" {
		t.Fatalf("signal=%+v", s)
	}
	if obs[0].Unit != "Cel" {
		t.Fatalf("unit=%q, want Cel (sensorenhet bevaras för strikt match)", obs[0].Unit)
	}
	if v := obsValue(t, &obs[0]); v != 6.625 {
		t.Fatalf("value=%v, want 6.625", v)
	}
	if want := time.Unix(1710151647, 0).UTC(); !obs[0].ObservedAt.Equal(want) {
		t.Fatalf("observedAt=%v, want %v (bt ska bli mättid)", obs[0].ObservedAt, want)
	}
	b := things.Binding{DeviceID: "senlabT", Object: compatTempURN, Resource: "5700", Input: "temperature.outdoor"}
	if !b.Matches(s) {
		t.Fatal("thing+property-bindning matchar inte verklig temp-signal")
	}
}

// Flerobjekt temp+fukt+ljus: samma resursnummer 5700 i tre objekt får aldrig
// blandas; varje observation bär sin egen URN.
func TestCompatMultiObjectResource5700KeptApart(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/3304/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3304"},
		{"n":"5700","u":"%RH","v":55.0},
		{"bn":"dev1/3301/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3301"},
		{"n":"5700","u":"lux","v":320.0}
	]`, ref)
	if len(obs) != 3 {
		t.Fatalf("observations=%d, want 3", len(obs))
	}
	bindings := []things.Binding{
		{DeviceID: "dev1", Object: compatTempURN, Resource: "5700", Input: "temperature.center"},
		{DeviceID: "dev1", Object: compatHumURN, Resource: "5700", Input: "humidity"},
		{DeviceID: "dev1", Object: compatLuxURN, Resource: "5700", Input: "illuminance"},
	}
	seen := map[string]bool{}
	wantValues := map[string]float64{
		"temperature.center": 21.5,
		"humidity":           55.0,
		"illuminance":        320.0,
	}
	for _, o := range obs {
		matched := matchingInputs(bindings, o.Signal)
		if len(matched) != 1 {
			t.Fatalf("signal %+v matchade %v, want exakt 1 ingång", o.Signal, matched)
		}
		if o.Value == nil || *o.Value != wantValues[matched[0]] {
			t.Fatalf("ingång %q värde=%v, want %v", matched[0], o.Value, wantValues[matched[0]])
		}
		seen[matched[0]] = true
	}
	for _, want := range []string{"temperature.center", "humidity", "illuminance"} {
		if !seen[want] {
			t.Fatalf("ingång %q fick ingen observation", want)
		}
	}
}

// Elsys-kanaler: flera givare av samma typ på samma enhet skiljs med
// mellansegment; explicit kanalbindning träffar rätt, tom kanal tar alla.
func TestCompatElsysChannels(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"abc123/0/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"abc123/1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":19.0}
	]`, ref)
	if len(obs) != 2 {
		t.Fatalf("observations=%d, want 2", len(obs))
	}
	outer := findObs(obs, "abc123", "0", compatTempURN, "5700")
	inner := findObs(obs, "abc123", "1", compatTempURN, "5700")
	if outer == nil || inner == nil {
		t.Fatalf("kanalsignaler saknas: %+v", obs)
	}
	if v := obsValue(t, outer); v != 21.5 {
		t.Fatalf("kanal 0 värde=%v, want 21.5", v)
	}
	if v := obsValue(t, inner); v != 19.0 {
		t.Fatalf("kanal 1 värde=%v, want 19.0", v)
	}
	explicit := things.Binding{DeviceID: "abc123", Channel: "0", Object: compatTempURN, Resource: "5700", Input: "temperature.outdoor"}
	if !explicit.Matches(outer.Signal) || explicit.Matches(inner.Signal) {
		t.Fatal("explicit kanalbindning måste träffa endast kanal 0")
	}
	wildcard := things.Binding{DeviceID: "abc123", Object: compatTempURN, Resource: "5700", Input: "temperature.average"}
	if !wildcard.Matches(outer.Signal) || !wildcard.Matches(inner.Signal) {
		t.Fatal("tom kanalbindning måste träffa alla kanaler")
	}
}

// Avstånd 3330: värderesurs 5700 i meter ger observation; 5701 ("metre")
// bär endast strängvärde utan numeriskt/bool-värde och hoppas över.
func TestCompatDistanceSkipsValueless(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"bin-sensor/3330/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3330"},
		{"n":"5700","u":"m","v":0.30},
		{"n":"5701","vs":"metre"}
	]`, ref)
	if len(obs) != 1 {
		t.Fatalf("observations=%d, want 1 (5701 utan numeriskt värde skippas)", len(obs))
	}
	if obs[0].Signal.Resource != "5700" || obs[0].Unit != "m" {
		t.Fatalf("distanssignal=%+v", obs[0].Signal)
	}
	if v := obsValue(t, &obs[0]); v < 0.299 || v > 0.301 {
		t.Fatalf("distansvärde=%v, want ~0.30", v)
	}
	b := things.Binding{DeviceID: "bin-sensor", Object: compatDistURN, Resource: "5700", Input: "distance"}
	if !b.Matches(obs[0].Signal) {
		t.Fatal("soptunne-bindning matchar inte verklig distanssignal")
	}
}

// Luftkvalitet 3428: CO2 (resurs 17, ppm) särskiljs från partiklar (1, 3,
// ug/m3) via resurs-exakt bindning; samma prefix, olika ingångar.
// Ordning och enheter följer iot-agent pkg/lwm2m marshalJSON.
func TestCompatAirQualityResourceSelectsInput(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"am100/3428/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3428"},
		{"n":"1","u":"ug/m3","v":5},
		{"n":"3","u":"ug/m3","v":7},
		{"n":"17","u":"ppm","v":886}
	]`, ref)
	if len(obs) != 3 {
		t.Fatalf("observations=%d, want 3", len(obs))
	}
	wantValues := map[string]float64{"1": 5, "3": 7, "17": 886}
	for _, o := range obs {
		if o.Value == nil || *o.Value != wantValues[o.Signal.Resource] {
			t.Fatalf("resurs %q värde=%v, want %v", o.Signal.Resource, o.Value, wantValues[o.Signal.Resource])
		}
	}
	co2 := things.Binding{DeviceID: "am100", Object: compatAQURN, Resource: "17", Input: "co2"}
	for _, o := range obs {
		matched := co2.Matches(o.Signal)
		isCO2 := o.Signal.Resource == "17"
		if matched != isCO2 {
			t.Fatalf("resurs %q co2-match=%v, want %v", o.Signal.Resource, matched, isCO2)
		}
	}
}

// Positionsmetadata (lat/lon, verklig trådform utan n, endast u/v) blir
// aldrig observationer: diwise.Parse exponerar dem som objektmetadata, inte
// som objektresurser. convPack:s Atoi-skydd (samma som iot-things convPack)
// är försvar på djupet för handbyggda pack; via Parse når icke-numeriska
// resursnamn utanför metadatamängden aldrig convPack utan ger Parse-fel.
func TestCompatNonNumericNamesSkipped(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"u":"lat","v":62.39},
		{"u":"lon","v":17.30}
	]`, ref)
	if len(obs) != 1 {
		t.Fatalf("observations=%d, want 1 (lat/lon är metadata, inte resurser)", len(obs))
	}
	for _, o := range obs {
		if o.Signal.Resource == "lat" || o.Signal.Resource == "lon" {
			t.Fatalf("ickenumerisk post blev observation: %+v", o.Signal)
		}
	}
	o := findObs(obs, "dev1", "", compatTempURN, "5700")
	if o == nil {
		t.Fatal("värdeposten 5700 måste finnas kvar")
	}
	if v := obsValue(t, o); v != 21.5 {
		t.Fatalf("värde=%v, want 21.5", v)
	}
}

// Anrikat core-pack (message.accepted-form, jfr iot-core golden): tenant,
// source och positionsmetadata under samma objektprefix ger ingen extra
// observation. Parse kräver ingen tenant (endast ValidateAccepted och
// handlergrinden gör det); tenant läses via Pack.Tenant().
func TestCompatEnrichedPackMetadataIgnored(t *testing.T) {
	ref := time.Date(2025, 4, 10, 11, 44, 1, 0, time.UTC)
	payload := `[
		{"bn":"device-42/0/3303/","bt":1744285441,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"n":"tenant","vs":"acme"},
		{"n":"source","vs":"test"},
		{"u":"lat","v":62.39},
		{"u":"lon","v":17.31}
	]`
	var pack senml.Pack
	if err := json.Unmarshal([]byte(payload), &pack); err != nil {
		t.Fatalf("unmarshal enriched pack: %v", err)
	}
	parsed, err := diwisepkg.Parse(pack, ref)
	if err != nil {
		t.Fatalf("diwise.Parse: %v", err)
	}
	if parsed.Tenant() != "acme" {
		t.Fatalf("tenant=%q, want acme", parsed.Tenant())
	}
	obs := convPack(parsed, ref, "compat-m1")
	if len(obs) != 1 {
		t.Fatalf("observations=%d, want 1 (tenant/source/lat/lon ger inga observationer)", len(obs))
	}
	o := findObs(obs, "device-42", "0", compatTempURN, "5700")
	if o == nil {
		t.Fatalf("temperatursignal saknas: %+v", obs)
	}
	if v := obsValue(t, o); v != 21.5 {
		t.Fatalf("värde=%v, want 21.5", v)
	}
}

// Full kedja med verklig tråddata: enobjektspack med tre temperaturkanaler
// (0/1/2, 18/21/24 grader) -> kanalbindningar -> kompilerad rumskonfig ->
// medeltemperaturen 21 beräknas ur de tre temperaturkanalerna.
func TestCompatEndToEndRoomFromWireData(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"room-sensor/0/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":18.0},
		{"bn":"room-sensor/1/3303/","bt":1720000060,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.0},
		{"bn":"room-sensor/2/3303/","bt":1720000120,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":24.0}
	]`, ref)
	if len(obs) != 3 {
		t.Fatalf("observations=%d, want 3", len(obs))
	}
	c := catalog.New()
	tmpl := catalog.TemplateSpec{Template: things.Template{
		ID: "room", Version: "v1",
		Category:     "room",
		RepeatGroups: []string{"temperature."},
		Optional:     []string{"temperature.average"},
	}}
	if err := c.PutTemplate(tmpl); err != nil {
		t.Fatalf("put room: %v", err)
	}
	cfg, err := c.Compile(catalog.ObjectSpec{
		ThingID: "kontorslandskap-a", Tenant: "t", Name: "Kontorslandskap A",
		Location: rtLoc(), Metadata: rtMeta(),
		TemplateID: "room", TemplateVersion: "v1",
		Properties: map[string]things.Property{
			"temperature.window": {}, "temperature.center": {},
			"temperature.door": {}, "temperature.average": {},
		},
		Bindings: []things.Binding{
			{DeviceID: "room-sensor", Channel: "0", Object: compatTempURN, Resource: "5700", Input: "temperature.window"},
			{DeviceID: "room-sensor", Channel: "1", Object: compatTempURN, Resource: "5700", Input: "temperature.center"},
			{DeviceID: "room-sensor", Channel: "2", Object: compatTempURN, Resource: "5700", Input: "temperature.door"},
		},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Medelrecept läggs av mallen; här explicit för kedjebeviset.
	cfg.Recipes = []catalog.Recipe{{
		Name: "mean-temp", Operator: "mean", Version: "v1",
		Inputs:  []string{"temperature.window", "temperature.center", "temperature.door"},
		Outputs: []string{"temperature.average"},
	}}
	out, err := EvaluateBatchFlow(cfg, nil, nil, obs, ref, "compat-m1", 1)
	if err != nil {
		t.Fatalf("flow: %v", err)
	}
	avg, ok := out.Updated["temperature.average"]
	if !ok || avg.Value == nil || *avg.Value != 21 {
		t.Fatalf("average=%+v, want 21", avg)
	}
	found := false
	for _, e := range out.Events {
		if e.PropertyID == "temperature.average" {
			found = true
			if e.Available != 3 || e.Expected != 3 || len(e.Sources) != 3 {
				t.Fatalf("medelproveniens=%+v, want 3/3 källor", e)
			}
		}
	}
	if !found {
		t.Fatal("medel-event saknas")
	}
}

// Upprepad observation (samma objekt/resurs, olika mättid): båda bevaras i
// trådordning med varsin mättid (observationskontraktets
// repeated-observation-fixtur). Bindningen träffar båda; flödet väljer
// senaste mättid som aktuellt (B3) medan båda raderna sparas i historik.
func TestCompatRepeatedObservationKeptInOrder(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5,"t":0},
		{"n":"5700","u":"Cel","v":22.0,"t":60}
	]`, ref)
	if len(obs) != 2 {
		t.Fatalf("observations=%d, want 2 (repeated resource kept, not collapsed)", len(obs))
	}
	if !obs[0].ObservedAt.Before(obs[1].ObservedAt) {
		t.Fatalf("trådordning bruten: %v then %v", obs[0].ObservedAt, obs[1].ObservedAt)
	}
	if v := obsValue(t, &obs[0]); v != 21.5 {
		t.Fatalf("första värdet=%v, want 21.5", v)
	}
	if v := obsValue(t, &obs[1]); v != 22.0 {
		t.Fatalf("andra värdet=%v, want 22.0", v)
	}
	b := things.Binding{DeviceID: "dev1", Object: compatTempURN, Resource: "5700", Input: "temperature.center"}
	for i := range obs {
		if !b.Matches(obs[i].Signal) {
			t.Fatalf("obs %d matchar inte temp-bindning: %+v", i, obs[i].Signal)
		}
	}
}

// Digital ingång (dörr/pump/närvaro): boolesk SenML-post blir en
// observation med BoolValue som flödet mappar till 1/0.
func TestCompatBoolDigitalInput(t *testing.T) {
	ref := time.Date(2024, 7, 3, 12, 0, 0, 0, time.UTC)
	obs := parseAgentPack(t, `[
		{"bn":"door1/3200/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3200"},
		{"n":"5500","vb":true}
	]`, ref)
	if len(obs) != 1 {
		t.Fatalf("observations=%d, want 1", len(obs))
	}
	o := obs[0]
	if o.Value != nil || o.BoolValue == nil || !*o.BoolValue {
		t.Fatalf("bool observation=%+v, want BoolValue true utan Value", o)
	}
	b := things.Binding{DeviceID: "door1", Object: compatDinURN, Resource: "5500", Input: "door"}
	if !b.Matches(o.Signal) {
		t.Fatalf("bool matchar inte din-bindning: %+v", o.Signal)
	}
}
