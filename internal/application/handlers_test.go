package application

import (
	"context"
	"encoding/json"
	"errors"
	"go.opentelemetry.io/otel/codes"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
)

// fakeThingsApp fångar handler-anrop för envelope-tester.
type fakeThingsApp struct {
	tenant    string
	messageID string
	obs       []things.Observation
	calls     int
	handleErr error
}

func (f *fakeThingsApp) HandleMeasurements(_ context.Context, tenant, messageID string, obs []things.Observation) error {
	f.tenant, f.messageID, f.obs = tenant, messageID, obs
	f.calls++
	return f.handleErr
}
func (f *fakeThingsApp) Add(context.Context, []byte) error            { return nil }
func (f *fakeThingsApp) Seed(context.Context, io.Reader) error        { return nil }
func (f *fakeThingsApp) MigrateBindings(context.Context) (int, error) { return 0, nil }
func (f *fakeThingsApp) HasUnmigratedThings(context.Context) (bool, error) {
	return false, nil
}
func (f *fakeThingsApp) DrainPending(context.Context) (int, error) { return 0, nil }

func incomingMessage(body []byte, messageID string) *messaging.IncomingTopicMessageMock {
	return &messaging.IncomingTopicMessageMock{
		BodyFunc:          func() []byte { return body },
		TopicNameFunc:     func() string { return "message.accepted" },
		MessageIDFunc:     func() string { return messageID },
		CorrelationIDFunc: func() string { return "" },
		RedeliveredFunc:   func() bool { return false },
		HeadersFunc:       func() map[string]any { return nil },
	}
}

// Verkligt message.accepted-kuvert (berikat av core: tenant per objekt):
// flerobjekt temp+fukt+ljus med egna mättider.
func TestHandlerAcceptedEnvelope(t *testing.T) {
	pack := `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/3304/","bt":1720003600,"n":"0","vs":"urn:oma:lwm2m:ext:3304"},
		{"n":"5700","u":"%RH","v":55.0},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	]`
	var rawPack senml.Pack
	if err := json.Unmarshal([]byte(pack), &rawPack); err != nil {
		t.Fatalf("pack: %v", err)
	}
	env, _ := json.Marshal(map[string]any{
		"pack":      rawPack,
		"timestamp": time.Date(2024, 7, 3, 12, 5, 0, 0, time.UTC),
	})
	fake := &fakeThingsApp{}
	h := NewMeasurementsHandler(context.Background(), fake)
	err := h(context.Background(), incomingMessage(env, "env-m1"), slog.Default())
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("calls=%d, want 1", fake.calls)
	}
	if fake.tenant != "acme" {
		t.Fatalf("tenant=%q, want enriched acme", fake.tenant)
	}
	if fake.messageID != "env-m1" {
		t.Fatalf("messageID=%q transported from envelope, not payload", fake.messageID)
	}
	if len(fake.obs) != 2 {
		t.Fatalf("observations=%d, want 2 (tenant record is metadata, not data)", len(fake.obs))
	}
	if fake.obs[0].Signal.Object != "urn:oma:lwm2m:ext:3303" || fake.obs[1].Signal.Object != "urn:oma:lwm2m:ext:3304" {
		t.Fatalf("per-observation URN lost: %+v", fake.obs)
	}
	if fake.obs[1].ObservedAt.Unix() != 1720003600 {
		t.Fatalf("mättid=%v, envelope-timestamp får inte användas som mättid", fake.obs[1].ObservedAt)
	}
}

func TestHandlerMalformedIsPermanent(t *testing.T) {
	fake := &fakeThingsApp{}
	h := NewMeasurementsHandler(context.Background(), fake)
	err := h(context.Background(), incomingMessage([]byte("{ojsan"), "m-bad"), slog.Default())
	if err == nil {
		t.Fatal("malformed body must fail")
	}
	if fake.calls != 0 {
		t.Fatal("malformed body must not reach app")
	}
}

func TestHandlerMissingTenantIsPermanent(t *testing.T) {
	pack := `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5}
	]`
	var rawPack senml.Pack
	if err := json.Unmarshal([]byte(pack), &rawPack); err != nil {
		t.Fatalf("pack: %v", err)
	}
	env, _ := json.Marshal(map[string]any{
		"pack":      rawPack,
		"timestamp": time.Now().UTC(),
	})
	fake := &fakeThingsApp{}
	h := NewMeasurementsHandler(context.Background(), fake)
	err := h(context.Background(), incomingMessage(env, "m-notenant"), slog.Default())
	if err == nil {
		t.Fatal("missing tenant must fail permanently (never heals on retry)")
	}
	if fake.calls != 0 {
		t.Fatal("tenantless message must not reach app")
	}
}

func TestHandlerAppErrorPropagatesForRetry(t *testing.T) {
	pack := `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	]`
	var rawPack senml.Pack
	if err := json.Unmarshal([]byte(pack), &rawPack); err != nil {
		t.Fatalf("pack: %v", err)
	}
	env, _ := json.Marshal(map[string]any{
		"pack":      rawPack,
		"timestamp": time.Now().UTC(),
	})
	fake := &fakeThingsApp{handleErr: errors.New("store unavailable")}
	h := NewMeasurementsHandler(context.Background(), fake)
	if err := h(context.Background(), incomingMessage(env, "m-retry"), slog.Default()); err == nil {
		t.Fatal("handling error must propagate for redelivery")
	}
}

// Regression L6/L10: tomt transport-MessageID avvisas permanent (stabila
// outbox-ID:n kräver ID); får aldrig nå appen eller läka vid retry.
func TestHandlerMissingMessageIDIsPermanent(t *testing.T) {
	pack := `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	]`
	var rawPack senml.Pack
	if err := json.Unmarshal([]byte(pack), &rawPack); err != nil {
		t.Fatalf("pack: %v", err)
	}
	env, _ := json.Marshal(map[string]any{
		"pack":      rawPack,
		"timestamp": time.Now().UTC(),
	})
	fake := &fakeThingsApp{}
	h := NewMeasurementsHandler(context.Background(), fake)
	if err := h(context.Background(), incomingMessage(env, ""), slog.Default()); err == nil {
		t.Fatal("missing message ID must fail permanently (never heals on retry)")
	}
	if fake.calls != 0 {
		t.Fatal("message without ID must not reach app")
	}
}

// TV-07: receive-measurements ska sätta Error-status vid hanteringsfel.
// Tidigare skuggade `if err :=` det yttre err som deferred span-avslut läser.
func TestHandlerSpanErrorOnAppFailure(t *testing.T) {
	rec, restore := withSpanRecorder(t)
	defer restore()

	pack := `[
		{"bn":"dev1/3303/","bt":1720000000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},
		{"n":"5700","u":"Cel","v":21.5},
		{"bn":"dev1/","n":"tenant","vs":"acme"}
	]`
	var rawPack senml.Pack
	if err := json.Unmarshal([]byte(pack), &rawPack); err != nil {
		t.Fatalf("pack: %v", err)
	}
	env, _ := json.Marshal(map[string]any{
		"pack":      rawPack,
		"timestamp": time.Now().UTC(),
	})
	fake := &fakeThingsApp{handleErr: errors.New("store unavailable")}
	h := NewMeasurementsHandler(context.Background(), fake)
	if err := h(context.Background(), incomingMessage(env, "m-span-err"), slog.Default()); err == nil {
		t.Fatal("handling error must propagate")
	}
	span := spanByName(rec.Ended(), "receive-measurements")
	if span == nil {
		t.Fatalf("no receive-measurements span, got %v", spanNames(rec.Ended()))
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status=%v, want Error on handling failure", span.Status().Code)
	}
}
