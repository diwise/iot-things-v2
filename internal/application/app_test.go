package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

// app.HandleMeasurements: batch -> runtime -> publish i ett anrop.
func TestAppHandleMeasurementsPublishes(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	pub := &fakePublisher{}
	a := New(nil, nil, nil, rt, pub)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if err := a.HandleMeasurements(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("published=%d, want 1 batch event", pub.count())
	}
	if pending := fs.PendingEvents("t", "bin-42"); len(pending) != 0 {
		t.Fatalf("pending=%d after publish, want 0", len(pending))
	}
	// Dubblett: ingen omberäkning och ingen dubbelleverans (första
	// leveransen lyckades; stabila ID:n gör eventuell återleverans
	// ofarlig nedströms).
	if err := a.HandleMeasurements(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}); err != nil {
		t.Fatalf("duplicate handle: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("published=%d, want 1 (no duplicate delivery)", pub.count())
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d, want 3", n)
	}
}

// Fel + återleverans: misslyckad publicering behåller kön och nästa
// (dubblett-)batch levererar den.
func TestAppFailureThenRedeliveryDelivers(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	pub := &fakePublisher{err: errors.New("broker down")}
	a := New(nil, nil, nil, rt, pub)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	obs := []things.Observation{rtDistObs("milesight:214", 0.30, base)}
	if err := a.HandleMeasurements(context.Background(), "t", "m1", obs); err == nil {
		t.Fatal("delivery failure must propagate for retry")
	}
	pub.err = nil
	if err := a.HandleMeasurements(context.Background(), "t", "m1", obs); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("published=%d, want exactly-once delivery after retry", pub.count())
	}
	if n := fs.historyCount("t", "bin-42"); n != 3 {
		t.Fatalf("history=%d, want 3", n)
	}
}

// app.DrainPending: omstartsväg utan ny batch.
func TestAppDrainPending(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if _, err := rt.HandleBatch(context.Background(), "t", "m1", []things.Observation{rtDistObs("milesight:214", 0.30, base)}, base); err != nil {
		t.Fatalf("batch: %v", err)
	}
	a := New(nil, nil, nil, NewRuntime(fs), &fakePublisher{})
	n, err := a.DrainPending(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 1 {
		t.Fatalf("drained=%d, want 1", n)
	}
}

// TopicPublisher levererar via MsgContext och propagerar fel (kön behålls
// av DrainAndPublish, se runtime-tester).
func TestTopicPublisherDelivery(t *testing.T) {
	var got []messaging.TopicMessage
	mock := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(_ context.Context, msg messaging.TopicMessage) error {
			got = append(got, msg)
			return nil
		},
	}
	pub := &TopicPublisher{MsgCtx: mock}
	now := time.Date(2026, 9, 17, 12, 5, 0, 0, time.UTC)
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ev := buildThingValuesEvent("t", "bin-42", "wastebin", "v1", map[string]string{}, []FlowEvent{{
		PropertyID: "distance", Value: 0.3, Quality: "ok",
		ObservedAt: at, ReceivedAt: at, MessageID: "m1",
	}}, 1, now)
	if err := pub.PublishThingValues(context.Background(), ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(got) != 1 || got[0].TopicName() != "things.v1.values" {
		t.Fatalf("topic=%+v", got)
	}
	if ct := got[0].ContentType(); ct == "" {
		t.Fatal("content type required")
	}
	fail := &messaging.MsgContextMock{
		PublishOnTopicFunc: func(_ context.Context, _ messaging.TopicMessage) error {
			return errors.New("broker down")
		},
	}
	if err := (&TopicPublisher{MsgCtx: fail}).PublishThingValues(context.Background(), ev); err == nil {
		t.Fatal("delivery failure must propagate")
	}
}

// Regression L10: tomt messageID avvisas utan sidoeffekter.
func TestAppRejectsEmptyMessageID(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	cat := rtBinCatalog(t)
	if _, err := rt.RegisterThing("t", cat, rtBinObject(), 1); err != nil {
		t.Fatalf("register: %v", err)
	}
	a := New(nil, nil, nil, rt, &fakePublisher{})
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if err := a.HandleMeasurements(context.Background(), "t", "", []things.Observation{rtDistObs("milesight:214", 0.30, base)}); err == nil {
		t.Fatal("empty messageID must be rejected")
	}
	if n := fs.historyCount("t", "bin-42"); n != 0 {
		t.Fatalf("history=%d, want 0", n)
	}
}

// Regression L7: samma batch till två saker får samma PublishedAt (enhetlig now).
func TestAppSamePublishedAtAcrossThings(t *testing.T) {
	fs := newFakeStore()
	rt := NewRuntime(fs)
	c := catalog.New()
	if err := c.PutTemplate(catalog.TemplateSpec{Template: things.Template{
		ID: "gauge", Version: "v1", Required: []string{"distance"},
	}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	mk := func(id string) catalog.ObjectSpec {
		return catalog.ObjectSpec{
			ThingID: id, Tenant: "t",
			TemplateID: "gauge", TemplateVersion: "v1",
			Properties: map[string]things.Property{"distance": {}},
			Bindings: []things.Binding{
				{DeviceID: "d", Object: rtDistURN, Resource: "5700", Input: "distance"},
			},
		}
	}
	if _, err := rt.RegisterThing("t", c, mk("g1"), 1); err != nil {
		t.Fatalf("register g1: %v", err)
	}
	if _, err := rt.RegisterThing("t", c, mk("g2"), 1); err != nil {
		t.Fatalf("register g2: %v", err)
	}
	pub := &fakePublisher{}
	a := New(nil, nil, nil, rt, pub)
	base := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if err := a.HandleMeasurements(context.Background(), "t", "m1", []things.Observation{rtDistObs("d", 1, base)}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if pub.count() != 2 {
		t.Fatalf("published=%d, want 2 (one per thing)", pub.count())
	}
	if !pub.events[0].PublishedAt.Equal(pub.events[1].PublishedAt) {
		t.Fatalf("PublishedAt differs across things in same batch: %v vs %v",
			pub.events[0].PublishedAt, pub.events[1].PublishedAt)
	}
}
