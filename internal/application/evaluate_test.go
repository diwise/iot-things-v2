package application

import (
	"testing"
	"time"
)

func TestShouldBecomeCurrentNilBecomesCurrent(t *testing.T) {
	obs := ObservedInput{Value: new(float64(1)), Quality: "ok", ObservedAt: time.Now(), ReceivedAt: time.Now()}
	if !ShouldBecomeCurrent(nil, obs) {
		t.Fatal("nil current ska ersättas")
	}
}

func TestShouldBecomeCurrentInvalidNeverReplaces(t *testing.T) {
	now := time.Now()
	cur := &CurrentValue{Value: new(float64(1)), Quality: "ok", ObservedAt: now, ReceivedAt: now}
	// Senare ObservedAt men invalid => ändå false.
	obs := ObservedInput{Value: new(float64(99)), Quality: "invalid", ObservedAt: now.Add(time.Hour), ReceivedAt: now.Add(time.Hour)}
	if ShouldBecomeCurrent(cur, obs) {
		t.Fatal("invalid får aldrig ersätta current")
	}
	// Även mot nil current ska invalid nekas.
	if ShouldBecomeCurrent(nil, obs) {
		t.Fatal("invalid får inte bli current ens från nil")
	}
}

func TestShouldBecomeCurrentNewerObservedWins(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: new(float64(1)), ObservedAt: base, ReceivedAt: base}
	newer := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base.Add(time.Second), ReceivedAt: base}
	if !ShouldBecomeCurrent(cur, newer) {
		t.Fatal("senare ObservedAt ska vinna")
	}
}

func TestShouldBecomeCurrentLateSavedAsHistory(t *testing.T) {
	// Sen (äldre) observation: blir ej current men ska sparas som historik.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: new(float64(2)), ObservedAt: base.Add(time.Minute), ReceivedAt: base.Add(time.Minute)}
	late := ObservedInput{Value: new(float64(1)), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(2 * time.Minute)}
	if ShouldBecomeCurrent(cur, late) {
		t.Fatal("äldre ObservedAt ska inte bli current")
	}
}

func TestShouldBecomeCurrentReceivedAtTieBreak(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: new(float64(1)), ObservedAt: base, ReceivedAt: base, Source: "a"}
	laterRx := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(time.Second), Source: "a"}
	if !ShouldBecomeCurrent(cur, laterRx) {
		t.Fatal("senare ReceivedAt ska vinna vid lika ObservedAt")
	}
	earlierRx := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(-time.Second), Source: "z"}
	if ShouldBecomeCurrent(cur, earlierRx) {
		t.Fatal("tidigare ReceivedAt ska förlora vid lika ObservedAt")
	}
}

func TestShouldBecomeCurrentSourceOrdering(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: new(float64(1)), ObservedAt: base, ReceivedAt: base, Source: "a"}
	greater := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "b"}
	if !ShouldBecomeCurrent(cur, greater) {
		t.Fatal("större Source ska vinna vid lika ObservedAt+ReceivedAt")
	}
	smaller := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "a"}
	curB := &CurrentValue{Value: new(float64(1)), ObservedAt: base, ReceivedAt: base, Source: "b"}
	if ShouldBecomeCurrent(curB, smaller) {
		t.Fatal("mindre Source ska förlora vid lika tidsstämplar")
	}
	equal := ObservedInput{Value: new(float64(2)), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "a"}
	if ShouldBecomeCurrent(cur, equal) {
		t.Fatal("lika Source ska behålla befintlig (false)")
	}
}

func TestShouldBecomeCurrentRejectsUnknownQuality(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: new(float64(1)), Quality: "ok", ObservedAt: base, ReceivedAt: base}
	for _, q := range []string{"", "good", "bogus", "INVALID"} {
		obs := ObservedInput{Value: new(float64(2)), Quality: q, ObservedAt: base.Add(time.Hour), ReceivedAt: base.Add(time.Hour)}
		if ShouldBecomeCurrent(cur, obs) {
			t.Fatalf("kvalitet %q ska aldrig bli current", q)
		}
		if ShouldBecomeCurrent(nil, obs) {
			t.Fatalf("kvalitet %q ska inte bli current ens från nil", q)
		}
	}
}
