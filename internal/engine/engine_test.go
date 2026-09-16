package engine

import (
	"testing"
	"time"
)

func fptr(v float64) *float64 { return &v }

func TestShouldBecomeCurrentNilBecomesCurrent(t *testing.T) {
	obs := ObservedInput{Value: fptr(1), Quality: "ok", ObservedAt: time.Now(), ReceivedAt: time.Now()}
	if !ShouldBecomeCurrent(nil, obs) {
		t.Fatal("nil current ska ersättas")
	}
}

func TestShouldBecomeCurrentInvalidNeverReplaces(t *testing.T) {
	now := time.Now()
	cur := &CurrentValue{Value: fptr(1), Quality: "ok", ObservedAt: now, ReceivedAt: now}
	// Senare ObservedAt men invalid => ändå false.
	obs := ObservedInput{Value: fptr(99), Quality: "invalid", ObservedAt: now.Add(time.Hour), ReceivedAt: now.Add(time.Hour)}
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
	cur := &CurrentValue{Value: fptr(1), ObservedAt: base, ReceivedAt: base}
	newer := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base.Add(time.Second), ReceivedAt: base}
	if !ShouldBecomeCurrent(cur, newer) {
		t.Fatal("senare ObservedAt ska vinna")
	}
}

func TestShouldBecomeCurrentLateSavedAsHistory(t *testing.T) {
	// Sen (äldre) observation: blir ej current men ska sparas som historik.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: fptr(2), ObservedAt: base.Add(time.Minute), ReceivedAt: base.Add(time.Minute)}
	late := ObservedInput{Value: fptr(1), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(2 * time.Minute)}
	if ShouldBecomeCurrent(cur, late) {
		t.Fatal("äldre ObservedAt ska inte bli current")
	}
	if !LateSavedAsHistory {
		t.Fatal("LateSavedAsHistory ska vara true: sen observation sparas som historik")
	}
}

func TestShouldBecomeCurrentReceivedAtTieBreak(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: fptr(1), ObservedAt: base, ReceivedAt: base, Source: "a"}
	laterRx := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(time.Second), Source: "a"}
	if !ShouldBecomeCurrent(cur, laterRx) {
		t.Fatal("senare ReceivedAt ska vinna vid lika ObservedAt")
	}
	earlierRx := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base, ReceivedAt: base.Add(-time.Second), Source: "z"}
	if ShouldBecomeCurrent(cur, earlierRx) {
		t.Fatal("tidigare ReceivedAt ska förlora vid lika ObservedAt")
	}
}

func TestShouldBecomeCurrentSourceOrdering(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: fptr(1), ObservedAt: base, ReceivedAt: base, Source: "a"}
	greater := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "b"}
	if !ShouldBecomeCurrent(cur, greater) {
		t.Fatal("större Source ska vinna vid lika ObservedAt+ReceivedAt")
	}
	smaller := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "a"}
	curB := &CurrentValue{Value: fptr(1), ObservedAt: base, ReceivedAt: base, Source: "b"}
	if ShouldBecomeCurrent(curB, smaller) {
		t.Fatal("mindre Source ska förlora vid lika tidsstämplar")
	}
	equal := ObservedInput{Value: fptr(2), Quality: "ok", ObservedAt: base, ReceivedAt: base, Source: "a"}
	if ShouldBecomeCurrent(cur, equal) {
		t.Fatal("lika Source ska behålla befintlig (false)")
	}
}

func TestEvaluateBatchSingleResult(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)
	inputs := map[string]CurrentValue{
		"a": {Value: fptr(10), Quality: "ok", ObservedAt: t0, Source: "s1"},
		"b": {Value: fptr(20), Quality: "ok", ObservedAt: t1, Source: "s2"},
	}
	res, err := EvaluateBatch(inputs, []string{"a", "b"})
	if err != nil {
		t.Fatalf("oväntat fel: %v", err)
	}
	if res == nil {
		t.Fatal("väntat ett resultat")
	}
	if res.Value != 15 {
		t.Fatalf("väntat medel 15, fick %v", res.Value)
	}
	if res.Quality != "ok" {
		t.Fatalf("väntat good, fick %q", res.Quality)
	}
	if !res.ObservedAt.Equal(t1) {
		t.Fatalf("väntat senaste ObservedAt %v, fick %v", t1, res.ObservedAt)
	}
	if len(res.Sources) != 2 {
		t.Fatalf("väntat 2 källor, fick %v", res.Sources)
	}
}

func TestEvaluateBatchUncertainPropagates(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inputs := map[string]CurrentValue{
		"a": {Value: fptr(10), Quality: "ok", ObservedAt: t0, Source: "s1"},
		"b": {Value: fptr(20), Quality: "uncertain", ObservedAt: t0, Source: "s2"},
	}
	res, err := EvaluateBatch(inputs, []string{"a", "b"})
	if err != nil {
		t.Fatalf("oväntat fel: %v", err)
	}
	if res.Quality != "uncertain" {
		t.Fatalf("väntat uncertain, fick %q", res.Quality)
	}
}

func TestEvaluateBatchMissingRequiredErrors(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inputs := map[string]CurrentValue{
		"a": {Value: fptr(10), Quality: "ok", ObservedAt: t0, Source: "s1"},
	}
	if _, err := EvaluateBatch(inputs, []string{"a", "b"}); err == nil {
		t.Fatal("väntat fel vid saknad ingång")
	}
	if _, err := EvaluateBatch(inputs, nil); err == nil {
		t.Fatal("väntat fel vid tom required-lista")
	}
}

func TestShouldBecomeCurrentRejectsUnknownQuality(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &CurrentValue{Value: fptr(1), Quality: "ok", ObservedAt: base, ReceivedAt: base}
	for _, q := range []string{"", "good", "bogus", "INVALID"} {
		obs := ObservedInput{Value: fptr(2), Quality: q, ObservedAt: base.Add(time.Hour), ReceivedAt: base.Add(time.Hour)}
		if ShouldBecomeCurrent(cur, obs) {
			t.Fatalf("kvalitet %q ska aldrig bli current", q)
		}
		if ShouldBecomeCurrent(nil, obs) {
			t.Fatalf("kvalitet %q ska inte bli current ens från nil", q)
		}
	}
}

func TestEvaluateBatchRejectsInvalidQuality(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, q := range []string{"invalid", "", "good", "bogus"} {
		inputs := map[string]CurrentValue{
			"a": {Value: fptr(10), Quality: "ok", ObservedAt: t0, Source: "s1"},
			"b": {Value: fptr(20), Quality: q, ObservedAt: t0, Source: "s2"},
		}
		if _, err := EvaluateBatch(inputs, []string{"a", "b"}); err == nil {
			t.Fatalf("väntat fel för kvalitet %q (medel kräver alla ok/uncertain)", q)
		}
	}
}
