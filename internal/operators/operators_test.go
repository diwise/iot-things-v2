package operators

import "testing"

func TestMeanA17(t *testing.T) {
	got, q, err := Mean([]NumberWithQuality{
		{Value: 18, Quality: QualityOk},
		{Value: 21, Quality: QualityOk},
		{Value: 24, Quality: QualityOk},
	})
	if err != nil {
		t.Fatalf("Mean returned error: %v", err)
	}
	if got != 21 {
		t.Fatalf("Mean(18,21,24) = %v, want 21", got)
	}
	if q != QualityOk {
		t.Fatalf("Mean quality = %q, want ok", q)
	}

	// Third point updated 24 -> 27 gives 22, via a second call.
	got, q, err = Mean([]NumberWithQuality{
		{Value: 18, Quality: QualityOk},
		{Value: 21, Quality: QualityOk},
		{Value: 27, Quality: QualityOk},
	})
	if err != nil {
		t.Fatalf("Mean returned error: %v", err)
	}
	if got != 22 {
		t.Fatalf("Mean(18,21,27) = %v, want 22", got)
	}
	if q != QualityOk {
		t.Fatalf("Mean quality = %q, want ok", q)
	}
}

func TestMeanEmptyError(t *testing.T) {
	if _, _, err := Mean(nil); err == nil {
		t.Fatal("Mean(nil) expected error, got nil")
	}
	if _, _, err := Mean([]NumberWithQuality{}); err == nil {
		t.Fatal("Mean(empty) expected error, got nil")
	}
	if _, _, err := Min(nil); err == nil {
		t.Fatal("Min(nil) expected error, got nil")
	}
	if _, _, err := Max(nil); err == nil {
		t.Fatal("Max(nil) expected error, got nil")
	}
}

func TestMeanInvalidDenied(t *testing.T) {
	vals := []NumberWithQuality{
		{Value: 18, Quality: QualityOk},
		{Value: 21, Quality: QualityInvalid},
		{Value: 24, Quality: QualityOk},
	}
	if _, _, err := Mean(vals); err == nil {
		t.Fatal("Mean with invalid expected error, got nil")
	}
	if _, _, err := Min(vals); err == nil {
		t.Fatal("Min with invalid expected error, got nil")
	}
	if _, _, err := Max(vals); err == nil {
		t.Fatal("Max with invalid expected error, got nil")
	}
}

func TestMeanUncertainPropagates(t *testing.T) {
	vals := []NumberWithQuality{
		{Value: 18, Quality: QualityOk},
		{Value: 21, Quality: QualityUncertain},
		{Value: 24, Quality: QualityOk},
	}
	got, q, err := Mean(vals)
	if err != nil {
		t.Fatalf("Mean returned error: %v", err)
	}
	if got != 21 {
		t.Fatalf("Mean = %v, want 21", got)
	}
	if q != QualityUncertain {
		t.Fatalf("Mean quality = %q, want uncertain", q)
	}

	if _, q, err := Min(vals); err != nil || q != QualityUncertain {
		t.Fatalf("Min = (_, %q, %v), want uncertain, nil", q, err)
	}
	if _, q, err := Max(vals); err != nil || q != QualityUncertain {
		t.Fatalf("Max = (_, %q, %v), want uncertain, nil", q, err)
	}
}

func TestMinMaxValues(t *testing.T) {
	vals := []NumberWithQuality{
		{Value: 18, Quality: QualityOk},
		{Value: 21, Quality: QualityOk},
		{Value: 24, Quality: QualityOk},
	}
	min, q, err := Min(vals)
	if err != nil || min != 18 || q != QualityOk {
		t.Fatalf("Min = (%v, %q, %v), want (18, ok, nil)", min, q, err)
	}
	max, q, err := Max(vals)
	if err != nil || max != 24 || q != QualityOk {
		t.Fatalf("Max = (%v, %q, %v), want (24, ok, nil)", max, q, err)
	}
}

func TestLevel(t *testing.T) {
	got, err := Level(1.2, 0.9)
	if err != nil {
		t.Fatalf("Level returned error: %v", err)
	}
	if got != 0.30000000000000004 && got-0.3 > 1e-9 {
		t.Fatalf("Level(1.2, 0.9) = %v, want 0.3", got)
	}

	if _, err := Level(0, 0.5); err == nil {
		t.Fatal("Level with distanceToBottom=0 expected error, got nil")
	}
	if _, err := Level(-1, 0.5); err == nil {
		t.Fatal("Level with distanceToBottom<0 expected error, got nil")
	}
}

func TestFillRate(t *testing.T) {
	got, q, err := FillRate(0.3, 1.2)
	if err != nil {
		t.Fatalf("FillRate returned error: %v", err)
	}
	if got != 25 {
		t.Fatalf("FillRate(0.3, 1.2) = %v, want 25", got)
	}
	if q != QualityOk {
		t.Fatalf("FillRate quality = %q, want ok", q)
	}

	// Out-of-range below zero: kept, flagged uncertain.
	got, q, err = FillRate(-0.1, 1.2)
	if err != nil {
		t.Fatalf("FillRate returned error: %v", err)
	}
	if q != QualityUncertain {
		t.Fatalf("FillRate quality = %q, want uncertain", q)
	}
	if got >= 0 {
		t.Fatalf("FillRate out-of-range value = %v, want < 0 (kept, not clamped)", got)
	}

	// Out-of-range above 100: kept, flagged uncertain.
	got, q, err = FillRate(2.0, 1.2)
	if err != nil {
		t.Fatalf("FillRate returned error: %v", err)
	}
	if q != QualityUncertain {
		t.Fatalf("FillRate quality = %q, want uncertain", q)
	}
	if got <= 100 {
		t.Fatalf("FillRate out-of-range value = %v, want > 100 (kept, not clamped)", got)
	}

	if _, _, err := FillRate(0.5, 0); err == nil {
		t.Fatal("FillRate with height=0 expected error, got nil")
	}
	if _, _, err := FillRate(0.5, -1); err == nil {
		t.Fatal("FillRate with height<0 expected error, got nil")
	}
}

func TestLinear(t *testing.T) {
	if got := Linear(10, 2, 5); got != 25 {
		t.Fatalf("Linear(10, 2, 5) = %v, want 25", got)
	}
	if got := Linear(10, 1, 0); got != 10 {
		t.Fatalf("Linear(10, 1, 0) = %v, want 10", got)
	}
}

func TestPercent(t *testing.T) {
	got, err := Percent(30, 120)
	if err != nil {
		t.Fatalf("Percent returned error: %v", err)
	}
	if got != 25 {
		t.Fatalf("Percent(30, 120) = %v, want 25", got)
	}
	if _, err := Percent(1, 0); err == nil {
		t.Fatal("Percent with ref=0 expected error, got nil")
	}
}

func TestThresholdWithHysteresis(t *testing.T) {
	// Below low stays/turns false.
	if got := ThresholdWithHysteresis(1, 10, 5, true); got != false {
		t.Fatal("value<=low should turn false")
	}
	// Above high turns true.
	if got := ThresholdWithHysteresis(11, 10, 5, false); got != true {
		t.Fatal("value>=high should turn true")
	}
	// Boundaries inclusive.
	if got := ThresholdWithHysteresis(10, 10, 5, false); got != true {
		t.Fatal("value==high should turn true")
	}
	if got := ThresholdWithHysteresis(5, 10, 5, true); got != false {
		t.Fatal("value==low should turn false")
	}
	// Between keeps state (stability, no flapping).
	if got := ThresholdWithHysteresis(7, 10, 5, true); got != true {
		t.Fatal("between should keep true")
	}
	if got := ThresholdWithHysteresis(7, 10, 5, false); got != false {
		t.Fatal("between should keep false")
	}
}

func TestActiveTime(t *testing.T) {
	var s ActiveTimeState

	// inactive -> active stores start.
	s = Update(s, true, 100)
	if !s.Active || s.StartUnix != 100 || s.AccumulatedSec != 0 {
		t.Fatalf("after activation = %+v, want {Active:true Start:100 Acc:0}", s)
	}

	// Tick while active: no accumulation, start kept.
	s = Update(s, true, 160)
	if !s.Active || s.StartUnix != 100 || s.AccumulatedSec != 0 {
		t.Fatalf("tick while active must not accumulate: %+v", s)
	}

	// active -> inactive accumulates 60s.
	s = Update(s, false, 160)
	if s.Active || s.AccumulatedSec != 60 {
		t.Fatalf("after deactivation = %+v, want inactive with 60 accumulated", s)
	}

	// Tick while inactive: no accumulation.
	s = Update(s, false, 200)
	if s.Active || s.AccumulatedSec != 60 {
		t.Fatalf("tick while inactive must not accumulate: %+v", s)
	}

	// Second period 300 -> 360 accumulates another 60s = 120 total.
	s = Update(s, true, 300)
	s = Update(s, true, 330) // tick, no accumulation
	if s.AccumulatedSec != 60 {
		t.Fatalf("tick must not accumulate: %+v", s)
	}
	s = Update(s, false, 360)
	if s.Active || s.AccumulatedSec != 120 {
		t.Fatalf("after second period = %+v, want 120 accumulated", s)
	}
}

func TestFlagOutOfRange(t *testing.T) {
	if got := FlagOutOfRange(50, 0, 100); got != QualityOk {
		t.Fatalf("in range = %q, want ok", got)
	}
	if got := FlagOutOfRange(0, 0, 100); got != QualityOk {
		t.Fatalf("boundary min = %q, want ok", got)
	}
	if got := FlagOutOfRange(100, 0, 100); got != QualityOk {
		t.Fatalf("boundary max = %q, want ok", got)
	}
	if got := FlagOutOfRange(-1, 0, 100); got != QualityUncertain {
		t.Fatalf("below = %q, want uncertain", got)
	}
	if got := FlagOutOfRange(101, 0, 100); got != QualityUncertain {
		t.Fatalf("above = %q, want uncertain", got)
	}
}
