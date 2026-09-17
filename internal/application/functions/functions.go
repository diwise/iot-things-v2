package functions

import "errors"

// Quality values as plain strings to avoid an import cycle against things.
// Valid values are "ok", "uncertain" and "invalid".
const (
	QualityOk        = "ok"
	QualityUncertain = "uncertain"
	QualityInvalid   = "invalid"
)

// NumberWithQuality is a numeric input with an explicit quality flag.
type NumberWithQuality struct {
	Value   float64
	Quality string
}

// Mean returns the unweighted arithmetic mean of vals.
//
// Rules (THNGS_NEW.md C3/C4 + B4/B12/B16):
//   - empty list -> error (invalid configuration)
//   - any "invalid" (or unknown) quality -> error, all inputs must be ok/uncertain
//   - any "uncertain" -> result "uncertain", otherwise "ok"
func Mean(vals []NumberWithQuality) (float64, string, error) {
	if len(vals) == 0 {
		return 0, "", errors.New("functions: mean requires at least one input")
	}

	quality := QualityOk
	sum := 0.0
	for _, v := range vals {
		switch v.Quality {
		case QualityOk:
		case QualityUncertain:
			quality = QualityUncertain
		default:
			return 0, "", errors.New("functions: mean requires all inputs ok/uncertain")
		}
		sum += v.Value
	}

	return sum / float64(len(vals)), quality, nil
}

// Min returns the minimum value of vals with the same quality rules as Mean.
func Min(vals []NumberWithQuality) (float64, string, error) {
	if len(vals) == 0 {
		return 0, "", errors.New("functions: min requires at least one input")
	}

	quality := QualityOk
	min := vals[0].Value
	for _, v := range vals {
		switch v.Quality {
		case QualityOk:
		case QualityUncertain:
			quality = QualityUncertain
		default:
			return 0, "", errors.New("functions: min requires all inputs ok/uncertain")
		}
		if v.Value < min {
			min = v.Value
		}
	}

	return min, quality, nil
}

// Max returns the maximum value of vals with the same quality rules as Mean.
func Max(vals []NumberWithQuality) (float64, string, error) {
	if len(vals) == 0 {
		return 0, "", errors.New("functions: max requires at least one input")
	}

	quality := QualityOk
	max := vals[0].Value
	for _, v := range vals {
		switch v.Quality {
		case QualityOk:
		case QualityUncertain:
			quality = QualityUncertain
		default:
			return 0, "", errors.New("functions: max requires all inputs ok/uncertain")
		}
		if v.Value > max {
			max = v.Value
		}
	}

	return max, quality, nil
}

// Level converts a measured distance to a level for a vertical,
// downward-facing sensor:
//
//	level = distanceToBottom - distance
//
// distanceToBottom is the distance from the sensor to the empty bottom.
// It must be > 0.
func Level(distanceToBottom, distance float64) (float64, error) {
	if distanceToBottom <= 0 {
		return 0, errors.New("functions: level requires distanceToBottom > 0")
	}

	return distanceToBottom - distance, nil
}

// FillRate returns 100 * level / usableHeight without clamping.
//
// Rules:
//   - usableHeight <= 0 -> error
//   - result < 0 or > 100 is kept as-is with quality "uncertain",
//     otherwise quality is "ok" (no silent clamping/dropping).
func FillRate(level, usableHeight float64) (float64, string, error) {
	if usableHeight <= 0 {
		return 0, "", errors.New("functions: fillrate requires usableHeight > 0")
	}

	rate := 100 * level / usableHeight
	if rate < 0 || rate > 100 {
		return rate, QualityUncertain, nil
	}

	return rate, QualityOk, nil
}

// Linear applies factor and offset: value * factor + offset.
func Linear(value, factor, offset float64) float64 {
	return value*factor + offset
}

// Percent relates value to a reference: 100 * value / ref.
// ref == 0 -> error.
func Percent(value, ref float64) (float64, error) {
	if ref == 0 {
		return 0, errors.New("functions: percent requires ref != 0")
	}

	return 100 * value / ref, nil
}

// ThresholdWithHysteresis maps value to a stable boolean state:
// true if value >= high, false if value <= low, otherwise keep state.
func ThresholdWithHysteresis(value, high, low float64, state bool) bool {
	if value >= high {
		return true
	}
	if value <= low {
		return false
	}

	return state
}

// ActiveTimeState is the persistent state for the active-time operator.
type ActiveTimeState struct {
	Active         bool
	StartUnix      int64
	AccumulatedSec int64
}

// Update advances ActiveTimeState to the new observed active flag at nowUnix.
//
//   - inactive -> active: store start time, accumulated time unchanged
//   - active -> active (tick): no accumulation, start time unchanged
//   - active -> inactive: accumulate nowUnix-StartUnix, clear start time
//   - inactive -> inactive: no change
//
// There is no scheduled tick; state changes only on new observations.
// A backwards clock (nowUnix < StartUnix) accumulates zero.
func Update(state ActiveTimeState, active bool, nowUnix int64) ActiveTimeState {
	switch {
	case !state.Active && active:
		state.Active = true
		state.StartUnix = nowUnix
	case state.Active && !active:
		if nowUnix > state.StartUnix {
			state.AccumulatedSec += nowUnix - state.StartUnix
		}
		state.Active = false
		state.StartUnix = 0
	default:
		// Tick without transition: active->active keeps StartUnix,
		// inactive->inactive keeps everything. No accumulation.
		state.Active = active
	}

	return state
}

// FlagOutOfRange returns "ok" when min <= value <= max,
// otherwise "uncertain".
func FlagOutOfRange(value, min, max float64) string {
	if value < min || value > max {
		return QualityUncertain
	}

	return QualityOk
}

// BoolToFloat maps a boolean observation onto the numeric pipeline:
// true -> 1, false -> 0. Boolean state (presence, door, pump run signal)
// thereby flows through the same operators as thresholds report it
// (threshold outputs 1/0, activeTime treats != 0 as active).
func BoolToFloat(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// CounterState is the persistent state for the counter operator:
// the cumulative count plus the previous input level for edge detection.
type CounterState struct {
	Count      int64
	LastActive bool
}

// CountEdge advances CounterState for the new observed active flag and
// reports whether the edge counted. Edge selects which transitions count:
// "rising" (default), "falling" or "both". The zero value counts the first
// active observation as a rising edge, mirroring a sensor that starts idle.
//
// State changes only on new observations, like the other stateful
// operators; retries converge because the runtime deduplicates batches
// before evaluation.
func CountEdge(state CounterState, active bool, edge string) (CounterState, bool) {
	var counted bool

	switch edge {
	case "falling":
		counted = state.LastActive && !active
	case "both":
		counted = state.LastActive != active
	default: // "rising" and anything else: conservative default
		counted = !state.LastActive && active
	}

	if counted {
		state.Count++
	}

	state.LastActive = active

	return state, counted
}

// DeltaState is the persistent state for the delta operator: the previous
// input value. The first observation only seeds the state and yields no
// result (like a missing required input).
type DeltaState struct {
	HasLast   bool
	LastValue float64
}

// DeltaNext advances DeltaState with the new observed value and reports
// the difference (value - previous). ok is false on the first observation
// (no previous yet) and on operator version reset, where the state is
// discarded rather than silently reused.
func DeltaNext(state DeltaState, value float64) (DeltaState, float64, bool) {
	if !state.HasLast {
		return DeltaState{HasLast: true, LastValue: value}, 0, false
	}

	return DeltaState{HasLast: true, LastValue: value}, value - state.LastValue, true
}

// SumState is the persistent state for the sum operator: the running total.
//
// Each evaluated round adds the observed input value once, so the operator
// suits increment-per-report sensors (rain gauge tips, energy pulses). For
// cumulative meters use delta instead: re-reported levels would otherwise
// be added repeatedly.
type SumState struct {
	Total float64
}

// SumNext advances SumState by adding the new observed value.
func SumNext(state SumState, value float64) SumState {
	state.Total += value

	return state
}

// Subtract returns the difference between two inputs: a - b.
// The signed result keeps direction (which entrance is warmer);
// use threshold or absolute handling downstream if only the magnitude
// matters. Quality combination follows the multi-input rule (uncertain
// propagates) in the flow layer.
func Subtract(a, b float64) float64 {
	return a - b
}
