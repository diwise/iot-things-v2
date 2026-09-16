package application

import (
	"fmt"
	"time"
)

// Tidsregler B3/B13/B14/B15 (minnesimplementation, stdlib only).
// Egna minimala typer används lokalt för att undvika importcykel mot
// things; API kan anpassas mot things av integrator.

// LateSavedAsHistory dokumenterar beslutet att en sen (äldre ObservedAt)
// observation inte blir current men ändå sparas som historik.
const LateSavedAsHistory = true

// CurrentValue är det gällande värdet för en egenskap.
type CurrentValue struct {
	Value      *float64
	Quality    string
	ObservedAt time.Time
	ReceivedAt time.Time
	Source     string
	MessageID  string
}

// ObservedInput är en inkommande observation som ska prövas mot current.
type ObservedInput struct {
	Value      *float64
	Quality    string
	ObservedAt time.Time
	ReceivedAt time.Time
	Source     string
}

// ShouldBecomeCurrent avgör om obs ska ersätta cur som gällande värde.
//   - endast Quality "ok"/"uncertain" kan bli current ("invalid" och
//     okänd/tom kvalitet nekas alltid, även mot nil current).
//   - cur == nil => true (för giltig kvalitet).
//   - senare ObservedAt vinner; tidigare förlorar.
//   - vid lika ObservedAt vinner senare ReceivedAt.
//   - vid lika ObservedAt+ReceivedAt avgör Source i strängordning
//     (större Source vinner; lika Source => false, behåll befintlig).
func ShouldBecomeCurrent(cur *CurrentValue, obs ObservedInput) bool {
	if obs.Quality != "ok" && obs.Quality != "uncertain" {
		return false
	}
	if cur == nil {
		return true
	}
	if obs.ObservedAt.After(cur.ObservedAt) {
		return true
	}
	if obs.ObservedAt.Before(cur.ObservedAt) {
		return false
	}
	if obs.ReceivedAt.After(cur.ReceivedAt) {
		return true
	}
	if obs.ReceivedAt.Before(cur.ReceivedAt) {
		return false
	}
	return obs.Source > cur.Source
}

// BatchResult är resultatet av en batch-/aggregeringsutvärdering.
type BatchResult struct {
	PropertyID string
	Value      float64
	Quality    string
	ObservedAt time.Time
	Sources    []string
}

// EvaluateBatch kräver att samtliga required-nycklar finns i inputs och att
// varje bidragande har giltig kvalitet ("ok"/"uncertain"); "invalid" eller
// okänd/tom kvalitet ger fel (B4/B12: medel kräver alla ok/uncertain).
// Medel av värdena returneras; Quality blir "uncertain" om någon
// bidragande är "uncertain" (annars "ok"); ObservedAt sätts till den
// senaste bidragande tidpunkten. Sources följer required-ordningen.
// En anrop per härledd egenskap ger ett resultat/event per egenskap (B14).
// PropertyID lämnas tomt: anroparen sätter målegenskapen.
func EvaluateBatch(inputs map[string]CurrentValue, required []string) (*BatchResult, error) {
	if len(required) == 0 {
		return nil, fmt.Errorf("batch: no required inputs")
	}
	sum := 0.0
	quality := "ok"
	var latest time.Time
	sources := make([]string, 0, len(required))
	for _, name := range required {
		cv, ok := inputs[name]
		if !ok {
			return nil, fmt.Errorf("batch: missing required input %q", name)
		}
		if cv.Value == nil {
			return nil, fmt.Errorf("batch: missing value for required input %q", name)
		}
		sum += *cv.Value
		switch cv.Quality {
		case "ok":
		case "uncertain":
			quality = "uncertain"
		default:
			return nil, fmt.Errorf("batch: invalid quality for required input %q: %q (want ok/uncertain)", name, cv.Quality)
		}
		if cv.ObservedAt.After(latest) {
			latest = cv.ObservedAt
		}
		sources = append(sources, cv.Source)
	}
	return &BatchResult{
		Value:      sum / float64(len(required)),
		Quality:    quality,
		ObservedAt: latest,
		Sources:    sources,
	}, nil
}
