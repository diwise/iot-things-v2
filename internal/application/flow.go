// Beräkningsflöde Etapp 3: från observationer till aktuella värden,
// historikskrivningar och utgående händelser i en utvärderingsomgång.
//
// Flödet är en ren funktion utan broker eller databas (G8): anroparen
// (senare app.HandleMeasurements med store/outbox) persisterar History och
// publicerar Events. Reglerna följer THNGS_NEW.md kap 6-7:
//
//   - Bindningar matchar signal -> namngiven ingång; obundna signaler
//     ignoreras.
//   - Direktfasen uppdaterar aktuellt per tidsreglerna i evaluate.go
//     (senaste mättid vinner, tie-break mottagningstid/källa). Ogiltig
//     ersätter aldrig aktuellt. Sen observation sparas i historik (B13).
//   - Enheter måste matcha exakt i v1; avvikande enhet ger historikskrivning
//     med kvalitet invalid utan current-uppdatering (B12 strikt).
//   - Härledda egenskaper utvärderas efter direktfasen i beroendeordning,
//     en omgång per rapport: godtycklig ordning inom rapporten ger samma
//     resultat (B14). Ett resultat/event per härledd egenskap.
//   - Medel kräver alla valda ingångar med ok/uncertain; saknad ingång ger
//     inget resultat och inget event (B4). Mättid = senaste bidragande.
//   - Beräkningsgrafen är riktad och acyklisk; cykler nekas.
//   - Tillståndsoperatorn activeTime körs inte i Etapp 3 (Etapp 5); tröskel
//     med hysteres stöds med tillstånd ur aktuellt värde.
package application

import (
	"fmt"
	"sort"
	"time"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/functions"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// HistoryWrite är en historikrad flödet ber anroparen persistera.
// Late markerar sena observationer som sparas utan att bli aktuella (B13).
// Anroparens lagring är idempotent per (mättid, källa, värde), så retry
// konvergerar.
type HistoryWrite struct {
	PropertyID string
	Value      *float64
	Quality    string
	ObservedAt time.Time
	ReceivedAt time.Time
	Source     string
	MessageID  string
	Late       bool
}

// FlowEvent är en utgående logisk händelse per uppdaterad egenskap.
// Stabilt ID bygger anroparen som messageID + egenskap, så återleverans
// dedupliceras hos konsumenten.
type FlowEvent struct {
	PropertyID      string
	Value           float64
	Quality         string
	ObservedAt      time.Time
	Sources         []string
	Available       int
	Expected        int
	Operator        string
	OperatorVersion string
	ConfigRevision  int64
	MessageID       string
}

// FlowOutcome är resultatet av en utvärderingsomgång: nya aktuella värden
// (delta mot inkommande current), historikskrivningar och händelser.
type FlowOutcome struct {
	Updated map[string]CurrentValue
	History []HistoryWrite
	Events  []FlowEvent
}

// EvaluateBatchFlow kör en definierad uppdateringsomgång för cfg mot current
// med observationerna i obs. receivedAt används som mottagningstid där
// observationen saknar egen; messageID spårar ursprung och dedup;
// configRevision stämplar händelser med låst konfigurationsrevision.
func EvaluateBatchFlow(cfg *catalog.EffectiveConfig, current map[string]CurrentValue, obs []things.Observation, receivedAt time.Time, messageID string, configRevision int64) (*FlowOutcome, error) {
	if cfg == nil {
		return nil, fmt.Errorf("flow: nil effective config")
	}
	out := &FlowOutcome{Updated: make(map[string]CurrentValue)}
	working := make(map[string]CurrentValue, len(current)+len(obs))
	for k, v := range current {
		v.Value = cloneFloatPtr(v.Value)
		working[k] = v
	}

	// Deterministisk batch-ordning (B14): sortera observationer nyast först
	// (mättid, mottagningstid, källa) så att godtycklig ordning inom
	// rapporten ger samma vinnare, ett event per egenskap och stabila
	// Late-flaggor oavsett inflödesordning.
	orderedObs := append([]things.Observation(nil), obs...)
	sort.SliceStable(orderedObs, func(i, j int) bool {
		a, b := orderedObs[i], orderedObs[j]
		if !a.ObservedAt.Equal(b.ObservedAt) {
			return a.ObservedAt.After(b.ObservedAt)
		}
		arx, brx := a.ReceivedAt, b.ReceivedAt
		if arx.IsZero() {
			arx = receivedAt
		}
		if brx.IsZero() {
			brx = receivedAt
		}
		if !arx.Equal(brx) {
			return arx.After(brx)
		}
		if a.Signal.ID() != b.Signal.ID() {
			return a.Signal.ID() > b.Signal.ID()
		}
		av, bv := a.Value, b.Value
		if (av == nil) != (bv == nil) {
			return av != nil
		}
		if av != nil && bv != nil && *av != *bv {
			return *av > *bv
		}
		return false
	})

	for _, o := range orderedObs {
		inputs := matchingInputs(cfg.Bindings, o.Signal)
		if len(inputs) == 0 {
			continue // Obunden signal: ingen egenskap berörs.
		}
		rx := o.ReceivedAt
		if rx.IsZero() {
			rx = receivedAt
		}
		quality := string(o.Quality)
		for _, input := range inputs {
			prop, ok := cfg.Properties[input]
			if !ok {
				continue
			}
			q := quality
			if q != functions.QualityOk && q != functions.QualityUncertain && q != functions.QualityInvalid {
				q = functions.QualityInvalid
			}
			if unitMismatch(prop.Unit, o.Unit) {
				q = functions.QualityInvalid
			}
			if o.Value == nil {
				// Saknat värde får aldrig bli numeriskt noll (C6):
				// behandla som invalid, spara i historik utan current/event.
				q = functions.QualityInvalid
			}
			hw := HistoryWrite{
				PropertyID: input,
				Value:      cloneFloatPtr(o.Value),
				Quality:    q,
				ObservedAt: o.ObservedAt,
				ReceivedAt: rx,
				Source:     o.Signal.ID(),
				MessageID:  messageID,
			}
			cur, has := working[input]
			var curPtr *CurrentValue
			if has {
				cp := cur
				curPtr = &cp
			}
			if ShouldBecomeCurrent(curPtr, ObservedInput{
				Value:      o.Value,
				Quality:    q,
				ObservedAt: o.ObservedAt,
				ReceivedAt: rx,
				Source:     o.Signal.ID(),
			}) {
				nv := CurrentValue{
					Value:      cloneFloatPtr(o.Value),
					Quality:    q,
					ObservedAt: o.ObservedAt,
					ReceivedAt: rx,
					Source:     o.Signal.ID(),
					MessageID:  messageID,
				}
				working[input] = nv
				out.Updated[input] = nv
				out.Events = append(out.Events, FlowEvent{
					PropertyID:     input,
					Quality:        q,
					ObservedAt:     o.ObservedAt,
					Sources:        []string{o.Signal.ID()},
					Available:      1,
					Expected:       1,
					ConfigRevision: configRevision,
					MessageID:      messageID,
				})
				if o.Value != nil {
					out.Events[len(out.Events)-1].Value = *o.Value
				}
			} else {
				// Late markerar endast tidsmässigt passerade giltiga
				// observationer (B13). Ogiltiga (B12) sparas utan
				// Late-flagg: de nekas av kvalitet, inte av tid.
				if q == functions.QualityOk || q == functions.QualityUncertain {
					hw.Late = true
				}
			}
			out.History = append(out.History, hw)
		}
	}

	ordered, err := orderRecipes(cfg.Recipes)
	if err != nil {
		return nil, err
	}
	for _, r := range ordered {
		res, skip, err := runRecipe(cfg, working, r)
		if err != nil {
			return nil, fmt.Errorf("flow: recipe %q: %w", r.Name, err)
		}
		if skip {
			continue // Saknad ingång: inget resultat, inget event (B4).
		}
		for _, output := range r.Outputs {
			nv := CurrentValue{
				Value:      cloneFloatPtr(&res.Value),
				Quality:    res.Quality,
				ObservedAt: res.ObservedAt,
				ReceivedAt: receivedAt,
				Source:     "recipe:" + r.Name,
				MessageID:  messageID,
			}
			working[output] = nv
			out.Updated[output] = nv
			out.History = append(out.History, HistoryWrite{
				PropertyID: output,
				Value:      cloneFloatPtr(&res.Value),
				Quality:    res.Quality,
				ObservedAt: res.ObservedAt,
				ReceivedAt: receivedAt,
				Source:     "recipe:" + r.Name,
				MessageID:  messageID,
			})
			out.Events = append(out.Events, FlowEvent{
				PropertyID:      output,
				Value:           res.Value,
				Quality:         res.Quality,
				ObservedAt:      res.ObservedAt,
				Sources:         res.Sources,
				Available:       res.Available,
				Expected:        res.Expected,
				Operator:        r.Operator,
				OperatorVersion: r.Version,
				ConfigRevision:  configRevision,
				MessageID:       messageID,
			})
		}
	}

	sort.SliceStable(out.Events, func(i, j int) bool {
		if out.Events[i].PropertyID != out.Events[j].PropertyID {
			return out.Events[i].PropertyID < out.Events[j].PropertyID
		}
		if !out.Events[i].ObservedAt.Equal(out.Events[j].ObservedAt) {
			return out.Events[i].ObservedAt.Before(out.Events[j].ObservedAt)
		}
		return out.Events[i].Value < out.Events[j].Value
	})
	return out, nil
}

// cloneFloatPtr kopierar ett float-värde så att Updated/History/working inte
// delar pekare med anroparens observationer eller med varandra (C/L: ingen
// append-aliasing, ingen delad state via pekare).
func cloneFloatPtr(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// matchingInputs returnerar de egenskaps-ID:n vars bindningar väljer signalen,
// utan dubbletter och i bindningsordning.
func matchingInputs(bindings []things.Binding, s things.Signal) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, b := range bindings {
		if !b.Matches(s) {
			continue
		}
		if _, ok := seen[b.Input]; ok {
			continue
		}
		seen[b.Input] = struct{}{}
		out = append(out, b.Input)
	}
	return out
}

// unitMismatch rapporterar strikt enhetsavvikelse i v1: båda angivna och
// olika nekas; tom enhet på någondera sidan accepteras (äldre källor anger
// inte alltid enhet).
func unitMismatch(propUnit, obsUnit string) bool {
	return propUnit != "" && obsUnit != "" && propUnit != obsUnit
}

type recipeResult struct {
	Value      float64
	Quality    string
	ObservedAt time.Time
	Sources    []string
	Available  int
	Expected   int
}

// runRecipe exekverar ett recept mot working. skip=true betyder att en
// krävd ingång saknas: inget resultat och inget event.
func runRecipe(cfg *catalog.EffectiveConfig, working map[string]CurrentValue, r catalog.Recipe) (recipeResult, bool, error) {
	vals := make([]functions.NumberWithQuality, 0, len(r.Inputs))
	sources := make([]string, 0, len(r.Inputs))
	var latest time.Time
	for _, in := range r.Inputs {
		cv, ok := working[in]
		if !ok || cv.Value == nil {
			return recipeResult{}, true, nil
		}
		if cv.Quality != functions.QualityOk && cv.Quality != functions.QualityUncertain {
			return recipeResult{}, true, nil
		}
		vals = append(vals, functions.NumberWithQuality{Value: *cv.Value, Quality: cv.Quality})
		sources = append(sources, cv.Source)
		if cv.ObservedAt.After(latest) {
			latest = cv.ObservedAt
		}
	}
	mk := func(v float64, q string) (recipeResult, bool, error) {
		return recipeResult{
			Value: v, Quality: q, ObservedAt: latest,
			Sources:   append([]string(nil), sources...),
			Available: len(r.Inputs), Expected: len(r.Inputs),
		}, false, nil
	}

	switch r.Operator {
	case "passthrough", "direct":
		if len(vals) != 1 {
			return recipeResult{}, false, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		return mk(vals[0].Value, vals[0].Quality)
	case "linear", "calibration":
		if len(vals) != 1 {
			return recipeResult{}, false, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		factor, ok := cfg.ParamValues["factor"]
		if !ok {
			factor = 1
		}
		offset := cfg.ParamValues["offset"]
		return mk(functions.Linear(vals[0].Value, factor, offset), vals[0].Quality)
	case "distanceToLevel", "level":
		if len(vals) != 1 {
			return recipeResult{}, false, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		bottom, ok := cfg.ParamValues["sensorToBottom"]
		if !ok {
			return recipeResult{}, false, fmt.Errorf("operator %q needs param %q", r.Operator, "sensorToBottom")
		}
		v, err := functions.Level(bottom, vals[0].Value)
		if err != nil {
			return recipeResult{}, false, err
		}
		return mk(v, vals[0].Quality)
	case "fillRate", "percent", "percentage":
		if len(vals) != 1 {
			return recipeResult{}, false, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		height, ok := cfg.ParamValues["usableHeight"]
		if !ok {
			return recipeResult{}, false, fmt.Errorf("operator %q needs param %q", r.Operator, "usableHeight")
		}
		v, q, err := functions.FillRate(vals[0].Value, height)
		if err != nil {
			return recipeResult{}, false, err
		}
		if vals[0].Quality == functions.QualityUncertain && q == functions.QualityOk {
			q = functions.QualityUncertain
		}
		return mk(v, q)
	case "mean", "average":
		v, q, err := functions.Mean(vals)
		if err != nil {
			return recipeResult{}, false, err
		}
		return mk(v, q)
	case "min":
		v, q, err := functions.Min(vals)
		if err != nil {
			return recipeResult{}, false, err
		}
		return mk(v, q)
	case "max":
		v, q, err := functions.Max(vals)
		if err != nil {
			return recipeResult{}, false, err
		}
		return mk(v, q)
	case "threshold":
		if len(vals) != 1 {
			return recipeResult{}, false, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		high, ok := cfg.ParamValues["high"]
		if !ok {
			return recipeResult{}, false, fmt.Errorf("operator %q needs param %q", r.Operator, "high")
		}
		low, ok := cfg.ParamValues["low"]
		if !ok {
			return recipeResult{}, false, fmt.Errorf("operator %q needs param %q", r.Operator, "low")
		}
		prev := false
		if len(r.Outputs) > 0 {
			if cv, ok := working[r.Outputs[0]]; ok && cv.Value != nil && *cv.Value != 0 {
				prev = true
			}
		}
		state := functions.ThresholdWithHysteresis(vals[0].Value, high, low, prev)
		v := 0.0
		if state {
			v = 1
		}
		return mk(v, vals[0].Quality)
	case "activeTime", "active_time", "hysteresis":
		return recipeResult{}, false, fmt.Errorf("operator %q executes in Etapp 5, not Etapp 3", r.Operator)
	default:
		return recipeResult{}, false, fmt.Errorf("unknown operator %q", r.Operator)
	}
}

// orderRecipes sorterar recept i beroendeordning (Kahn): ett recept som
// läser en annan recipes utgång körs efter den. Cykler nekas.
func orderRecipes(recipes []catalog.Recipe) ([]catalog.Recipe, error) {
	producer := make(map[string]int, len(recipes))
	for i, r := range recipes {
		for _, output := range r.Outputs {
			if prev, ok := producer[output]; ok {
				return nil, fmt.Errorf("flow: output %q produced by both %q and %q", output, recipes[prev].Name, r.Name)
			}
			producer[output] = i
		}
	}
	indegree := make([]int, len(recipes))
	dependents := make([][]int, len(recipes))
	for i, r := range recipes {
		seen := make(map[int]struct{})
		for _, in := range r.Inputs {
			if p, ok := producer[in]; ok && p != i {
				if _, dup := seen[p]; !dup {
					seen[p] = struct{}{}
					indegree[i]++
					dependents[p] = append(dependents[p], i)
				}
			}
		}
	}
	queue := make([]int, 0, len(recipes))
	for i := range recipes {
		if indegree[i] == 0 {
			queue = append(queue, i)
		}
	}
	ordered := make([]catalog.Recipe, 0, len(recipes))
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		ordered = append(ordered, recipes[n])
		for _, m := range dependents[n] {
			indegree[m]--
			if indegree[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if len(ordered) != len(recipes) {
		return nil, fmt.Errorf("flow: computation graph has a cycle")
	}
	return ordered, nil
}
