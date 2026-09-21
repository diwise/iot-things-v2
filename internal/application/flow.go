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
	"encoding/json"
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
	PropertyID string    `json:"propertyId"`
	Value      *float64  `json:"value,omitempty"`
	Quality    string    `json:"quality,omitempty"`
	ObservedAt time.Time `json:"observedAt"`
	ReceivedAt time.Time `json:"receivedAt"`
	Source     string    `json:"source,omitempty"`
	MessageID  string    `json:"messageId,omitempty"`
	Late       bool      `json:"late,omitempty"`
}

// FlowEvent är en utgående logisk händelse per uppdaterad egenskap.
// Stabilt ID bygger anroparen som messageID + egenskap, så återleverans
// dedupliceras hos konsumenten.
type FlowEvent struct {
	PropertyID      string    `json:"propertyId"`
	Value           float64   `json:"value"`
	Quality         string    `json:"quality,omitempty"`
	ObservedAt      time.Time `json:"observedAt"`
	ReceivedAt      time.Time `json:"receivedAt"`
	Sources         []string  `json:"sources,omitempty"`
	Available       int       `json:"available,omitempty"`
	Expected        int       `json:"expected,omitempty"`
	Operator        string    `json:"operator,omitempty"`
	OperatorVersion string    `json:"operatorVersion,omitempty"`
	ConfigRevision  int64     `json:"configRevision,omitempty"`
	MessageID       string    `json:"messageId,omitempty"`
}

// StoredProcessor är versionerat beständigt tillstånd för en tillstånds-
// bärande processor, nycklat per utegenskap hos anroparen. StateJSON
// avkodas av respektive operator (activeTime: functions.ActiveTimeState).
// Operator/version stämplas vid skrivning: vid versionsbyte nollställs
// tillståndet och resultatet flaggas uncertain i stället för att tyst
// återanvända ett främmande tillstånd.
type StoredProcessor struct {
	Operator  string `json:"operator"`
	Version   string `json:"version,omitempty"`
	StateJSON []byte `json:"state,omitempty"`
}

// FlowOutcome är resultatet av en utvärderingsomgång: nya aktuella värden
// (delta mot inkommande current), historikskrivningar, händelser samt
// uppdaterade processortillstånd (endast berörda).
type FlowOutcome struct {
	Updated    map[string]CurrentValue
	History    []HistoryWrite
	Events     []FlowEvent
	ProcStates map[string]StoredProcessor
}

// EvaluateBatchFlow kör en definierad uppdateringsomgång för cfg mot current
// med observationerna i obs. proc bär persistade processortillstånd per
// utegenskap (nil vid första rundan); returnerade ProcStates sparar
// anroparen för restart-säkerhet. receivedAt används som mottagningstid där
// observationen saknar egen; messageID spårar ursprung och dedup;
// configRevision stämplar händelser med låst konfigurationsrevision.
//
// Efter direktfasen körs endast (transitivt) berörda recept (L5): ett recept
// körs om någon ingång uppdaterats i rundan, annars återanvänds aktuellt
// värde tyst. Paramändringar slår därför igenom vid nästa beröringsrunda
// för berörda recept.
func EvaluateBatchFlow(cfg *catalog.EffectiveConfig, current map[string]CurrentValue, proc map[string]StoredProcessor, obs []things.Observation, receivedAt time.Time, messageID string, configRevision int64) (*FlowOutcome, error) {
	if cfg == nil {
		return nil, fmt.Errorf("flow: nil effective config")
	}
	out := &FlowOutcome{Updated: make(map[string]CurrentValue), ProcStates: make(map[string]StoredProcessor)}
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
			// Bool-ingest: en boolesk observation (utan numeriskt värde)
			// mappas till 1/0 så att tillståndssignaler (presence,
			// digitalInput) flödar genom samma pipeline som trösklar
			// rapporterar den. Booleska poster bär ingen enhet och
			// unitMismatch accepterar tom enhet, så ingen särregel behövs
			// där. Numeriskt värde vinner om båda är satta.
			num := o.Value
			if num == nil && o.BoolValue != nil {
				num = new(float64)
				*num = functions.BoolToFloat(*o.BoolValue)
			}
			if unitMismatch(prop.Unit, o.Unit) {
				q = functions.QualityInvalid
			}
			if num == nil {
				// Saknat värde får aldrig bli numeriskt noll (C6):
				// behandla som invalid, spara i historik utan current/event.
				q = functions.QualityInvalid
			}
			hw := HistoryWrite{
				PropertyID: input,
				Value:      cloneFloatPtr(num),
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
				Value:      num,
				Quality:    q,
				ObservedAt: o.ObservedAt,
				ReceivedAt: rx,
				Source:     o.Signal.ID(),
			}) {
				nv := CurrentValue{
					Value:      cloneFloatPtr(num),
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
					ReceivedAt:     rx,
					Sources:        []string{o.Signal.ID()},
					Available:      1,
					Expected:       1,
					ConfigRevision: configRevision,
					MessageID:      messageID,
				})
				if num != nil {
					out.Events[len(out.Events)-1].Value = *num
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
	changed := make(map[string]bool, len(out.Updated))
	for k := range out.Updated {
		changed[k] = true
	}
	for _, r := range ordered {
		// Statisk validering gäller varje runda (C4): felaktig config
		// nekas även när rundan inte berör receptet.
		if err := validateRecipeStatic(cfg, r); err != nil {
			return nil, fmt.Errorf("flow: recipe %q: %w", r.Name, err)
		}
		touched := false
		for _, in := range r.Inputs {
			if changed[in] {
				touched = true
				break
			}
		}
		if !touched {
			continue // Oberörd i rundan: aktuellt värde står kvar tyst.
		}
		res, skip, stateOut, err := runRecipe(cfg, working, proc, r)
		if err != nil {
			return nil, fmt.Errorf("flow: recipe %q: %w", r.Name, err)
		}
		if stateOut != nil && len(r.Outputs) > 0 {
			// Tillstånd sparas även vid skip (t.ex. deltats första
			// seedningsrunda ger inget resultat men måste minnas).
			// Övriga operatorer sätter aldrig state vid skip.
			out.ProcStates[r.Outputs[0]] = *stateOut
		}
		if skip {
			continue // Saknad ingång: inget resultat och inget event (B4).
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
			changed[output] = true
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
				ReceivedAt:      receivedAt,
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

// canonicalOperator normaliserar alias till kanonisk form så att
// versionsjämförelser inte nollställer i onödan vid aliasbyte
// (activeTime/active_time, threshold/hysteresis, etc.).
func canonicalOperator(op string) string {
	switch op {
	case "direct":
		return "passthrough"
	case "calibration":
		return "linear"
	case "level":
		return "distanceToLevel"
	case "percent", "percentage":
		return "fillRate"
	case "average":
		return "mean"
	case "hysteresis":
		return "threshold"
	case "active_time":
		return "activeTime"
	case "counterRising":
		return "counter"
	case "difference":
		return "delta"
	case "accumulate":
		return "sum"
	case "minus":
		return "subtract"
	default:
		return op
	}
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

// cloneBytes kopierar tillståndsblobbar så att outcome/store/anropare inte
// delar StateJSON-underliggande array (ingen tillstånds-aliasing).
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
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

// recipeParamKey resolves a recipe parameter by position: the recipe's
// declared name wins (per-recipe values for repeated operators), the
// legacy global key is the fallback for older templates.
func recipeParamKey(r catalog.Recipe, i int, legacy string) string {
	if i < len(r.Params) {
		return r.Params[i]
	}
	return legacy
}

// runRecipe exekverar ett recept mot working. skip=true betyder att en
// krävd ingång saknas: inget resultat och inget event. Tillståndsbärande
// operatorer (activeTime) läser proc per utegenskap och returnerar
// uppdaterat tillstånd i stateOut (nil för tillståndslösa operatorer).
//
// Parametrar löses per recept: receptets deklarerade namn (Params) vinner,
// så att samma operator kan användas flera gånger med olika värden (t.ex.
// två nivågivare på olika höjd). Äldre mallar utan deklaration faller
// tillbaka på de globala legacy-nycklarna.
func runRecipe(cfg *catalog.EffectiveConfig, working map[string]CurrentValue, proc map[string]StoredProcessor, r catalog.Recipe) (recipeResult, bool, *StoredProcessor, error) {
	vals := make([]functions.NumberWithQuality, 0, len(r.Inputs))
	sources := make([]string, 0, len(r.Inputs))
	var latest time.Time
	for _, in := range r.Inputs {
		cv, ok := working[in]
		if !ok || cv.Value == nil {
			return recipeResult{}, true, nil, nil
		}
		if cv.Quality != functions.QualityOk && cv.Quality != functions.QualityUncertain {
			return recipeResult{}, true, nil, nil
		}
		vals = append(vals, functions.NumberWithQuality{Value: *cv.Value, Quality: cv.Quality})
		sources = append(sources, cv.Source)
		if cv.ObservedAt.After(latest) {
			latest = cv.ObservedAt
		}
	}
	mk := func(v float64, q string) (recipeResult, bool, *StoredProcessor, error) {
		return recipeResult{
			Value: v, Quality: q, ObservedAt: latest,
			Sources:   append([]string(nil), sources...),
			Available: len(r.Inputs), Expected: len(r.Inputs),
		}, false, nil, nil
	}

	switch r.Operator {
	case "passthrough", "direct":
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		return mk(vals[0].Value, vals[0].Quality)
	case "linear", "calibration":
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		factor, offset := 1.0, 0.0
		if len(r.Params) > 0 {
			f, ok := cfg.ParamValues[r.Params[0]]
			if !ok {
				return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, r.Params[0])
			}
			factor = f
		} else if f, ok := cfg.ParamValues["factor"]; ok {
			factor = f
		}
		if len(r.Params) > 1 {
			o, ok := cfg.ParamValues[r.Params[1]]
			if !ok {
				return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, r.Params[1])
			}
			offset = o
		} else if o, ok := cfg.ParamValues["offset"]; ok {
			offset = o
		}
		return mk(functions.Linear(vals[0].Value, factor, offset), vals[0].Quality)
	case "distanceToLevel", "level":
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		key := recipeParamKey(r, 0, "sensorToBottom")
		bottom, ok := cfg.ParamValues[key]
		if !ok {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, key)
		}
		v, err := functions.Level(bottom, vals[0].Value)
		if err != nil {
			return recipeResult{}, false, nil, err
		}
		return mk(v, vals[0].Quality)
	case "fillRate", "percent", "percentage":
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		key := recipeParamKey(r, 0, "usableHeight")
		height, ok := cfg.ParamValues[key]
		if !ok {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, key)
		}
		v, q, err := functions.FillRate(vals[0].Value, height)
		if err != nil {
			return recipeResult{}, false, nil, err
		}
		if vals[0].Quality == functions.QualityUncertain && q == functions.QualityOk {
			q = functions.QualityUncertain
		}
		return mk(v, q)
	case "mean", "average":
		v, q, err := functions.Mean(vals)
		if err != nil {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q: %w", r.Operator, err)
		}
		return mk(v, q)
	case "subtract", "minus":
		// Differens mellan två ingångar (t.ex. temperatur vid två
		// ingångar): första minus andra, med tecken. Båda ingångar
		// krävs som för mean (B4); uncertain propagerar.
		if len(vals) != 2 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly two inputs", r.Operator)
		}
		q := functions.QualityOk
		if vals[0].Quality == functions.QualityUncertain || vals[1].Quality == functions.QualityUncertain {
			q = functions.QualityUncertain
		}
		return mk(functions.Subtract(vals[0].Value, vals[1].Value), q)
	case "min":
		v, q, err := functions.Min(vals)
		if err != nil {
			return recipeResult{}, false, nil, err
		}
		return mk(v, q)
	case "max":
		v, q, err := functions.Max(vals)
		if err != nil {
			return recipeResult{}, false, nil, err
		}
		return mk(v, q)
	case "threshold", "hysteresis":
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		highKey := recipeParamKey(r, 0, "high")
		high, ok := cfg.ParamValues[highKey]
		if !ok {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, highKey)
		}
		lowKey := recipeParamKey(r, 1, "low")
		low, ok := cfg.ParamValues[lowKey]
		if !ok {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs param %q", r.Operator, lowKey)
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
	case "activeTime", "active_time":
		// Aktiv tid (Etapp 5): ackumulerar sekunder medan ingången är
		// aktiv (värde != 0). Endast händelsetid (mättid) driver
		// tillståndet (B15, ingen tick): sena observationer som inte blir
		// aktuella når aldrig hit, så ackumulerad tid korrigeras aldrig
		// godtyckligt. Utdata = fullbordade + pågående sekunder vid
		// senaste bidragande mättid.
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		if len(r.Outputs) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		key := r.Outputs[0]
		var persisted functions.ActiveTimeState
		versionReset := false
		if prev, ok := proc[key]; ok {
			if canonicalOperator(prev.Operator) != canonicalOperator(r.Operator) || prev.Version != r.Version {
				versionReset = true
			} else if len(prev.StateJSON) > 0 {
				if err := json.Unmarshal(prev.StateJSON, &persisted); err != nil {
					return recipeResult{}, false, nil, fmt.Errorf("operator %q: decode state: %w", r.Operator, err)
				}
			}
		}
		active := vals[0].Value != 0
		nowUnix := latest.Unix()
		next := functions.Update(persisted, active, nowUnix)
		total := next.AccumulatedSec
		if next.Active && nowUnix > next.StartUnix {
			total += nowUnix - next.StartUnix
		}
		quality := vals[0].Quality
		if versionReset {
			quality = functions.QualityUncertain
		}
		stateJSON, err := json.Marshal(next)
		if err != nil {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q: encode state: %w", r.Operator, err)
		}
		res, skip, _, err := mk(float64(total), quality)
		if err != nil || skip {
			return res, skip, nil, err
		}
		return res, false, &StoredProcessor{Operator: r.Operator, Version: r.Version, StateJSON: stateJSON}, nil
	case "counter", "counterRising", "counterFalling", "counterBoth":
		// Räknare (passager, pumpstarter): räknar flanker på en
		// boolesk-ingång (0/1 via bool-ingest eller threshold) med
		// beständigt tillstånd. Utdata = kumulativt antal. Dagsantal
		// och andra fönster härleds via historikfrågor, inte via
		// väggklocka i operatorn.
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		if len(r.Outputs) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		key := r.Outputs[0]
		var persisted functions.CounterState
		versionReset := false
		if prev, ok := proc[key]; ok {
			if canonicalOperator(prev.Operator) != canonicalOperator(r.Operator) || prev.Version != r.Version {
				versionReset = true
			} else if len(prev.StateJSON) > 0 {
				if err := json.Unmarshal(prev.StateJSON, &persisted); err != nil {
					return recipeResult{}, false, nil, fmt.Errorf("operator %q: decode state: %w", r.Operator, err)
				}
			}
		}
		edge := "rising"
		switch canonicalOperator(r.Operator) {
		case "counterFalling":
			edge = "falling"
		case "counterBoth":
			edge = "both"
		}
		next, _ := functions.CountEdge(persisted, vals[0].Value != 0, edge)
		quality := vals[0].Quality
		if versionReset {
			quality = functions.QualityUncertain
		}
		stateJSON, err := json.Marshal(next)
		if err != nil {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q: encode state: %w", r.Operator, err)
		}
		res, skip, _, err := mk(float64(next.Count), quality)
		if err != nil || skip {
			return res, skip, nil, err
		}
		return res, false, &StoredProcessor{Operator: r.Operator, Version: r.Version, StateJSON: stateJSON}, nil
	case "delta", "difference":
		// Differens (förbrukning per period ur kumulativa mätare):
		// utdata = ingång - föregående ingång. Första observationen
		// seedar endast tillståndet och ger inget resultat.
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		if len(r.Outputs) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		key := r.Outputs[0]
		var persisted functions.DeltaState
		versionReset := false
		if prev, ok := proc[key]; ok {
			if canonicalOperator(prev.Operator) != canonicalOperator(r.Operator) || prev.Version != r.Version {
				versionReset = true
			} else if len(prev.StateJSON) > 0 {
				if err := json.Unmarshal(prev.StateJSON, &persisted); err != nil {
					return recipeResult{}, false, nil, fmt.Errorf("operator %q: decode state: %w", r.Operator, err)
				}
			}
		}
		if versionReset {
			persisted = functions.DeltaState{}
		}
		next, d, ok := functions.DeltaNext(persisted, vals[0].Value)
		if !ok {
			stateJSON, err := json.Marshal(next)
			if err != nil {
				return recipeResult{}, false, nil, fmt.Errorf("operator %q: encode state: %w", r.Operator, err)
			}
			return recipeResult{}, true, &StoredProcessor{Operator: r.Operator, Version: r.Version, StateJSON: stateJSON}, nil
		}
		quality := vals[0].Quality
		if versionReset {
			quality = functions.QualityUncertain
		}
		stateJSON, err := json.Marshal(next)
		if err != nil {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q: encode state: %w", r.Operator, err)
		}
		res, skip, _, err := mk(d, quality)
		if err != nil || skip {
			return res, skip, nil, err
		}
		return res, false, &StoredProcessor{Operator: r.Operator, Version: r.Version, StateJSON: stateJSON}, nil
	case "sum", "accumulate":
		// Summa (nederbörd, energipulser): adderar observerat värde per
		// utvärderad runda till löpande total. För kumulativa mätare
		// används delta i stället.
		if len(vals) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one input", r.Operator)
		}
		if len(r.Outputs) != 1 {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		key := r.Outputs[0]
		var persisted functions.SumState
		versionReset := false
		if prev, ok := proc[key]; ok {
			if canonicalOperator(prev.Operator) != canonicalOperator(r.Operator) || prev.Version != r.Version {
				versionReset = true
			} else if len(prev.StateJSON) > 0 {
				if err := json.Unmarshal(prev.StateJSON, &persisted); err != nil {
					return recipeResult{}, false, nil, fmt.Errorf("operator %q: decode state: %w", r.Operator, err)
				}
			}
		}
		if versionReset {
			persisted = functions.SumState{}
		}
		next := functions.SumNext(persisted, vals[0].Value)
		quality := vals[0].Quality
		if versionReset {
			quality = functions.QualityUncertain
		}
		stateJSON, err := json.Marshal(next)
		if err != nil {
			return recipeResult{}, false, nil, fmt.Errorf("operator %q: encode state: %w", r.Operator, err)
		}
		res, skip, _, err := mk(next.Total, quality)
		if err != nil || skip {
			return res, skip, nil, err
		}
		return res, false, &StoredProcessor{Operator: r.Operator, Version: r.Version, StateJSON: stateJSON}, nil
	default:
		return recipeResult{}, false, nil, fmt.Errorf("unknown operator %q", r.Operator)
	}
}

// validateRecipeStatic kontrollerar receptets statiska form mot
// konfigurationsparametrarna utan värden: okänd operator, fel antal
// in-/utgångar, saknade parametrar och ogiltiga parametervärden nekas (C4).
// Hålls synkad med runRecipe: varje statiskt krav där upprepas här, inklusive
// positiv höjd/avstånd (S3) så att felaktig config nekas varje runda även när
// rundan inte berör receptet.
func validateRecipeStatic(cfg *catalog.EffectiveConfig, r catalog.Recipe) error {
	needParams := func(names ...string) error {
		for _, p := range names {
			if _, ok := cfg.ParamValues[p]; !ok {
				return fmt.Errorf("operator %q needs param %q", r.Operator, p)
			}
		}
		return nil
	}
	needPositive := func(name string) error {
		v, ok := cfg.ParamValues[name]
		if !ok {
			return fmt.Errorf("operator %q needs param %q", r.Operator, name)
		}
		if v <= 0 {
			return fmt.Errorf("operator %q needs param %q > 0, got %v", r.Operator, name, v)
		}
		return nil
	}
	needInputs := func(n int) error {
		if len(r.Inputs) != n {
			return fmt.Errorf("operator %q needs exactly %d input(s)", r.Operator, n)
		}
		return nil
	}
	switch r.Operator {
	case "passthrough", "direct":
		return needInputs(1)
	case "linear", "calibration":
		if err := needInputs(1); err != nil {
			return err
		}
		// Deklarerade parametrar måste finnas; utan deklaration gäller
		// legacy-nycklar med standardvärden (factor 1, offset 0).
		var declared []string
		if len(r.Params) > 0 {
			declared = append(declared, r.Params[0])
		}
		if len(r.Params) > 1 {
			declared = append(declared, r.Params[1])
		}
		return needParams(declared...)
	case "distanceToLevel", "level":
		if err := needInputs(1); err != nil {
			return err
		}
		// K2: endast vertical-down med ett bottenavstånd; fler
		// deklarerade parametrar signalerar geometri som inte hedras.
		// Speglar catalog.validateRecipe (vinkelparametern angle nekas
		// vid publicering/kompilering och syns aldrig här).
		if len(r.Params) > 1 {
			return fmt.Errorf("operator %q supports only vertical-down geometry (K2): single bottom-distance param, got %d declared params", r.Operator, len(r.Params))
		}
		return needPositive(recipeParamKey(r, 0, "sensorToBottom"))
	case "fillRate", "percent", "percentage":
		if err := needInputs(1); err != nil {
			return err
		}
		return needPositive(recipeParamKey(r, 0, "usableHeight"))
	case "mean", "average", "min", "max":
		if len(r.Inputs) == 0 {
			return fmt.Errorf("operator %q needs at least one input", r.Operator)
		}
		return nil
	case "subtract", "minus":
		if len(r.Inputs) != 2 {
			return fmt.Errorf("operator %q needs exactly two inputs", r.Operator)
		}
		return nil
	case "threshold", "hysteresis":
		if err := needInputs(1); err != nil {
			return err
		}
		return needParams(recipeParamKey(r, 0, "high"), recipeParamKey(r, 1, "low"))
	case "activeTime", "active_time":
		if err := needInputs(1); err != nil {
			return err
		}
		if len(r.Outputs) != 1 {
			return fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		return nil
	case "counter", "counterRising", "counterFalling", "counterBoth":
		if err := needInputs(1); err != nil {
			return err
		}
		if len(r.Outputs) != 1 {
			return fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		return nil
	case "delta", "difference", "sum", "accumulate":
		if err := needInputs(1); err != nil {
			return err
		}
		if len(r.Outputs) != 1 {
			return fmt.Errorf("operator %q needs exactly one output", r.Operator)
		}
		return nil
	default:
		return fmt.Errorf("unknown operator %q", r.Operator)
	}
}

// orderRecipes sorterar recept i beroendeordning (Kahn): ett recept som
// läser en annan recipes utgång körs efter den. Cykler nekas, inklusive
// självloop (recept som läser sin egen utgång, C2 riktad acyklisk graf).
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
			if p, ok := producer[in]; ok {
				if p == i {
					return nil, fmt.Errorf("flow: recipe %q reads its own output %q (cycle)", r.Name, in)
				}
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
