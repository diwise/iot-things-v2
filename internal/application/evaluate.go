package application

import (
	"time"
)

// Tidsregler B3/B13/B14/B15 (minnesimplementation, stdlib only).
// Egna minimala typer används lokalt för att undvika importcykel mot
// things; API kan anpassas mot things av integrator.

// CurrentValue är det gällande värdet för en egenskap. Unit är fryst per
// configRevision (konvertering endast via explicit convert-regel): gamla
// värden märks aldrig om under oförenliga enheter/definitioner.
// ConfigRevision är den låsta konfigurationsrevisionen vid beräkning.
// Noll/false bevaras (nil betyder saknat, inte noll); subsekundprecision
// (RFC3339Nano, mikrosekund-dedup); varje värde behåller egen tid, enhet,
// kvalitet och proveniens över konfigurationsbyte och omstart.
type CurrentValue struct {
	Value          *float64  `json:"value,omitempty"`
	Quality        string    `json:"quality,omitempty"`
	Unit           string    `json:"unit,omitempty"`
	ObservedAt     time.Time `json:"observedAt"`
	ReceivedAt     time.Time `json:"receivedAt"`
	Source         string    `json:"source,omitempty"`
	MessageID      string    `json:"messageId,omitempty"`
	ConfigRevision int64     `json:"configRevision,omitempty"`
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
