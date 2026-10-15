// CSV-import av v1-saker (THINGS_FILE, things.csv).
//
// Formatet är v1:s seed-format: ';'-separerade rader med 10 kolumner och
// huvudrad (id;type;subType;name;decsription;location;tenant;tags;bindings;args).
// Bindningar är 'device|channel|object|resource|input' separerade med komma,
// eller ett bart enhets-ID som expanderas till typens alla ingångar (samma
// semantik som v1). Args är en Python-dict med LevelConfig-nycklar
// (maxd/maxl/meanl/offset/angle, enkelfnuttar accepteras).
//
// Mappning till v2:
//   - type Container -> mallen wastebin, övriga typer mappar 1:1 (gemener).
//   - v1-ingångar digitalInput -> sakens booleska egenskap (overflow/door/run
//     för sewer/passage/pumpingstation), övriga namn oförändrade.
//   - Harledda obligatoriska egenskaper (level, fillRate, ...) aktiveras
//     automatiskt från mallen; de behöver inga bindningar.
//   - maxd -> sensorToBottom-override, maxl -> usableHeight-override.
//     angle/meanl/offset skilt från 0 kan inte uttryckas och nekar raden.
//   - "lat,lon" -> Point [lon,lat]; tom eller 0,0 betyder okänd plats (v1
//     skrev 0 vid parsningsfel) och ger ingen plats.
//   - subType/description/taggar bevaras i metadata (v2 har inga taggar);
//     tenant krävs.
//
// Importen är idempotent: redan skapade saker räknas som skipped, så den kan
// köras vid varje start efter katalogseeden. Felaktiga rader samlas per rad
// och stoppar inte övriga; strukturellt trasig CSV (fel kolumnantal) är
// fatal. Saker hör inte hemma i catalog-seed.json; de importeras här.
package application

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"

	"github.com/diwise/iot-things-v2/internal/application/catalog"
	"github.com/diwise/iot-things-v2/internal/application/things"
)

// ImportFailure describes one rejected CSV row; other rows still import.
type ImportFailure struct {
	Row   int
	ID    string
	Error string
}

// csvInput is one v1 type input (name + source signal), mirroring the v1
// input table for bare-device expansion.
type csvInput struct {
	name, object, resource string
}

// csvTypeInputs mirrors the v1 input table (type -> named source signals).
var csvTypeInputs = map[string][]csvInput{
	"building": {
		{"energy", "urn:oma:lwm2m:ext:3331", "5700"},
		{"power", "urn:oma:lwm2m:ext:3328", "5700"},
		{"temperature", "urn:oma:lwm2m:ext:3303", "5700"},
	},
	"container": {
		{"distance", "urn:oma:lwm2m:ext:3330", "5700"},
	},
	"lifebuoy": {
		{"presence", "urn:oma:lwm2m:ext:3200", "5500"},
		{"presence", "urn:oma:lwm2m:ext:3302", "5500"},
	},
	"passage": {
		{"digitalInput", "urn:oma:lwm2m:ext:3200", "5500"},
	},
	"pointofinterest": {
		{"temperature", "urn:oma:lwm2m:ext:3303", "5700"},
	},
	"pumpingstation": {
		{"digitalInput", "urn:oma:lwm2m:ext:3200", "5500"},
	},
	"room": {
		{"temperature", "urn:oma:lwm2m:ext:3303", "5700"},
		{"humidity", "urn:oma:lwm2m:ext:3304", "5700"},
		{"illuminance", "urn:oma:lwm2m:ext:3301", "5700"},
		{"co2", "urn:oma:lwm2m:ext:3428", "17"},
	},
	"sewer": {
		{"distance", "urn:oma:lwm2m:ext:3330", "5700"},
		{"digitalInput", "urn:oma:lwm2m:ext:3200", "5500"},
	},
	"watermeter": {
		{"volume", "urn:oma:lwm2m:ext:3424", "1"},
		{"leakage", "urn:oma:lwm2m:ext:3424", "10"},
		{"backflow", "urn:oma:lwm2m:ext:3424", "11"},
		{"fraud", "urn:oma:lwm2m:ext:3424", "13"},
	},
}

// csvTemplate maps a v1 type to its v2 template. Container (alla subtyper)
// täcks av wastebin-mallen med modellmått per rad.
func csvTemplate(v1type string) (string, bool) {
	if v1type == "container" {
		return "wastebin", true
	}
	if _, ok := csvTypeInputs[v1type]; ok {
		return v1type, true
	}
	return "", false
}

// latestSeedTemplate returnerar senaste publicerade version av en mall
// (seed-filen innehåller efter rensning bara senaste version per mall:
// wastebin/room/lifebuoy/passage/pumpingstation/sewer/watermeter v2,
// building/pointofinterest v3, greenhouse v2, tank v6; desk/sink borttagna).
func latestSeedTemplate(cat *catalog.Catalog, templateID string) (catalog.TemplateSpec, string, bool) {
	var best catalog.TemplateSpec
	bestVer := ""
	bestNum := -1
	for _, spec := range cat.ListTemplates() {
		if spec.Template.ID != templateID {
			continue
		}
		n := 0
		if _, err := fmt.Sscanf(spec.Template.Version, "v%d", &n); err != nil {
			continue
		}
		if n > bestNum {
			bestNum = n
			best = spec
			bestVer = spec.Template.Version
		}
	}
	if bestNum < 0 {
		return catalog.TemplateSpec{}, "", false
	}
	return best, bestVer, true
}

// csvProperty maps a v1 input name to the v2 property for templates where
// the boolean input feeds a named state property.
func csvProperty(templateID, v1input string) string {
	switch templateID {
	case "sewer":
		if v1input == "digitalInput" {
			return "overflow"
		}
	case "passage":
		if v1input == "digitalInput" {
			return "door"
		}
	case "pumpingstation":
		if v1input == "digitalInput" {
			return "run"
		}
	}
	return v1input
}

// ImportCSV imports v1 things.csv rows as things. It returns added and
// skipped (already exists) counts plus per-row failures; err is only set
// for structurally broken input.
func (m *Manager) ImportCSV(ctx context.Context, r io.Reader) (added, skipped int, failures []ImportFailure, err error) {
	f := csv.NewReader(r)
	f.Comma = ';'
	f.FieldsPerRecord = -1

	rowNum := 0
	for {
		record, readErr := f.Read()
		if readErr == io.EOF {
			return added, skipped, failures, nil
		}
		rowNum++
		if readErr != nil {
			return added, skipped, failures, fmt.Errorf("import: csv row %d: %w", rowNum, readErr)
		}
		if rowNum == 1 {
			continue // huvudrad
		}
		if len(record) != 10 {
			return added, skipped, failures, fmt.Errorf("import: csv row %d: expected 10 columns, got %d", rowNum, len(record))
		}
		obj, fail := m.csvRow(record)
		if fail != "" {
			failures = append(failures, ImportFailure{Row: rowNum, ID: record[0], Error: fail})
			continue
		}
		if _, err := m.CreateObject(ctx, obj.Tenant, obj); err != nil {
			// Redan importerad vid tidigare start: hoppa över.
			if errors.Is(err, things.ErrConflict) {
				skipped++
				continue
			}
			failures = append(failures, ImportFailure{Row: rowNum, ID: obj.ThingID, Error: err.Error()})
			continue
		}
		added++
	}
}

// csvRow maps one 10-column v1 record to an ObjectSpec; fail text is empty
// on success. Columns: id;type;subType;name;decsription;location;tenant;
// tags;bindings;args.
func (m *Manager) csvRow(record []string) (catalog.ObjectSpec, string) {
	var obj catalog.ObjectSpec
	fail := func(format string, args ...any) (catalog.ObjectSpec, string) {
		return catalog.ObjectSpec{}, fmt.Sprintf(format, args...)
	}

	id := strings.TrimSpace(record[0])
	if err := things.ValidateThingID(id); err != nil {
		return fail("invalid id: %v", err)
	}
	v1type := strings.ToLower(strings.TrimSpace(record[1]))
	templateID, ok := csvTemplate(v1type)
	if !ok {
		return fail("unknown type %q", record[1])
	}
	tmpl, tmplVersion, ok := latestSeedTemplate(m.cat, templateID)
	if !ok {
		return fail("template %s not published (seed catalog first)", templateID)
	}
	tenant := strings.TrimSpace(record[6])
	if tenant == "" {
		return fail("missing tenant")
	}

	obj = catalog.ObjectSpec{
		ThingID:         id,
		Tenant:          tenant,
		Name:            strings.TrimSpace(record[3]),
		TemplateID:      templateID,
		TemplateVersion: tmplVersion,
		Properties:      map[string]things.Property{},
	}

	// Härledda obligatoriska egenskaper aktiveras utan bindningar.
	for _, req := range tmpl.Template.Required {
		obj.Properties[req] = things.Property{}
	}

	inputs, ok := csvTypeInputs[v1type]
	if !ok {
		return fail("unknown type %q", record[1])
	}
	bindings, failText := csvBindings(record[8], inputs)
	if failText != "" {
		return fail("%s", failText)
	}
	for _, b := range bindings {
		prop := csvProperty(templateID, b.Input)
		b.Input = prop
		obj.Bindings = append(obj.Bindings, b)
		if _, exists := obj.Properties[prop]; !exists {
			obj.Properties[prop] = things.Property{}
		}
	}

	overrides, meta, failText := csvArgs(record[9])
	if failText != "" {
		return fail("%s", failText)
	}
	if len(overrides) > 0 {
		obj.ParamOverrides = overrides
	}

	if loc, failText := csvLocation(strings.TrimSpace(record[5])); failText != "" {
		return fail("%s", failText)
	} else if loc != nil {
		obj.Location = loc
	}

	md := map[string]string{}
	maps.Copy(md, meta)
	// K1: subType och alternativeName befordras till förstklassiga fält
	// (bakåtkompatibelt: de stannar även i metadata för äldre konsumenter).
	if sub := strings.TrimSpace(record[2]); sub != "" {
		md["subType"] = sub
		obj.SubType = sub
	}
	if alt, ok := meta["alternativeName"]; ok && strings.TrimSpace(alt) != "" {
		obj.AlternativeName = strings.TrimSpace(alt)
	}
	if desc := strings.TrimSpace(record[4]); desc != "" {
		md["description"] = desc
	}
	if tags := strings.TrimSpace(record[7]); tags != "" {
		md["tags"] = tags
	}
	if len(md) > 0 {
		obj.Metadata = md
	}

	return obj, ""
}

// csvBindings parses the bindings column: 'device|channel|object|resource|input'
// segments separated by comma, or a bare device ID expanded to all type inputs.
// A malformed explicit segment rejects the row (never silently dropped:
// a missing binding would create a thing without its intended signal).
func csvBindings(col string, inputs []csvInput) ([]things.Binding, string) {
	var out []things.Binding
	for seg := range strings.SplitSeq(col, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if !strings.Contains(seg, "|") {
			for _, in := range inputs {
				out = append(out, things.Binding{
					DeviceID: seg,
					Object:   in.object,
					Resource: in.resource,
					Input:    in.name,
				})
			}
			continue
		}
		parts := strings.Split(seg, "|")
		if len(parts) != 5 {
			return nil, fmt.Sprintf("invalid binding %q (want device|channel|object|resource|input)", seg)
		}
		out = append(out, things.Binding{
			DeviceID: strings.TrimSpace(parts[0]),
			Channel:  strings.TrimSpace(parts[1]),
			Object:   strings.TrimSpace(parts[2]),
			Resource: strings.TrimSpace(parts[3]),
			Input:    strings.TrimSpace(parts[4]),
		})
	}
	return out, ""
}

// csvArgs maps LevelConfig args to parameter overrides. maxd/maxl become
// sensorToBottom/usableHeight; angle/meanl/offset other than 0 cannot be
// expressed and reject the row. String values are annotations, not
// parameters: they are preserved as metadata instead of rejected.
func csvArgs(col string) (map[string]float64, map[string]string, string) {
	col = strings.TrimSpace(col)
	if col == "" || col == "{}" {
		return nil, nil, ""
	}
	normalised := strings.ReplaceAll(col, "'", "\"")
	var raw map[string]any
	if err := json.Unmarshal([]byte(normalised), &raw); err != nil {
		return nil, nil, fmt.Sprintf("invalid args %q", col)
	}
	out := map[string]float64{}
	meta := map[string]string{}
	for k, v := range raw {
		num, ok := v.(float64)
		if !ok {
			if s, ok := v.(string); ok {
				meta[k] = s
				continue
			}
			return nil, nil, fmt.Sprintf("args %q: %q must be numeric or string", col, k)
		}
		switch k {
		case "maxd":
			out["sensorToBottom"] = num
		case "maxl":
			out["usableHeight"] = num
		case "angle", "meanl", "offset":
			if num != 0 {
				return nil, nil, fmt.Sprintf("args %q: %q=%v cannot be expressed (level supports only vertical-down geometry, K2)", col, k, num)
			}
		default:
			return nil, nil, fmt.Sprintf("args %q: unknown numeric key %q", col, k)
		}
	}
	return out, meta, ""
}

// csvLocation parses "lat,lon". Empty or 0,0 means unknown location (v1
// wrote 0 on parse failure); malformed non-empty rejects the row.
func csvLocation(col string) (*things.Location, string) {
	if col == "" {
		return nil, ""
	}
	parts := strings.Split(col, ",")
	if len(parts) != 2 {
		return nil, fmt.Sprintf("invalid location %q (want lat,lon)", col)
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil {
		return nil, fmt.Sprintf("invalid location %q (want lat,lon)", col)
	}
	if lat == 0 && lon == 0 {
		return nil, ""
	}
	loc := &things.Location{Type: "Point"}
	raw, _ := json.Marshal([]float64{lon, lat})
	loc.Coordinates = raw
	if err := loc.Validate(); err != nil {
		return nil, fmt.Sprintf("invalid location %q: %v", col, err)
	}
	return loc, ""
}
