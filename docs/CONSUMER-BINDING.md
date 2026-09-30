# Konsumentbindning för things.v1.* (K1.6)

Exempelregler per pilotmall som en extern konsument skriver mot det
låsta K1-kontraktet. Tjänsten känner inte till sina konsumenter:
reglerna nedan bor hos konsumenten, inte här. `match` på
mall-ID/subtyp/tenant, värden via
`values.<propertyId>.{value,observedAt,quality,unit,source}`,
identitet via deskriptorn, tid via `EventTimestamp`
(`max(values[].observedAt)`), kvalitet via
`ShouldEmitProperty/ShouldEmitEntity`.

Bindningen nedan beskriver `iot-transform-fiware` och dess regler. Källpayloaden
definieras i `pkg/types`; måltyp och ID-mall väljs av konsumenten. Standard-ID
använder rått `thingId`, helst en GUID/UUID. Namn och alternativnamn är attribut.
`nameOrID` används inte som regelvariabel eller implicit fallback.

| Konsumentvariabel | Källa i v1 |
| --- | --- |
| `id` | `thingId` |
| `name` | rått `thing.name`, aldrig fallback till ID |
| `alternativeName` | rått `thing.alternativeName` |
| `type` | rått `thingType` (mall-ID) |
| `typeName` | explicit genväg: rå subType om satt, annars thingType |
| `tenant` | `tenant` (aldrig tom; tom payload nekas) |
| `timestamp` | `EventTimestamp()` = `max(values[].observedAt)` (RFC3339/UTC) |
| `source` | per värde `source` (`device/[channel]/object/resource`) |
| `quality` | per värde `quality`; `invalid` hoppar property, `required` saknad/invalid hoppar entitet |

Tillåtna `match`-fält för saker: `type` (mall-ID), `subType`,
`tenant`. `match.device` för saker är död flagga (lås i K1.2).
Trace endast i transport-headers/OTel, aldrig i payload (beslut).

Generella mallfält kan läsas med exempelvis `{{thing.metadata.description}}`,
`{{values."temperature.average".value}}` och `{{relation.to.thingId}}`.
`safeURI` kodar text när regeln uttryckligen behöver URI-segmentkodning.
Egen namnbaserad ID-policy är regelns författares ansvar: ingen automatisk
hantering av namnbyten, kollisioner eller tidigare mål-ID:n införs.

## wastebin → WasteContainer

```yaml
- match: { kind: thing, event: things.v1.values, type: wastebin }
  entities:
    - id: "urn:ngsi-ld:WasteContainer:{{id}}"
      type: WasteContainer
      properties:
        - { target: name, type: Text, field: name, observedAt: none }
        - { target: alternateName, type: Text, field: alternativeName, observedAt: none }
        - { target: fillingLevel, type: Number, field: values.fillRate.value, observedAt: "field:values.fillRate.observedAt", required: true }
        - { target: location, type: Geo, field: thing.location, required: true }
```

Mappningens obligatoriska källa är `fillRate`:
partiellt event utan `fillRate` hoppar entiteten
(`ShouldEmitEntity([fillRate]) == false`).
Detta är en mappningsregel, inte bevis på full Smart Data Models-konformitet.

## room → IndoorEnvironmentObserved (medel + direktvärden)

```yaml
- match: { kind: thing, event: things.v1.values, type: room }
  entities:
    - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{id}}"
      type: IndoorEnvironmentObserved
      properties:
        - { target: name, type: Text, field: name, observedAt: none }
        - { target: temperature, type: Number, field: 'values."temperature.average".value', observedAt: 'field:values."temperature.average".observedAt' }
        - { target: relativeHumidity, type: Number, field: values.humidity.value, observedAt: "field:values.humidity.observedAt" }
```

Individuella `temperature.window/center/door` och `temperature.average`
är separata egenskaper; medelns ingångar är explicita och en ny
mätpunkt läggs aldrig till automatiskt (R4/B4).

## greenhouse → AgriGreenhouse

```yaml
- match: { kind: thing, event: things.v1.values, type: greenhouse }
  entities:
    - id: "urn:ngsi-ld:AgriGreenhouse:{{id}}"
      type: AgriGreenhouse
      properties:
        - { target: name, type: Text, field: name, observedAt: none }
        - { target: alternateName, type: Text, field: alternativeName, observedAt: none }
        - { target: relativeHumidity, type: Number, field: values.humidity.value, observedAt: "field:values.humidity.observedAt" }
        - { target: co2, type: Number, field: values.co2.value, observedAt: "field:values.co2.observedAt" }
```

Lufttemperatur hör till en separat observation från mätvärdet, inte
till växthusentiteten. `alternateName` mappas från sakens
`alternativeName`; ID-segmentet är alltid det stabila käll-ID:t.

## Relationer (partOf set/removed)

Relationshändelser (`things.v1.relations`) bär from/to-descriptors så att
konsumenten kan routa och rendera utan kataloguppslag. `removed: true`
betyder bortkopplad länk; `toId` bär då sista kända mål (vilken länk som
försvann). Saknad rad, DB-fel och korrupt blob skiljs åt i lagringen;
endast verkligt saknat mål ger `target: null` vid läsning (TV-05).

```yaml
# fromThingType/from: barnets mall + deskriptor (alltid satt).
# toThingType/to: förälderns mall + deskriptor (saknas endast om
# föräldersidan är okänd). Fältsökvägarna nedan är de som
# transform-fiware exponerar (relation.{from,to}.*).
- match: { kind: thing, event: things.v1.relations, relation: partOf, type: tank }
  entities:
    - id: "urn:ngsi-ld:AgriGreenhouse:{{relation.to.thingId}}"
      type: AgriGreenhouse
      properties:
        - { target: hasWaterQualityObserved, type: Relationship, array: true, value: "urn:ngsi-ld:WaterQualityObserved:{{relation.from.thingId}}", required: true }
```

Konsumenten väljer modellprefix i regeln och bygger båda målidentiteterna
från råa käll-ID:n, inte namn. `relation.from` och `relation.to` innehåller
generella descriptors med rå typ, undertyp, namn, kategori, plats, metadata
och variantinformation. De innehåller inga modellberoende mål-URN:er.
`array: true` gör en skalär URI till en enelementslista utan dubbelnästling.
Det är inte flerrelationsaggregation: exemplet antar en tank per växthus,
och bortkoppling rensar hela attributet. Set/remove för flera tankar kräver
ett separat tillståndskontrakt.
