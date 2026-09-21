# Konsumentbindning för things.v1.* (K1.6)

Exempelregler per pilotmall som en extern konsument skriver mot det
låsta K1-kontraktet. Tjänsten känner inte till sina konsumenter:
reglerna nedan bor hos konsumenten, inte här. `match` på
mall-ID/subtyp/tenant, värden via
`values.<propertyId>.{value,observedAt,quality,unit,source}`,
identitet via deskriptorn, tid via `EventTimestamp`
(`max(values[].observedAt)`), kvalitet via
`ShouldEmitProperty/ShouldEmitEntity`.

Variabelhärledning (låst i `pkg/types`, testad i
`pkg/types/consumer_binding_test.go`):

| Konsumentvariabel | Källa i v1 |
| --- | --- |
| `id` | `thingId` |
| `nameOrID` | `thing.alternativeName > thing.name > thingId` (`NameOrID`), URN via `SafeURISegment` |
| `uuid` | `UUID(thingId)` (sista `/`-segmentet) |
| `type` | `thingType` (mall-ID) eller `thing.subType` via `TypeName(thingType)` (`subType > thingType`) |
| `tenant` | `tenant` (aldrig tom; tom payload nekas) |
| `timestamp` | `EventTimestamp()` = `max(values[].observedAt)` (RFC3339/UTC) |
| `revision` | `configRevision` (låst vid beräkning) |
| `source` | per värde `source` (`device/[channel]/object/resource`) |
| `quality` | per värde `quality`; `invalid` hoppar property, `required` saknad/invalid hoppar entitet |

Tillåtna `match`-fält för saker: `type` (mall-ID), `subType`,
`tenant`. `match.device` för saker är död flagga (lås i K1.2).
Trace endast i transport-headers/OTel, aldrig i payload (beslut).

## wastebin → WasteContainer

```yaml
- match: { kind: thing, type: wastebin }
  entities:
    - id: "urn:ngsi-ld:WasteContainer:{{nameOrID}}"
      type: WasteContainer
      properties:
        - { target: fillingLevel, type: Number, valueFrom: values.fillRate.value, unitFrom: values.fillRate.unit, observedAt: values.fillRate.observedAt, required: true }
        - { target: location, type: Geo, valueFrom: thing.location, required: true }
```

`required: [fillRate]` för modellens `required (id, type, location)`:
partiellt event utan `fillRate` hoppar entiteten
(`ShouldEmitEntity([fillRate]) == false`).

## room → IndoorEnvironmentObserved (medel + direktvärden)

```yaml
- match: { kind: thing, type: room }
  entities:
    - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{nameOrID}}"
      type: IndoorEnvironmentObserved
      properties:
        - { target: temperature, type: Number, valueFrom: values.temperature.average.value, observedAt: values.temperature.average.observedAt }
        - { target: relativeHumidity, type: Number, valueFrom: values.humidity.value, observedAt: values.humidity.observedAt }
```

Individuella `temperature.window/center/door` och `temperature.average`
är separata egenskaper; medelns ingångar är explicita och en ny
mätpunkt läggs aldrig till automatiskt (R4/B4).

## greenhouse → AgriGreenhouse

```yaml
- match: { kind: thing, type: greenhouse }
  entities:
    - id: "urn:ngsi-ld:AgriGreenhouse:{{nameOrID}}"
      type: AgriGreenhouse
      properties:
        - { target: relativeHumidity, type: Number, valueFrom: values.humidity.value, observedAt: values.humidity.observedAt }
        - { target: co2, type: Number, valueFrom: values.co2.value, observedAt: values.co2.observedAt }
```

Lufttemperatur hör till en separat observation från mätvärdet, inte
till växthusentiteten. `alternateName` mappas från sakens
`alternativeName` (samma värde som ID-segmentet) för spårbarhet.
