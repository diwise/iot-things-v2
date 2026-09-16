# Systemintegration iot-things-v2 (Etapp 7)

## 1. Producent/konsument-karta

| Flöde | Typ | Kontrakt | Version |
| --- | --- | --- | --- |
| `iot-agent` -> `iot-core` | Kommando `MessageReceived` (`{pack, timestamp}`) | `iot-core/pkg/messaging/events` | oförändrat |
| `iot-core` -> `message.accepted` | Topic `{pack, timestamp}` + content-type per pack | `iot-core/pkg/messaging/{topics,events}` | oförändrat |
| `message.accepted` -> `iot-events` | broadcast-prenumeration | egen handler | oberörd av v2 |
| `message.accepted` -> `iot-things` (v1) | broadcast-prenumeration | oförändrat | oberörd av v2 |
| `message.accepted` -> `iot-things-v2` | broadcast-prenumeration (`RegisterTopicMessageHandler`) | samma kontrakt, endast läsning | nytt |
| `message.accepted` -> `iot-transform-fiware` | broadcast-prenumeration | oförändrat | oberörd av v2 |
| `iot-things-v2` -> `things.v1.values` | Topic, schema `v1` | `iot-things-v2/pkg/types` | nytt, inga konsumenter ännu |
| `iot-things-v2` -> `things.v1.relations` | Topic, schema `v1` | `iot-things-v2/pkg/types` | nytt, inga konsumenter ännu |
| `iot-things` (v1) -> `thing.updated` | Topic, wire-format låst | `iot-things/pkg/types` + fiware-kuvertlås | **orört (B20)** |
| `thing.updated` -> `iot-transform-fiware` | broadcast-prenumeration (vid sidan av `message.accepted`) | fiware `ThingFromJSON` (`id/type/thing/tenant/timestamp`) | oberörd av v2 |
| `diwise-web` -> `iot-things` | HTTP `/api/v0` (iot-things `apiPrefix`; webben använder konfigurerad `THINGS_URL`) | v1-svar | orört; v2-`/api/v1` är separat leverans |

Verifierat 2026-09-16:

- `RegisterTopicMessageHandler` = transient broadcast-kö per instans
  (`messaging-golang`: `subscribe("", TransientBroadcastQueue(), ...)`).
  v2 bredvid v1 stjäl inga meddelanden; båda får allt. Transient betyder
  också: backlog under frånkoppling sparas ej (samma som v1; beständigt
  delat abonnemang är ett senare driftbeslut, påverkar ej kontrakt).
- `message.accepted` har inget rapport-ID i payload (medvetet, core
  `observations-contract.md §6`); dedup sker på transport-`MessageID`
  + innehållsnycklar i v2.
- Tenant i `message.accepted` kommer alltid från betrodd enhetsmetadata
  (core berikar, payload styr aldrig); tom tenant avvisas permanent.
- Mättid per record (`bt+t`), aldrig kuverttid; upprepade observationer
  och kanaler bevaras i ordning (testat i `compat_test.go`).

## 2. Go-koppling: ingen för things-kontrakt

`grep` över agent/core/device-management/events/things/transform-fiware/web:
inga Go-importer av `iot-things`-paket (`iot-things/pkg/types` importeras
endast inom `iot-things` självt) och inga importer av `iot-things-v2/pkg/types`
utanför v2. All things-koppling är tråd (topics/HTTP), inte moduler. v2 inför
ingen ny Go-koppling (fristående modul, inga `replace`).

Avgränsning: agent→core delar Go-typer (`iot-agent` importerar
`iot-core/pkg/messaging/events` för `MessageReceived`-kommandot) — det är
alltså inte sant att all koppling i systemet är tråd; påståendet ovan gäller
enbart things-kontrakten.

## 3. Versionsgränser

- `thing.updated` (wire + HTTP `/api/v0` i v1) ändras aldrig av v2.
  fiware-konsumenten låser kuvertformen i eget kontraktstest.
- `message.accepted` konsumeras endast läsande av v2 (ingen återpublicering).
- v2-kontrakt (`things.v1.*`, HTTP `/api/v1`) är additiva och isolerade.
- Konsumentmappning `thing.updated` -> `things.v1.*` är testad i
  `pkg/types/contract_compat_test.go` (fälttabell + golden JSON):
  id<-thingId, type<-thingType (mall-ID, tillagt Etapp 7), thing.*<-values,
  tenant<-tenant, timestamp<-values[].observedAt.
  Kända luckor (medvetet, K1-arbete före konsumentomställning): v2 saknar
  presentations-/identitetsmetadata som fiware-konsumenten läser ur
  `thing.*` (namn/alternativnamn, `subType`, beskrivning, `location`/area,
  taggar) — `values[]` bär endast typade egenskapsobservationer
  (propertyId/värde/enhet/kvalitet/källa/tider). `timestamp`-mappningen är
  per värde (`values[].observedAt`); vid flera värden i ett event måste
  konsumenten välja (rekommenderat: senaste `observedAt`, samma regel som B3).
  Full proveniens (antal tillgängliga/förväntade per ingång) stannar i
  historik/outbox-lagren liksom källdetaljer bortom kommaseparerad `source`.

## 4. Införande som separat tjänst

- Egen modul, egen image (`deployments/Dockerfile`), egen databas
  (separat schema/instans; delar aldrig tabeller med v1).
- Samma broker; egen transient broadcast-prenumeration.
- Ingen dataimport vid införande: historik byggs från liveflödet;
  v1-databasen behålls orörd. Historisk migrering/avveckling av v1
  kräver separat beslut (utanför denna leverans).
- Beständig katalog-seed (idempotent ompublicering efter omstart) och
  PostgreSQL-backend (K4) är kvarvarande punkter före produktion.

## 5. Driftsättning (förslag)

1. Deploya v2 mot isolerad broker + tom DB (drill nedan). Katalog
   publiceras via `/api/v1/catalog/*`, saker skapas via `/api/v1/things`.
2. Anslut mot delad broker (read-only på `message.accepted`) bredvid v1.
   Verifiera att v1 flödar oförändrat + att v2 beräknar samma värden
   (jämför stickprov v1-`thing.updated` mot v2-översikter).
3. Koppla konsumenter till `things.v1.*` (fiware/events-anpassning är
   separat leverans med egna kontraktstester).
4. Cutover-kriterier: kontraktstester gröna, drill grön, konsumentstöd
   driftsatt, observationsperiod utan avvikelser.

## 6. Återgång

- Stoppa v2. v1 har delat varken tillstånd, topics eller tabeller och
  påverkas inte (ingen återställning behövs).
- Väntande v2-outbox förkastas med tjänsten (konsumenter hade per
  definition inte ställt om; pre-cutover är v1-händelser
  sanningsbärande).
- Post-cutover-återgång = ställ tillbaka konsumenter på `thing.updated`
  (v1 har flödat parallellt; ingen dataförlust).

## 7. Isolerad drill (`deployments/docker-compose.yaml`)

```sh
docker compose -f deployments/docker-compose.yaml config  # validera
docker compose -f deployments/docker-compose.yaml up -d
# 1. Katalog: POST /api/v1/catalog/templates + /variants (se README-exempel)
# 2. Sak: POST /api/v1/things (If-Match-flöde per README)
# 3. Publicera fixture-pack MED tenant till message.accepted
#    (exempel i internal/application/handlers_test.go; tenant som
#    device-post `dev/tenant`, MessageID via transporthuvud, ej payload)
# 4. Kontrollera things.v1.values i broker-UI + GET /api/v1/things/{id}
# 5. Starta om tjänsten: väntande batchar återpubliceras, ingen
#    dubbelhistorik (samma påstående som runtime-testerna bevisar)
docker compose -f deployments/docker-compose.yaml down
```

Tjänsten kräver ingen nåbar PostgreSQL i drillen (`storage.New`
validerar endast anslutningssträngen; beständig backend är K4) och kan
köras brokerless med `RABBITMQ_DISABLED=true` för rent API-test.
