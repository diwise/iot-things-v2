# Systemintegration iot-things-v2 (Etapp 7)

## 1. Producent/konsument-karta

| Flöde | Typ | Kontrakt | Version |
| --- | --- | --- | --- |
| `iot-agent` -> `iot-core` | Kommando `MessageReceived` (`{pack, timestamp}`) | `iot-core/pkg/messaging/events` | oförändrat |
| `iot-core` -> `message.accepted` | Topic `{pack, timestamp}` + content-type per pack | `iot-core/pkg/messaging/{topics,events}` | oförändrat |
| `message.accepted` -> `iot-events` | broadcast-prenumeration | egen handler | oberörd av v2 |
| `message.accepted` -> `iot-things` (v1) | broadcast-prenumeration | oförändrat | oberörd av v2 |
| `message.accepted` -> `iot-things-v2` | broadcast-prenumeration (`RegisterTopicMessageHandler`) | samma kontrakt, endast läsning | nytt |
| `message.accepted` -> extern transformkonsument | broadcast-prenumeration | oförändrat | oberörd av v2 |
| `iot-things-v2` -> `things.v1.values` | Topic, schema `v1` | `iot-things-v2/pkg/types` | nytt, inga konsumenter ännu |
| `iot-things-v2` -> `things.v1.relations` | Topic, schema `v1` | `iot-things-v2/pkg/types` | nytt, inga konsumenter ännu |
| `iot-things` (v1) -> `thing.updated` | Topic, wire-format låst | `iot-things/pkg/types` + externt kuvertlås | **orört (B20)** |
| `thing.updated` -> extern transformkonsument | broadcast-prenumeration (vid sidan av `message.accepted`) | externt kuvert (`id/type/thing/tenant/timestamp`) | oberörd av v2 |
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

`grep` över övriga tjänster i labs-miljön:
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
  Konsumenten låser kuvertformen i eget kontraktstest.
- `message.accepted` konsumeras endast läsande av v2 (ingen återpublicering).
- v2-kontrakt (`things.v1.*`, HTTP `/api/v1`) är additiva och isolerade.
- Konsumentmappning `thing.updated` -> `things.v1.*` är testad i
  `pkg/types/contract_compat_test.go` (fälttabell + golden JSON):
  id<-thingId, type<-thingType (mall-ID, tillagt Etapp 7), thing.*<-values,
  tenant<-tenant, timestamp<-values[].observedAt.
  Sakdeskriptorn (`thing` i värdesevent: namn, alternativnamn (K1),
  subTyp (K1), kategori, plats, metadata, variant) stänger det mesta av
  presentationsgapet utan API-join:
  kvarvarande luckor mot `thing.*` är beskrivning (v2 bär sådant i
  metadata i stället) och taggar.
  `timestamp`-mappningen är kontrakt sedan K1: `EventTimestamp()` =
  `max(values[].observedAt)` (samma regel som B3).
  Full proveniens (antal tillgängliga/förväntade per ingång) stannar i
  historik/outbox-lagren liksom källdetaljer bortom kommaseparerad `source`.
  Livscykel (`things.v1.lifecycle`: created/updated/deleted med snapshot)
  och relationer (`things.v1.relations`: partOf set/removed) ger
  registerhållning inklusive delete-propagering; enhetsrelationer
  (`hasDevice`) härleds ur värdenas kommaseparerade källströmmar.

## 4. Införande som separat tjänst

- Egen modul, egen image (`deployments/Dockerfile`), egen databas
  (separat schema/instans; delar aldrig tabeller med v1).
- Samma broker; egen transient broadcast-prenumeration.
- Ingen dataimport vid införande: historik byggs från liveflödet;
  v1-databasen behålls orörd. Undantag: `things.csv` (v1-seedformat)
  kan importeras idempotent vid start (`THINGS_FILE`) för att skapa
  saker från seed-mallarna med korrekta bindningar (maxd/maxl ->
  parametrar, plats/metadata bevaras); rader som inte kan uttryckas
  (t.ex. vinkel) nekas per rad med loggning. Historisk migrering/
  avveckling av v1 kräver separat beslut (utanför denna leverans).
- Beständig backend är införd (K4): `PgStore` över PostgreSQL med
  Timescale-hypertabell för historik när tillägget finns (samma
  protokolltester gäller båda backendarna). Kvar: retention för
  historik/seen-mängder samt beständig katalog-seed (nytt format,
  separat arbete).

## 5. Driftsättning

Status 2026-09-21 (verifierat i drill, se §7 och E7-protokoll nedan):

1. [x] Isolerad drill grön: katalogseed (42 mallar), sakskapande via API,
   fixture-pack → ett `things.v1.values` per sak/rapport (medel 18/21/24 →
   21), omstart utan dubbelhistorik, återleverans idempotent.
2. [x] Samexistens bredvid v1 verifierad på delad broker: båda får samma
   `message.accepted` (transienta broadcast-köer, ingen stjäl), v1
   `thing.updated` oförändrat, värdeparitet 22 == 22 (v1-temperatur mot
   v2-medel). Verifierat på isolerad broker; mot delad driftbroker gäller
   samma mekanik — publicera aldrig testpack där (v1 skulle materialisera
   dem).
3. [ ] Koppla konsumenter till `things.v1.*`: separat leverans med egna
   kontraktstester, hos konsumenten. Konsumentstöd (prenumeration +
   regelhantering + golden-tester) är implementerat och live-verifierat
   mot stubbad mottagare; produktionsomställning är ett explicit
   driftbeslut per konsument (flag-styrt där så stöds).
4. [ ] Cutover-kriterier (se §8): kontraktstester gröna ✓, drill grön ✓,
   konsumentstöd driftsatt (per konsument), observationsperiod utan
   avvikelser (återstår som driftaktivitet).

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

Tjänsten kör PostgreSQL-backenden i drillen (migrationer appliceras
vid start; tom `POSTGRES_HOST` ger minnesbackend för dev/test) och kan
köras brokerless med `RABBITMQ_DISABLED=true` för rent API-test.

Drillfixar (krävs för att drillen ovan ska fungera, verifierat 2026-09-21):

- Brokern skapas med `RABBITMQ_DEFAULT_USER/PASS=user/bitnami` (composen).
  Default-imagen har annars bara `guest/guest` som inte får ansluta
  remote; tjänsten skulle aldrig koppla upp (403). Vid stale
  anonym volym från tidigare körning: `down -v` för helt ren start.
- API:t kräver Bearer-token via OPA även i drillen, och den bakade
  policyn accepterar endast live-IAM-token för `/api/v0`. Drillen
  montar därför `deployments/drill-authz.rego` (statisk `drill-token`,
  tenant `drill`) via `POLICIES_FILE`. Filen är drill-only och bakas
  aldrig in i imagen. Utan token svarar API:t 401 (verifierat).

## 8. Cutover-kriterier och E7-protokoll 2026-09-21

| Kriterium | Status | Bevis |
| --- | --- | --- |
| Kontraktstester gröna | ✓ | `go test ./...` grönt i v2 (8 paket) och i konsumentens modul (9 paket); golden för schema, tid, kvalitet, konsumentbindning |
| Drill grön (steg 1) | ✓ | 42 mallar seedade; sak via API (201, GUI-prefill); pack → 1 event/sak; medel 18/21/24→21; mättid/källa/revision låsta; omstart idempotent (0/42 seed-skip); återleverans 0 nya event, historik 1 rad |
| Samexistens + paritet (steg 2) | ✓ (mekanik) | v1+v2 på delad broker: båda fick samma pack; v1 `thing.updated` normalt (temperatur 22); v2-medel 22; separata transienta köer |
| Konsumentstöd (steg 3) | ✓ (kod, flag-gatat) | Prenumeration + adapter + `match.event`-isolering + 2 pilotregler + golden-tester; live mot stubbad mottagare: entitet med temperatur 23 @ mättid; gamla vägen parallellt opåverkad (separata entitets-ID:n) |
| Observationsperiod | Återstår | Driftaktivitet: parallellkörning mot delad broker, stickprov v1 mot v2 per saktyp över minst enették/cykel, avvikelser utreds före cutover |

Cutover-beslutet per konsument är separat: ställ om konsumenten till
`things.v1.*` först när dess kontraktstester + observationsperiod är
gröna. Återgång: tillbaka till `thing.updated` (v1 har flödat
parallellt; ingen dataförlust). `things.v1.relations`/`lifecycle`-
konsumtion ingår inte i denna leverans (endast `values`).
