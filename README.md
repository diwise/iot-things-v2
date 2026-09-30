# iot-things-v2

Ny version av iot-things enligt `../THNGS_NEW.md`. Separat tjänst/installation med **samma struktur och mönster som iot-things**: `cmd/<tjänst>`, `internal/{application,infrastructure,presentation}`, `pkg` för publika kontrakt, `assets`, `deployments`. Samma beroenden och versioner (`service-chassis`, `messaging-golang`, `senml`, `pgx`, `opa`, `otel`).

Status: Etapp 1–6 + K4-persistens (minne + PostgreSQL via PgStore, dedup/outbox/CAS, omstart/dränering, beständig katalog), domänkärna, operatorer, flödesregler, hanterings-API (katalog/sak-CRUD, bindningsbyte, uppgradering, relationer, underträd, historik med intervall+paging, sökning på mall/metadata/plats) och fixtures. Se `STATUS.md` för checklista mot `THNGS_NEW.md` kap 13.

## Struktur (spegel av iot-things)

- `cmd/iot-things-v2`: `main.go` + `config.go` (flags/env/CLI-precedens, `servicerunner`, control+public-servrar, `ownedResources`-shutdown, `storageConfigFromFlags`, seed, `MigrateBindings`-gating). Service-namn `iot-things-v2`. Vid start: katalogseed (`CATALOG_SEED_FILE`) före sakimport (`THINGS_FILE`, v1-things.csv, idempotent).
- `internal/application`: `app.go` (`ThingsApp`-fasad, `ThingsReader`/`ThingsWriter`), `handlers.go` (`message.accepted`-handler med senml/diwise-`Parse`, tenant-`Permanent`, `convPack`→`Observation`), `evaluate.go` (`ShouldBecomeCurrent`, `EvaluateBatch`).
- `internal/application/things`: Thing/Template/Variant/Property/Signal/Binding/Relation, sluten mall, stabila ID:n.
- `internal/application/functions`: mean/min/max, level, fillRate, linear, percent, hysteres, aktiv tid.
- `internal/infrastructure/storage`: `config.go` (`NewConfig`/`ConnStr`, enda väg via cmd-flags), `storage.go` (minnesbackend), `postgres.go` (`PgStore` över pgx + `migrations/`; Timescale-hypertabell när tillägget finns), `memory.go` (things/history/relations/outbox/dedup, tenant-isolering, cykelskydd). Backend väljs i `cmd`: tom `POSTGRES_HOST` ger minne, satt värd ger PostgreSQL.
- `internal/presentation/api`: `register.go` (`RegisterHandlers` med `router` + OPA-`auth`, scopes `things.create/read/update/delete`, `/api/v1`-prefix), `respond.go` (JSON/ETag-hjälpare), `auth/` (kopierad OPA-middleware från iot-things).
- `pkg/types`: `things.v1.values`, `things.v1.relations` + `Validate()` (gamla `thing.updated` orörda).
- `assets/data/fixtures`: bins, rooms+building, greenhouse, sewer. `assets/config`: `authz.rego`, `things.csv`, `catalog-seed.json` (startkatalog: en mall per iot-things-typ + greenhouse/tank-mallar + wastebin-varianter, laddas idempotent vid start via `CATALOG_SEED_FILE`). `assets/docs`: `openapi.yaml` (beskriver `/api/v1`).
- `deployments/Dockerfile`: samma multi-stage-bygge som iot-things (binary `iot-things-v2`).

## Auktorisering

Access-object är standard: runtime-policyn ska returnera ett `access`-objekt
med scopes per tenant, exempelvis `{"access":{"default":["things.read"]}}`.
Legacy-policyer som bara returnerar `tenants` kräver
`AUTHZ_ACCESS_OBJECT_ENABLED=false` eller `-authz-access-object=false`.
API-rutternas scopekrav och tenant-isolering gäller även när navigationen i
webben visar länkar utifrån det fullständiga access-objektet.

## Verifiering

```sh
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
docker build -f deployments/Dockerfile .
```

Källkod och `go.mod` är facit. Ingen `replace` mot syskonmappar.

## Retention och övervakning (K4)

- `HISTORY_RETENTION_DAYS` (default `0` = obegränsat, B6): `>0` raderar
  historik äldre än så många dygn vid start och därefter dygnsvis
  (samma i minne och PostgreSQL). Väntande outbox, dedup/sedda ID:n,
  blobbar, katalog och livscykel-/relationshändelser påverkas inte.
- Varje körning loggar antal + gräns (`history retention purge`).
  Radantal per tabell för drift: `SELECT count(*),
  min(observed_at), max(observed_at) FROM history_v2;` (index
  `history_v2_observed_at`, migration 006; Timescale-hypertabell med
  chunk-exkludering när tillägget finns).
- Explicit GDPR-radering per sak: `DELETE /things/{id}/history`.

## Tracing (OTEL via exporter/collector)

Allt skickas via OTLP-exporter till otel-collector — ingen `/metrics`.
Sätt `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` (se
`docker-compose.o11y.yaml` i compose-repot; tjänsten är ännu inte
tillagd där). Utan endpoint är SDK:t noop.

Spankedja mätvärde → beräkning → värdesevent (propageras via
broker-headers av messaging-biblioteket):

- `receive-measurements` (broker-ingress) → `handle-batch`
  (tenant/message/antal) → `evaluate-thing` per sak (sak-ID,
  events/historik-antal) → `drain-publish` (publicering av
  `things.v1.values`, injicerar samma context) + `drain-raw`
  (livscykel/relationer). `drain-all` vid omstart, `retention-purge`
  vid K4-körning.
- HTTP-ingress: otelhttp server-spans + `check-auth`-span; fel
  markeras med Error-status i hela kedjan.
