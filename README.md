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

## Verifiering

```sh
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
docker build -f deployments/Dockerfile .
```

Källkod och `go.mod` är facit. Ingen `replace` mot syskonmappar.
