# iot-things-v2

Ny version av iot-things enligt `../THNGS_NEW.md`. Separat tjänst/installation med **samma struktur och mönster som iot-things**: `cmd/<tjänst>`, `internal/{application,infrastructure,presentation}`, `pkg` för publika kontrakt, `assets`, `deployments`. Samma beroenden och versioner (`service-chassis`, `messaging-golang`, `senml`, `pgx`, `opa`, `otel`).

Status: Etapp 1–3 scaffold + domänkärna, operatorer, flödesregler, minneslager, HTTP-scaffold och fixtures. Se `STATUS.md` för checklista mot `THNGS_NEW.md` kap 13.

## Struktur (spegel av iot-things)

- `cmd/iot-things-v2`: `main.go` + `config.go` (flags/env/CLI-precedens, `servicerunner`, control+public-servrar, `ownedResources`-shutdown, `storageConfigFromFlags`, seed, `MigrateBindings`-gating). Service-namn `iot-things-v2`.
- `internal/application`: `app.go` (`ThingsApp`-fasad, `ThingsReader`/`ThingsWriter`), `handlers.go` (`message.accepted`-handler med senml/diwise-`Parse`, tenant-`Permanent`, `convPack`→`Observation`), `evaluate.go` (`ShouldBecomeCurrent`, `EvaluateBatch`).
- `internal/application/things`: Thing/Template/Variant/Property/Signal/Binding/Relation, sluten mall, stabila ID:n.
- `internal/application/functions`: mean/min/max, level, fillRate, linear, percent, hysteres, aktiv tid.
- `internal/infrastructure/storage`: `config.go` (`NewConfig`/`ConnStr`, enda väg via cmd-flags), `storage.go` (`New` validerar via `pgxpool.ParseConfig`, minnesbackend; Timescale-DDL återstår K4), `memory.go` (things/history/relations/outbox/dedup, tenant-isolering, cykelskydd).
- `internal/presentation/api`: `api.go` (v1-minnes-API: ETag `rev-n`, `If-Match`/`409`, history/children), `register.go` (`RegisterHandlers` med `router` + OPA-`auth`, scopes `things.create/read/update/delete`, `/api/v0`-prefix likt iot-things), `auth/` (kopierad OPA-middleware från iot-things).
- `pkg/types`: `things.v1.values`, `things.v1.relations` + `Validate()` (gamla `thing.updated` orörda).
- `assets/data/fixtures`: bins, rooms+building, greenhouse, sewer. `assets/config`: `authz.rego`, `config.yaml`, `things.csv`. `assets/docs`: `openapi.yaml` (v1-spec återstår).
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

## Verifiering

```sh
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
```

Källkod och `go.mod` är facit. Ingen `replace` mot syskonmappar.
