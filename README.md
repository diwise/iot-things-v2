# iot-things-v2

Ny version av iot-things enligt `../THNGS_NEW.md`. Separat tjänst/installation, samma arkitekturmönster (`cmd/<tjänst>`, `internal/{application→domain/engine/store/api}`, `pkg` för publika kontrakt).

Status: Etapp 1–3 scaffold + domänkärna, operatorer, flödesregler, minneslager, HTTP-scaffold och fixtures. Se `STATUS.md` för checklista mot `THNGS_NEW.md` kap 13.

## Struktur

- `cmd/iot-things-v2`: env-config (`LISTEN_ADDRESS`, `SERVICE_PORT`, `CONTROL_PORT`), public/control-servrar, graceful shutdown. TODO: `service-chassis`, `pgx`, `messaging-golang`, OPA.
- `internal/domain`: Thing/Template/Variant/Property/Signal/Binding/Relation, sluten mall, stabila ID:n.
- `internal/operators`: mean/min/max, level, fillRate, linear, percent, hysteres, aktiv tid.
- `internal/engine`: `ShouldBecomeCurrent`, `EvaluateBatch` (en omgång), sen-sparas.
- `internal/store`: minneslager (things/history/relations/outbox/dedup) med tenant-isolering, idempotens, partOf-cykelskydd.
- `internal/api`: `GET /healthz`, `GET/PUT /api/v1/things/{id}` (ETag `rev-n`, `If-Match`/`409`), history/children. Tenant via `X-Tenant` (OPA TODO).
- `pkg/contract`: `things.v1.values`, `things.v1.relations` + `Validate()`.
- `testdata/fixtures`: bins, rooms+building, greenhouse, sewer.
- `internal/acceptance`: strukturtester A1/A3-A7.

## Verifiering

```sh
gofmt -l .
go vet ./...
go test ./... -count=1
go build ./...
```

Källkod och `go.mod` är facit. Ingen `replace` mot syskonmappar.
