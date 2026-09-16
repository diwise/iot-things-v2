# Fixtures – körbar spec för E1

Filerna i denna katalog är den körbara specifikationen för E1
(verksamhetsobjekt med mall, varianter, bindningar och relationer).

- `bins.json` – mall `wastebin/v1` + 4 varianter (A–D) + `bin-42`.
- `rooms.json` – mall `room/v1` + `room-a`, `room-b` + `building-a`.
- `greenhouse.json` – mallarna `greenhouse/v1` + `bench/v1` + `greenhouse-a` med `bench-1`/`bench-2`.
- `sewer.json` – mall `sewer/v1` (bräddningstid via `activeTime`) + `sewer-1`.

`internal/acceptance/acceptance_test.go` laddar dessa filer med
`os.ReadFile` + `encoding/json` (enbart stdlib) och verifierar
A1/A3/A4/A5/A6/A7 på strukturnivå. Ändra fixture och test tillsammans.
