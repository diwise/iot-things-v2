# STATUS mot THNGS_NEW.md kap 13

`[x]` = infört + testat i denna repo. `[ ]` = återstår (se TODO i kod).

## Tillämpade beslut efter 2026-09-16

- Obligatoriska sakfält: `name`, `location` och `category` (egen eller
  ärvd från mallen) valideras i Compile. Metadata, bindningar och
  relationer är frivilliga: sensorlösa överobjekt och fristående
  objekt är giltiga enligt THNGS_NEW kap 5.
- `classification` heter `category` i kod/API (v2 ej i produktion;
  svenska dokument behåller "klassificering").
- Fler-tenant-användare väljer tenant explicit med `?tenant=` på samtliga
  endpoints; utan väljare ger tvetydighet 400, läsningar slås aldrig ihop.

## B – Beslut

- [x] B1 Sluten mall (inkl. bart-prefix-fix)
- [x] B2 Stabilt ID vs visningsnamn
- [x] B3 Aktuellt = senaste mättid, ingen TTL
- [x] B4 Enkelt medel, alla krävs
- [x] B5 Endast inom sak (graf-check saknas delvis, se K1)
- [x] B6 Sak-historik i things, sensor i events (minne + PostgreSQL via PgStore; retention återstår)
- [x] B7 Endast framåt (ingen omräkning)
- [x] B8 Ej manuellt (API saknar skrivskydd för mätvärde ännu, se P3)
- [x] B9 Tillstånd utan notis/styrning
- [x] B10 Gemensam katalog + tenant-isolering (OPA återstår, X-Tenant scaffold)
- [x] B11 `device/[channel]/object/resource`, wildcard
- [x] B12 Strikt kvalitet/enhet (enhetsmatch vid config delvis, se O2)
- [x] B13 Sen sparas + tie-break
- [x] B14 En omgång, ett resultat
- [x] B15 Endast vid observation
- [x] B16 Behåll+flagga out-of-range
- [x] B17 Strikt relation (inkl. cykelskydd-fix)
- [x] B18 Inaktiv bindning (statusfält i API återstår delvis)
- [x] B19 `If-Match`/`409`
- [x] B20 Nya topics v1 isolerat
- [x] B21 Separat tjänst (servicerunner/pgx/messaging/OPA-middleware wired; pg-backend vald via POSTGRES_HOST)
- [x] B22 Avlopp som exempel (fixture klart)

## F/D/T/S/R/H/C/L/P/G/E/A/K

- [x] F1-F7 (struktur, ej driftbevis)
- [x] D1-D2, T1-T10 (inkl. versionsdiff före aktivering via upgrade/preview)
- [x] S1-S4, R1-R6 (fixtures + recept; geometri-bredd återstår K2)
- [x] H1-H10 (minneslager + API-filter; DB-constraint återstår)
- [x] C1-C10 (operatorer + regler; quality-tabell per operator återstår K1)
- [x] L1-L7 (regler + minnes-outbox + PostgreSQL-PgStore med samma protokolltester; DLX/ack-verifiering i messaging-lib återstår), L8-L10 delvis
- [x] P1-P9: katalog publish/list/get enstaka (immutable, ingen update/delete), sak-CRUD, sökning med filter (mall/version/variant/namn/klassificering/metadata/bbox) + limit/offset + X-Total-Count, historik med egenskap + from/to-intervall + limit/offset, underträd med djupgräns, detach, OpenAPI för /api/v1
- [x] P10-P13 delvis (kontrakt + health + thing/history/children/tree; konsumentkartläggning återstår E7)
- [x] G1-G6, G8-G9 (kärna testbar utan broker/DB); G7 delvis (slog utan hemligheter, metrics/tracing återstår)
- [x] E1-E6 (E6-resten: beständig katalog, plats/metadata/sök, träd/detach/total, OpenAPI); E7 påbörjad (isolerad drill + kontraktstester + v1-CSV-import; konsumentomställning återstår)
- [x] A1/A3-A7 (struktur); A2/A8-A29 delvis via enhetstester (full E2E återstår)
- [ ] K1-K6 kvarstår delvis: K4-backend + beständig katalog + startkatalog klara (retention + övervakning återstår); övriga se kod-TODO + THNGS_NEW.md kap 12

Review 2026-09-16: 10 fynd fixade (bare-prefix, device-krav, ok/uncertain-strikthet, dedup-tenant, outbox-idempotens, historik-kvalitet, cykelskydd, relationsvalidering, kontrakt-tid/version, API-tester). `gofmt`/`vet`/`test` grönt.
