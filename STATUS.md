# STATUS mot THNGS_NEW.md kap 13

`[x]` = infört + testat i denna repo. `[ ]` = återstår (se TODO i kod).

## Tillämpade beslut efter 2026-09-16

- Obligatoriska sakfält: `name`, `location` och `category` (egen eller
  ärvd från mallen) valideras i Compile. Metadata, bindningar och
  relationer är frivilliga: sensorlösa överobjekt och fristående
  objekt är giltiga enligt THNGS_NEW kap 5.
- GUI-stöd i katalogen: mallar har visningsnamn/beskrivning,
  egenskapsdefinitioner (namn, enhet, kompatibla signaler; prefillas vid
  skapande) och parameterinfo (enhet, intervall valideras vid publicering,
  variant och override); mallista filtreras på `?category=`.
- Huvuddata: `ThingSummary.primary` löser mallens `primaryProperty` mot
  aktuella värden (nil utan primaryProperty/aktiverad egenskap).
- `classification` heter `category` i kod/API (v2 ej i produktion;
  svenska dokument behåller "klassificering").
- Fler-tenant-användare: läsningar fanar ut serverside över token-auktoriserade
  tenants (?tenant= filtrerar); enskild sak kräver entydig träff (0 -> 404,
  flera -> 400); skrivningar kräver exakt en tenant.
- K1 (2026-09-22): exakt v1-schema + quality-tabell låst. Deskriptorn bär
  additivt `alternativeName`/`subType` (NameOrID = alternativeName > name >
  thingId; TypeName = subType > thingType; SafeURISegment/UUID i pkg/types).
  Konsumenttid = `EventTimestamp()` (max observedAt). Field-vägar
  `values.<property>.{value,observedAt,quality,unit,source}` ersätter gamla
  field:-vägar. Producenttabell per operator i functions.go + flödestester
  för min/max- och falling/both-flödestester; död kod (Percent/FlagOutOfRange)
  borttagen. Konsumentregler: `ShouldEmitProperty/ShouldEmitEntity`
  (invalid hoppar property, required saknad/invalid hoppar entitet); trace
  endast headers/OTel. Exempel: `docs/CONSUMER-BINDING.md` (wastebin, room,
  greenhouse). `match.device` för saker är död flagga.
  Arkitekturgräns: tjänsten känner inte till sina konsumenter — inga
  konsumentnamn i kod, tester eller docs.

## B – Beslut

- [x] B1 Sluten mall (inkl. bart-prefix-fix)
- [x] B2 Stabilt ID vs visningsnamn
- [x] B3 Aktuellt = senaste mättid, ingen TTL
- [x] B4 Enkelt medel, alla krävs
- [x] B5 Endast inom sak (graf-check saknas delvis, se K1)
- [x] B6 Sak-historik i things, sensor i events (minne + PostgreSQL via PgStore; retention återstår)
- [x] B7 Endast framåt (ingen omräkning)
- [x] B8 Ej manuellt (skrivskydd i API: mätvärdesfält — values/value/observedAt/receivedAt/quality — i sakskrivningar (create/update/preview) nekas med 400 i stället för att tyst ignoreras; värden anländer endast via message.accepted; configparametrar fortsatt tillåtna)
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
- [x] S1-S4, R1-R6 (fixtures + recept; geometri låst i K2: vertical-down, vinkel/extra parametrar nekas)
- [x] H1-H10 (minneslager + API-filter; DB-constraint återstår)
- [x] C1-C10 (operatorer + regler; quality-tabell per operator låst i K1: functions.go-tabell + min/max- och falling/both-flödestester)
- [x] L1-L7 (regler + minnes-outbox + PostgreSQL-PgStore med samma protokolltester; dedupmarkering atomär i commit, omtag spelar upp vid samtidig slutförd batch; DLX/ack-verifiering i messaging-lib återstår), L8-L10 delvis
- [x] P1-P9: katalog publish/list/get enstaka (immutable, ingen update/delete), sak-CRUD, sökning med filter (mall/version/variant/namn/category/metadata/bbox) + limit/offset + X-Total-Count, historik med egenskap + from/to-intervall + limit/offset, underträd med djupgräns, detach, OpenAPI för /api/v1
- [x] P10-P13 delvis (kontrakt + health + thing/history/children/tree; livscykel created/updated/deleted + partOf set/removed publiceras beständigt med inline-drain; värdesevent bär sakdeskriptor; konsumentkartläggning återstår E7)
- [x] G1-G6, G8-G9 (kärna testbar utan broker/DB); G7 delvis (slog utan hemligheter, metrics/tracing återstår)
- [x] E1-E6 (E6-resten: beständig katalog, plats/metadata/sök, träd/detach/total, OpenAPI); E7 steg 1–2 verifierade i drill 2026-09-21 (isolering, samexistens, paritet), steg 3 konsumentstöd implementerat+live-verifierat (hos konsumenten, flag-gatat), steg 4 kriterier dokumenterade i INTEGRATION.md §8 (observationsperiod återstår som driftaktivitet; relations/lifecycle-konsumtion ingår ej)
- [x] A1/A3-A7 (struktur); A2/A8-A29 delvis via enhetstester (full E2E återstår)
- [ ] K1-K6 kvarstår delvis: K1 klart (se ovan); K2 klart (vertical-down låst, angle/extra parametrar nekas vid publicering/kompilering/statisk validering); K3 klart (sendetalj per tillståndsoperator låst i flödestester); K5 klart (tak 64/32/64 per mall, 64/16 per sak; latens-SLO kräver lasttest); K4-backend + beständig katalog + startkatalog klara (retention + övervakning återstår); övriga se kod-TODO + THNGS_NEW.md kap 12

Review 2026-09-16: 10 fynd fixade (bare-prefix, device-krav, ok/uncertain-strikthet, dedup-tenant, outbox-idempotens, historik-kvalitet, cykelskydd, relationsvalidering, kontrakt-tid/version, API-tester). `gofmt`/`vet`/`test` grönt.
