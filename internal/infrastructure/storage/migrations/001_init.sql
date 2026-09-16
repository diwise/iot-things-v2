-- K4: beständigt schema för iot-things-v2.
--
-- Designbeslut (dokumenterade i koden, inte bara här):
--   * Idempotensnycklar matchar minnesbackendens semantik exakt, så
--     protokolltester gäller båda backendarna.
--   * Historik-dedup inkluderar INTE message_id (retry med nytt ID men
--     samma innehåll dedupliceras), och NULL-värden skiljs från NaN via
--     (value IS NULL) i uniknyckeln (minnet: "<nil>" vs "NaN"). Late
--     persisteras som kolumn men ingår ej i dedupnyckeln (samma innehåll,
--     olika Late-flagga = dubblett).
--   * Outbox har två tabeller: pending (väntande) + seen (sedda ID:n som
--     behålls efter leverans). Retention för seen är ett K4-uppföljnings-
--     arbete, liksom för minnesbackendens seen-mängder.
--   * Cykelskydd för partOf ligger i applikationslagret (samma regler som
--     minnet); databasen garanterar entydighet (en partOf-förälder).
--   * Timescale-hypertabell skapas endast om tillägget finns (drill/CI kör
--     vanliga PostgreSQL; beteendet är identiskt, endast prestanda skiljer).

CREATE TABLE IF NOT EXISTS things_v2(
  tenant    TEXT NOT NULL,
  id        TEXT NOT NULL,
  revision  BIGINT NOT NULL,
  data      JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant, id)
);

CREATE TABLE IF NOT EXISTS history_v2(
  tenant      TEXT NOT NULL,
  thing_id    TEXT NOT NULL,
  property_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  value       DOUBLE PRECISION,
  quality     TEXT NOT NULL DEFAULT '',
  source      TEXT NOT NULL DEFAULT '',
  message_id  TEXT NOT NULL DEFAULT '',
  late        BOOLEAN NOT NULL DEFAULT FALSE
);
-- Innehålls-dedup: samma nyckel som minnet (tenant|sak|egenskap|mättid|
-- källa|kvalitet|värde). Mättid normaliseras till mikrosekundprecision i
-- Go-koden (Round) före skrivning i båda backendarna; TIMESTAMPTZ avrundar
-- ändå sub-mikrosekund vid lagring så nycklarna blir identiska.
-- NULL (saknat värde) skiljs från NaN via (value IS NULL): minnet mappar
-- nil->"<nil>" och NaN->"NaN" som olika nycklar; COALESCE ensamt skulle
-- conflata dem (båda NaN). -0 conflatas med 0 i båda backendarna (minnet
-- normaliserar "-0"->"0"; float8 '=' ser -0 == 0 i UNIQUE-index).
CREATE UNIQUE INDEX IF NOT EXISTS history_v2_dedup ON history_v2(
  tenant, thing_id, property_id, observed_at, source, quality,
  ((value IS NULL)),
  (COALESCE(value, 'NaN'::float8))
);
CREATE INDEX IF NOT EXISTS history_v2_lookup ON history_v2(tenant, thing_id, property_id, observed_at);

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
    BEGIN
      PERFORM create_hypertable('history_v2', 'observed_at', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
      -- Hypertable är prestandaoptimering, ej korrekthet: misslyckande
      -- (t.ex. rättigheter) får inte fälla migrationen. Beteendet är
      -- identiskt på vanliga tabeller, endast prestanda skiljer.
      RAISE NOTICE 'create_hypertable failed (non-fatal): %', SQLERRM;
    END;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS relations_v2(
  tenant  TEXT NOT NULL,
  id      TEXT NOT NULL,
  type    TEXT NOT NULL,
  from_id TEXT NOT NULL,
  to_id   TEXT NOT NULL,
  PRIMARY KEY(tenant, id)
);
-- Högst en partOf-förälder per barn (B17).
CREATE UNIQUE INDEX IF NOT EXISTS relations_v2_single_parent
  ON relations_v2(tenant, from_id) WHERE type = 'partOf';
CREATE INDEX IF NOT EXISTS relations_v2_children ON relations_v2(tenant, to_id);

CREATE TABLE IF NOT EXISTS outbox_v2(
  id          TEXT PRIMARY KEY,
  tenant      TEXT NOT NULL,
  thing_id    TEXT NOT NULL,
  property_id TEXT NOT NULL DEFAULT '',
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revision    BIGINT NOT NULL DEFAULT 0,
  payload     JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS outbox_v2_pending ON outbox_v2(tenant, thing_id);
CREATE TABLE IF NOT EXISTS outbox_seen_v2(
  id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dedup_v2(
  tenant     TEXT NOT NULL,
  message_id TEXT NOT NULL,
  thing_id   TEXT NOT NULL,
  PRIMARY KEY(tenant, message_id, thing_id)
);
