-- K4-uppföljning: Late-kolumn + NULL-vs-NaN-dedup för befintliga databaser
-- som migrerats med 001_init.sql före fixen.
--
-- 001 innehåller redan late-kolumnen och den korrigerade uniknyckeln för nya
-- databaser; denna fil gör samma ändring idempotent för befintliga.

ALTER TABLE IF EXISTS history_v2 ADD COLUMN IF NOT EXISTS late BOOLEAN NOT NULL DEFAULT FALSE;

DROP INDEX IF EXISTS history_v2_dedup;
CREATE UNIQUE INDEX IF NOT EXISTS history_v2_dedup ON history_v2(
  tenant, thing_id, property_id, observed_at, source, quality,
  ((value IS NULL)),
  (COALESCE(value, 'NaN'::float8))
);
