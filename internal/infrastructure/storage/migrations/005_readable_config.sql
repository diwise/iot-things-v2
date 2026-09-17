-- Läsbar konfiguration: config låg base64-kodad i data-blobben (Go []byte).
-- Med json.RawMessage bäddas den in som läsbar JSON i stället. Befintliga
-- rader konverteras här; nya skrivningar lagrar redan läsbart.
-- config_revision speglar blobens revision som frågebar kolumn
-- (skrivstaketet revision förblir CAS-nyckeln).
ALTER TABLE IF EXISTS things_v2 ADD COLUMN IF NOT EXISTS config_revision BIGINT NOT NULL DEFAULT 1;

UPDATE things_v2
SET data = jsonb_set(data, '{config}',
    convert_from(decode(data->>'config', 'base64'), 'UTF8')::jsonb)
WHERE jsonb_typeof(data->'config') = 'string';

UPDATE things_v2
SET config_revision = (data->>'revision')::bigint
WHERE jsonb_typeof(data->'revision') = 'number';

CREATE INDEX IF NOT EXISTS things_v2_config_revision ON things_v2(tenant, config_revision);
