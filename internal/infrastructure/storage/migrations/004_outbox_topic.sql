-- Råa outbox-rader för livscykel/relationer (andra topics än värden).
-- Befintliga rader får värdestopic som default; nya skrivningar sätter
-- topic explicit. Värdesdränering filtrerar på topic så att råa rader
-- aldrig tolkas som FlowEvent.
ALTER TABLE IF EXISTS outbox_v2 ADD COLUMN IF NOT EXISTS topic TEXT NOT NULL DEFAULT 'things.v1.values';
CREATE INDEX IF NOT EXISTS outbox_v2_topic ON outbox_v2(tenant, thing_id, topic);
