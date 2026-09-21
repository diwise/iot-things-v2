-- Index för K4-retention: radering av historik äldre än cutoff på
-- vanliga tabeller utan full skanning. På Timescale-hypertabell gäller
-- dessutom chunk-exkludering på observed_at; indexet är ofarligt där.
CREATE INDEX IF NOT EXISTS history_v2_observed_at ON history_v2(observed_at);
