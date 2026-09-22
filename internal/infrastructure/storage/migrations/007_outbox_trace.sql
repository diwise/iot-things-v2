-- TV-07: bevara transportens trace-context i outbox separat från
-- domänpayloaden så att omstart kan återställa/länka kedjan.
-- NULL/'' = ingen lagrad context (äldre rader, spårlösa händelser).
ALTER TABLE IF EXISTS outbox_v2 ADD COLUMN IF NOT EXISTS trace_context TEXT NOT NULL DEFAULT '';
