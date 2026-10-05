-- PLAN1050 B2: fryst enhet per värde (CurrentValue.Unit, HistoryWrite.Unit,
-- FlowEvent.Unit, snapshot unit). Historik bevarar enhet utan ommärkning:
-- gamla värden märks aldrig om under oförenliga enheter/definitioner.
-- Tom sträng = legacy-rad utan fryst enhet (pre-PLAN1050) eller enhetslös
-- egenskap (t.ex. bool 1/0, räkning). Noll/false bevaras (NULL = saknat,
-- inte noll); NaN/Inf normaliseras till NULL+invalid före lagring.
--
-- Dedupnyckeln utökas med unit så att samma tid/källa/kvalitet/värde med
-- olika enhet inte dedupliceras (olika frysta enheter = olika observationer).
-- Befintliga rader får unit='' via DEFAULT och förblir dedupade som förr;
-- nya skrivningar sätter unit explicit från flödets frysta prop.Unit.
ALTER TABLE IF EXISTS history_v2 ADD COLUMN IF NOT EXISTS unit TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS history_v2_dedup;
CREATE UNIQUE INDEX IF NOT EXISTS history_v2_dedup ON history_v2(
  tenant, thing_id, property_id, observed_at, source, quality, unit,
  ((value IS NULL)),
  (COALESCE(value, 'NaN'::float8))
);
