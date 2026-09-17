-- Katalogpersistens (E6): publicerade mall- och variantversioner överlever
-- omstart. Publicerade versioner är oföränderliga: PRIMARY KEY ger
-- konflikt vid återpublicering och skrivningar använder ON CONFLICT
-- DO NOTHING så att parallell publicering konvergerar.
CREATE TABLE IF NOT EXISTS templates_v2(
  id      TEXT NOT NULL,
  version TEXT NOT NULL,
  spec    JSONB NOT NULL,
  PRIMARY KEY(id, version)
);
CREATE TABLE IF NOT EXISTS variants_v2(
  id      TEXT NOT NULL,
  version TEXT NOT NULL,
  spec    JSONB NOT NULL,
  PRIMARY KEY(id, version)
);
