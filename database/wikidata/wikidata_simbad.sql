CREATE TABLE IF NOT EXISTS wikidata_simbad (
    item_id TEXT NOT NULL,
    ident TEXT NOT NULL,
    identinfo TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_wikidata_simbad_item_id ON wikidata_simbad (item_id);
CREATE INDEX IF NOT EXISTS IX_wikidata_simbad_ident ON wikidata_simbad (ident);
CREATE UNIQUE INDEX IF NOT EXISTS IX_wikidata_simbad_item_id_ident ON wikidata_simbad (item_id, ident);
