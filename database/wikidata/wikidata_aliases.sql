CREATE TABLE IF NOT EXISTS wikidata_aliases (
    item_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    aliasinfo TEXT NOT NULL,
    match_name TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_item_id ON wikidata_aliases (item_id);
CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_alias ON wikidata_aliases (alias);
CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_match_name ON wikidata_aliases (match_name);
CREATE UNIQUE INDEX IF NOT EXISTS IX_wikidata_aliases_item_id_alias ON wikidata_aliases (item_id, alias);
