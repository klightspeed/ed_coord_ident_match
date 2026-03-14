CREATE TABLE IF NOT EXISTS wikidata_idents (
    source TEXT NOT NULL,
    item_id TEXT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_wikidata_idents_item_id ON wikidata_idents (item_id);
CREATE INDEX IF NOT EXISTS IX_wikidata_idents_source ON wikidata_idents (source);
