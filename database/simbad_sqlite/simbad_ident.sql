CREATE TABLE IF NOT EXISTS simbad_ident (
    oidref INTEGER NOT NULL,
    id TEXT NOT NULL,
    update_date TEXT NOT NULL,
    match_name TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref ON simbad_ident (oidref);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_id ON simbad_ident (id);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_match_name ON simbad_ident (match_name, id);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_update_date ON simbad_ident (update_date);
CREATE UNIQUE INDEX IF NOT EXISTS UQ_simbad_ident_oidref_id ON simbad_ident (oidref, id);
