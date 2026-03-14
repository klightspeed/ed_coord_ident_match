CREATE TABLE IF NOT EXISTS simbad_ident (
    oidref BIGINT NOT NULL,
    id VARCHAR(255) NOT NULL,
    update_date DATE NOT NULL,
    match_name VARCHAR(255) NOT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref ON simbad_ident (oidref);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_id ON simbad_ident (id);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_match_name ON simbad_ident (match_name, id);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_update_date ON simbad_ident (update_date);
CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref_update_date ON simbad_ident (oidref, update_date);
CREATE UNIQUE INDEX IF NOT EXISTS UQ_simbad_ident_oidref_id ON simbad_ident (oidref, id);
