CREATE TABLE IF NOT EXISTS sys_coord_matches_ident (
    sys_name TEXT NOT NULL,
    sys_addr INTEGER NOT NULL,
    frame TEXT NOT NULL,
    sys_ra REAL NOT NULL,
    sys_dec REAL NOT NULL,
    sys_dist REAL NOT NULL,
    simbad_oid INTEGER NOT NULL,
    simbad_main_id TEXT NOT NULL,
    simbad_ident TEXT NOT NULL,
    simbad_ra REAL NOT NULL,
    simbad_dec REAL NOT NULL,
    simbad_plx REAL NULL,
    matched_name TEXT NOT NULL,
    match_source TEXT NULL,
    dist_plx REAL NULL,
    dist_ly REAL NOT NULL,
    dist_deg REAL NOT NULL,
    dist_jw REAL NOT NULL,
    dist_jw_punct REAL NOT NULL,
    dist_indel REAL NOT NULL,
    dist_indel_punct REAL NOT NULL,
    dist_hamming REAL NOT NULL,
    dist_hamming_punct REAL NOT NULL,
    dist_lev REAL NOT NULL,
    dist_lev_punct REAL NOT NULL,
    dist_dlev REAL NOT NULL,
    dist_dlev_punct REAL NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_name ON sys_coord_matches_ident (sys_name);
CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_addr ON sys_coord_matches_ident (sys_addr);
CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_oidref ON sys_coord_matches_ident (simbad_oid);
CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coord_matches_ident_match_ident ON sys_coord_matches_ident (
    sys_name,
    sys_addr,
    frame,
    simbad_oid,
    simbad_ident,
    matched_name
);