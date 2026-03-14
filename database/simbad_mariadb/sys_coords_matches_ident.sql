CREATE TABLE IF NOT EXISTS sys_coord_matches_ident (
    sys_name VARCHAR(255) NOT NULL,
    sys_addr BIGINT NOT NULL,
    frame VARCHAR(50) NOT NULL,
    sys_ra DECIMAL(12, 8) NOT NULL,
    sys_dec DECIMAL(12, 8) NOT NULL,
    sys_dist DECIMAL(12, 6) NOT NULL,
    simbad_oid BIGINT NOT NULL,
    simbad_main_id VARCHAR(255) NOT NULL,
    simbad_ident VARCHAR(255) NOT NULL,
    simbad_ra DECIMAL(12, 8) NOT NULL,
    simbad_dec DECIMAL(12, 8) NOT NULL,
    simbad_plx DECIMAL(12, 8) NULL,
    matched_name VARCHAR(255) NOT NULL,
    match_source VARCHAR(255) NULL,
    dist_plx DECIMAL(12, 6) NULL,
    dist_ly DECIMAL(12, 6) NOT NULL,
    dist_deg DECIMAL(12, 8) NOT NULL,
    dist_jw DECIMAL(12, 8) NOT NULL,
    dist_jw_punct DECIMAL(12, 8) NOT NULL,
    dist_indel DECIMAL(12, 8) NOT NULL,
    dist_indel_punct DECIMAL(12, 8) NOT NULL,
    dist_hamming DECIMAL(12, 8) NOT NULL,
    dist_hamming_punct DECIMAL(12, 8) NOT NULL,
    dist_lev DECIMAL(12, 8) NOT NULL,
    dist_lev_punct DECIMAL(12, 8) NOT NULL,
    dist_dlev DECIMAL(12, 8) NOT NULL,
    dist_dlev_punct DECIMAL(12, 8) NOT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

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
