CREATE TABLE IF NOT EXISTS simbad_basic (
    oid BIGINT NOT NULL PRIMARY KEY,
    main_id VARCHAR(255) NOT NULL,
    otype VARCHAR(255) NULL,
    ra DECIMAL(12, 8) NULL,
    `dec` DECIMAL(12, 8) NULL,
    plx_value DECIMAL(12, 8) NULL,
    update_date DATE NOT NULL,
    idcount INT NOT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE INDEX IF NOT EXISTS IX_simbad_basic_dec ON simbad_basic (`dec`);
CREATE INDEX IF NOT EXISTS IX_simbad_basic_update_date ON simbad_basic (update_date);
