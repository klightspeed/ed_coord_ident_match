CREATE TABLE IF NOT EXISTS simbad_basic (
    oid INTEGER NOT NULL UNIQUE,
    main_id TEXT NOT NULL,
    otype TEXT NULL,
    ra REAL NULL,
    `dec` REAL NULL,
    plx_value REAL NULL,
    update_date TEXT NOT NULL,
    idcount INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS IX_simbad_basic_dec ON simbad_basic (`dec`);
CREATE INDEX IF NOT EXISTS IX_simbad_basic_update_date ON simbad_basic (update_date);
