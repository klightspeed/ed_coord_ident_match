CREATE TABLE IF NOT EXISTS sys_coords (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    sys_addr INTEGER NOT NULL,
    sys_name TEXT NOT NULL,
    X REAL NOT NULL,
    Y REAL NOT NULL,
    Z REAL NOT NULL,
    frame TEXT NOT NULL,
    sys_ra REAL NOT NULL,
    sys_dec REAL NOT NULL,
    sys_dist REAL NOT NULL,
    search_radius REAL NOT NULL,
    search_ra_range REAL NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coords_addr_frame ON sys_coords (sys_addr, frame);
CREATE INDEX IF NOT EXISTS IX_sys_coords_dec ON sys_coords (sys_dec);
