CREATE TABLE IF NOT EXISTS sys_coords (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    sys_addr BIGINT NOT NULL,
    sys_name VARCHAR(255) NOT NULL,
    X DECIMAL(12, 6) NOT NULL,
    Y DECIMAL(12, 6) NOT NULL,
    Z DECIMAL(12, 6) NOT NULL,
    frame VARCHAR(50) NOT NULL,
    sys_ra DECIMAL(12, 8) NOT NULL,
    sys_dec DECIMAL(12, 8) NOT NULL,
    sys_dist DECIMAL(12, 6) NOT NULL,
    search_radius DECIMAL(12, 8) NOT NULL,
    search_ra_range DECIMAL(12, 8) NOT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coords_addr_frame ON sys_coords (sys_addr, frame);
CREATE INDEX IF NOT EXISTS IX_sys_coords_dec ON sys_coords (sys_dec);
