from .classes import *
from .interfaces import SystemQueryDatabase
from ..util import filter_match_name, calc_search_radius_ra_range
from collections.abc import Collection, Iterable
import MySQLdb
import json
import logging
import sys
from astropy.coordinates import angular_separation

logger = logging.getLogger(__name__)


class SystemQueryMariaDB(SystemQueryDatabase):
    conn_opts: dict
    conn: MySQLdb.Connection

    def __init__(self, **kwargs):
        self.conn_opts = {k: v for k, v in kwargs.items() if v is not None}
        self.conn = self.connect()

    def connect(self):
        return MySQLdb.connect(**self.conn_opts)

    def query_idents(self, names: set[str]) -> dict[str, set[SimbadEntry]]:
        with self.connect() as conn:
            cursor = conn.cursor()

            cursor.execute(
                '''
                    SELECT
                        basic.oid,
                        basic.main_id,
                        ident.id,
                        basic.ra,
                        basic.`dec`,
                        basic.plx_value
                    FROM JSON_TABLE(%s, '$[*]' COLUMNS(
                        match_name VARCHAR(255) COLLATE utf8mb4_uca1400_ai_ci PATH '$.name'
                    )) sys_names
                    JOIN simbad_ident ident ON ident.match_name = sys_names.match_name
                    JOIN simbad_basic basic ON basic.oid = ident.oidref
                    WHERE basic.ra IS NOT NULL
                    AND basic.`dec` IS NOT NULL
                ''',
                (json.dumps([{'name': filter_match_name(n)} for n in names]),)
            )

            idents = {}

            for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in cursor:
                name = filter_match_name(sb_ident)

                entry = SimbadEntry(
                    int(sb_oid),
                    str(sb_main_id),
                    str(sb_ident),
                    float(sb_ra) << u.deg,
                    float(sb_dec) << u.deg,
                    float(sb_plx) << u.mas if sb_plx is not None else None
                )

                idents.setdefault(name, set()).add(entry)

            return idents

    def get_simbad_idents(self) -> dict[str, set[SimbadEntry]]:
        with self.connect() as conn:
            cursor = conn.cursor()

            print('Getting Simbad names')

            cursor.execute('''
                SELECT oidref, main_id, id, ra, `dec`, plx_value
                FROM simbad_ident ident
                JOIN simbad_basic basic ON oid = oidref
                WHERE id NOT LIKE 'Gaia DR%'
                AND id NOT LIKE 'Gaia EDR%'
                AND id NOT LIKE 'TIC %'
                AND id NOT LIKE 'UCAC4 %'
                AND id NOT RLIKE '^\\[[A-Z]+201[4-9][a-z]*\\]'
                AND id NOT RLIKE '^\\[[A-Z]+202[0-9][a-z]*\\]'
                AND ra IS NOT NULL
                AND `dec` IS NOT NULL
            ''')

            rows = cursor.fetchall()

            print(f'{len(rows)} idents')

            idents = {}

            for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in rows:
                name = filter_match_name(sb_ident)

                entry = SimbadEntry(
                    int(sb_oid),
                    str(sb_main_id),
                    str(sb_ident),
                    float(sb_ra) << u.deg,
                    float(sb_dec) << u.deg,
                    float(sb_plx) << u.mas if sb_plx is not None else None
                )

                idents.setdefault(name, set()).add(entry)

            return idents

    def query_coords(self, matches: Collection[SimbadMatch]) -> Iterable[SimbadTableMatch|Iterable]:
        with self.connect() as conn:
            entries = []

            sys.stderr.write('[query_coords] ')
            sys.stderr.flush()

            for match in matches:
                (search_radius, search_ra_range) = calc_search_radius_ra_range(match.sys_dist, match.sys_dec)
                sys_ra = float(match.sys_ra / u.deg)
                sys_dec = float(match.sys_dec / u.deg)
                sys_dist = float(match.sys_dist / u.lightyear)

                cursor = conn.cursor()
                cursor.execute(
                    """
                        SELECT
                            basic.oid,
                            basic.main_id,
                            ident.id,
                            basic.ra,
                            basic.`dec`,
                            basic.plx_value
                        FROM simbad_basic basic
                        JOIN simbad_ident ident ON ident.oidref = basic.oid
                        WHERE basic.`dec` BETWEEN %(sys_dec)s - %(search_radius)s AND %(sys_dec)s + %(search_radius)s
                          AND (ABS(%(sys_dec)s) > 90 - %(search_radius)s
                           OR basic.`ra` BETWEEN %(sys_ra)s - %(search_ra_range)s AND %(sys_ra)s + %(search_ra_range)s
                           OR basic.`ra` BETWEEN %(sys_ra)s - 360 - %(search_ra_range)s AND %(sys_ra)s - 360 + %(search_ra_range)s
                           OR basic.`ra` BETWEEN %(sys_ra)s + 360 - %(search_ra_range)s AND %(sys_ra)s + 360 + %(search_ra_range)s)
                    """,
                    {
                        'sys_ra': sys_ra,
                        'sys_dec': sys_dec,
                        'search_radius': search_radius,
                        'search_ra_range': search_ra_range
                    }
                )

                for sb_oid, sb_main_id, sb_id, sb_ra, sb_dec, sb_plx in cursor.fetchall():
                    dist_deg = angular_separation(float(sys_ra), float(sys_dec), float(sb_ra), float(sb_dec)) << u.deg
                    if dist_deg.value < search_radius:
                        entries.append(SimbadTableMatch(
                            str(match.sys_name),
                            int(match.sys_addr),
                            str(match.frame),
                            float(sys_ra) * u.deg,
                            float(sys_dec) * u.deg,
                            float(sys_dist) * u.lyr,
                            int(sb_oid),
                            str(sb_main_id),
                            str(sb_id),
                            float(sb_ra) * u.deg,
                            float(sb_dec) * u.deg,
                            float(sb_plx) * u.mas if sb_plx is not None else None
                        ))

                sys.stderr.write('.')
                sys.stderr.flush()

            sys.stderr.write(f' {len(entries)}\n')

            return entries

    def create_tables(self):
        with self.connect() as conn:
            conn.cursor().execute(
                """
                    CREATE TABLE IF NOT EXISTS simbad_ident (
                        oidref BIGINT NOT NULL,
                        id VARCHAR(255) NOT NULL,
                        update_date DATE NOT NULL,
                        match_name VARCHAR(255) NOT NULL
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
                """
            )
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref ON simbad_ident (oidref)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_id ON simbad_ident (id)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_match_name ON simbad_ident (match_name, id)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_update_date ON simbad_ident (update_date)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref_update_date ON simbad_ident (oidref, update_date)')
            conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS UQ_simbad_ident_oidref_id ON simbad_ident (oidref, id)')

            conn.cursor().execute(
                """
                    CREATE TABLE IF NOT EXISTS simbad_basic (
                        oid BIGINT NOT NULL PRIMARY KEY,
                        main_id VARCHAR(255) NOT NULL,
                        otype VARCHAR(255) NULL,
                        ra DECIMAL(12, 8) NULL,
                        `dec` DECIMAL(12, 8) NULL,
                        plx_value DECIMAL(12, 8) NULL,
                        update_date DATE NOT NULL,
                        idcount INT NOT NULL
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
                """
            )
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_basic_dec ON simbad_basic (`dec`)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_basic_update_date ON simbad_basic (update_date)')

            conn.cursor().execute(
                """
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
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
                """
            )
            conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coords_addr_frame ON sys_coords (sys_addr, frame)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coords_dec ON sys_coords (sys_dec)')

            conn.cursor().execute(
                """
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
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
                """
            )
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_name ON sys_coord_matches_ident (sys_name)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_addr ON sys_coord_matches_ident (sys_addr)')
            conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_oidref ON sys_coord_matches_ident (simbad_oid)')
            conn.cursor().execute(
                '''
                    CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coord_matches_ident_match_ident ON sys_coord_matches_ident (
                        sys_name,
                        sys_addr,
                        frame,
                        simbad_oid,
                        simbad_ident,
                        matched_name
                    )
                '''
            )

    def get_basic_ident_diff(self) -> list[int]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                    SELECT
                        basic.oid
                    FROM simbad_basic basic
                    JOIN simbad_ident ident ON ident.oidref = basic.oid
                    WHERE basic.oid <= (SELECT MAX(oidref) FROM simbad_ident)
                    GROUP BY basic.oid, basic.idcount, basic.update_date
                    HAVING COUNT(*) > basic.idcount
                        OR MAX(ident.update_date) <> MIN(ident.update_date)
                        OR MAX(ident.update_date) <> basic.update_date
                '''
            )

            return [oid for oid, in cursor]

    def get_last_basic_oid_date(self) -> SimbadOidDate:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY oid DESC LIMIT 1')
            row = cursor.fetchone()

            if row is None:
                return SimbadOidDate(0, None, None, None)

            max_oid, last_date = row

            cursor = conn.cursor()
            cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY update_date DESC, oid DESC LIMIT 1')

            last_oid, max_date = cursor.fetchone()

            if last_oid == max_oid:
                return SimbadOidDate(0, last_date, max_date, max_oid)

            return SimbadOidDate(last_oid, last_date, max_date, max_oid)

    def get_last_ident_oidref_date(self) -> tuple[int, date|None, date|None, int|None]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY oidref DESC LIMIT 1')
            row = cursor.fetchone()

            if row is None:
                return SimbadOidDate(0, None, None, None)

            max_oid, last_date = row

            cursor = conn.cursor()
            cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY update_date DESC, oidref DESC LIMIT 1')

            last_oid, max_date = cursor.fetchone()

            if last_oid == max_oid:
                return SimbadOidDate(0, last_date, max_date, max_oid)

            return SimbadOidDate(last_oid, last_date, max_date, max_oid)

    def insert_basic(self, entries: Iterable[SimbadBasic|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT INTO simbad_basic (
                    oid,
                    main_id,
                    otype,
                    ra,
                    `dec`,
                    plx_value,
                    update_date,
                    idcount
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON DUPLICATE KEY UPDATE
                    main_id = VALUES(main_id),
                    otype = VALUES(otype),
                    ra = VALUES(ra),
                    `dec` = VALUES(`dec`),
                    plx_value = VALUES(plx_value),
                    update_date = VALUES(update_date),
                    idcount = VALUES(idcount)

            """,
            entries
        )

    def insert_idents(self, entries: Iterable[SimbadIdent|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT INTO simbad_ident (
                    oidref,
                    id,
                    update_date,
                    match_name
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON DUPLICATE KEY UPDATE
                    update_date = VALUES(update_date)
            """,
            entries
        )

    def insert_syscoords(self, coords: Iterable[SystemCoords|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT INTO sys_coords (
                    sys_name,
                    sys_addr,
                    x,
                    y,
                    z,
                    frame,
                    sys_ra,
                    sys_dec,
                    sys_dist,
                    search_radius,
                    search_ra_range
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON DUPLICATE KEY UPDATE
                    sys_name = VALUES(sys_name),
                    x = VALUES(x),
                    y = VALUES(y),
                    z = VALUES(z),
                    sys_ra = VALUES(sys_ra),
                    sys_dec = VALUES(sys_dec),
                    sys_dist = VALUES(sys_dist),
                    search_radius = VALUES(search_radius),
                    search_ra_range = VALUES(search_ra_range)
            """,
            coords
        )

    def commit(self):
        self.conn.commit()

    def query_all_matches(self) -> Iterable[SimbadTableMatch|Iterable]:
        with self.connect() as conn:
            cursor = conn.cursor(MySQLdb.cursors.SSCursor)
            cursor.execute(
                """
                    SELECT
                        sys_coords.sys_name,
                        sys_coords.sys_addr,
                        sys_coords.frame,
                        sys_coords.sys_ra,
                        sys_coords.sys_dec,
                        sys_coords.sys_dist,
                        sys_coords.search_radius,
                        basic.oid,
                        basic.main_id,
                        ident.id,
                        basic.ra,
                        basic.`dec`,
                        basic.plx_value
                    FROM sys_coords
                    JOIN simbad_basic basic ON basic.`dec` BETWEEN sys_dec - search_radius AND sys_dec + search_radius
                    JOIN simbad_ident ident ON ident.oidref = basic.oid
                    WHERE ABS(sys_dec) > 90 - search_radius
                    OR basic.`ra` BETWEEN sys_ra - search_ra_range AND sys_ra + search_ra_range
                    OR basic.`ra` BETWEEN sys_ra - 360 - search_ra_range AND sys_ra - 360 + search_ra_range
                    OR basic.`ra` BETWEEN sys_ra + 360 - search_ra_range AND sys_ra + 360 + search_ra_range
                """
            )

            for sys_name, sys_addr, frame, sys_ra, sys_dec, sys_dist, search_radius, sb_oid, sb_main_id, sb_id, sb_ra, sb_dec, sb_plx in cursor:
                dist_deg = angular_separation(float(sys_ra), float(sys_dec), float(sb_ra), float(sb_dec)) << u.deg
                if dist_deg.value < search_radius:
                    yield SimbadTableMatch(
                        str(sys_name),
                        int(sys_addr),
                        str(frame),
                        float(sys_ra) * u.deg,
                        float(sys_dec) * u.deg,
                        float(sys_dist) * u.lyr,
                        int(sb_oid),
                        str(sb_main_id),
                        str(sb_id),
                        float(sb_ra) * u.deg,
                        float(sb_dec) * u.deg,
                        float(sb_plx) * u.mas if sb_plx is not None else None
                    )

    def get_syscoords(self) -> Iterable[SystemCoords|Iterable]:
        with self.connect() as conn:
            cursor = conn.cursor(MySQLdb.cursors.SSCursor)
            cursor.execute(
                """
                    SELECT
                        sys_name,
                        sys_addr,
                        frame,
                        x,
                        y,
                        z,
                        sys_ra,
                        sys_dec,
                        sys_dist,
                        search_radius,
                        search_ra_range
                    FROM sys_coords
                    ORDER BY sys_coords.sys_name, sys_coords.sys_addr, sys_coords.frame
                """
            )

            for sys_name, sys_addr, frame, x, y, z, sys_ra, sys_dec, sys_dist, search_radius, search_ra_range in cursor:
                yield SystemCoords(
                    str(sys_name),
                    int(sys_addr),
                    str(frame),
                    float(x),
                    float(y),
                    float(z),
                    float(sys_ra),
                    float(sys_dec),
                    float(sys_dist),
                    float(search_radius),
                    float(search_ra_range)
                )

    def insert_matches(self, matchlist: Iterable[SimbadDBMatch|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT INTO sys_coord_matches_ident (
                    sys_name,
                    sys_addr,
                    frame,
                    sys_ra,
                    sys_dec,
                    sys_dist,
                    simbad_oid,
                    simbad_main_id,
                    simbad_ident,
                    simbad_ra,
                    simbad_dec,
                    simbad_plx,
                    matched_name,
                    match_source,
                    dist_plx,
                    dist_ly,
                    dist_deg,
                    dist_jw,
                    dist_jw_punct,
                    dist_indel,
                    dist_indel_punct,
                    dist_hamming,
                    dist_hamming_punct,
                    dist_lev,
                    dist_lev_punct,
                    dist_dlev,
                    dist_dlev_punct
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON DUPLICATE KEY UPDATE
                    sys_ra = VALUES(sys_ra),
                    sys_dec = VALUES(sys_dec),
                    sys_dist = VALUES(sys_dist),
                    simbad_main_id = VALUES(simbad_main_id),
                    simbad_ra = VALUES(simbad_ra),
                    simbad_dec = VALUES(simbad_dec),
                    simbad_plx = VALUES(simbad_plx),
                    dist_plx = VALUES(dist_plx),
                    dist_ly = VALUES(dist_ly),
                    dist_deg = VALUES(dist_deg),
                    dist_jw = VALUES(dist_jw),
                    dist_jw_punct = VALUES(dist_jw_punct),
                    dist_indel = VALUES(dist_indel),
                    dist_indel_punct = VALUES(dist_indel_punct),
                    dist_hamming = VALUES(dist_hamming),
                    dist_hamming_punct = VALUES(dist_hamming_punct),
                    dist_lev = VALUES(dist_lev),
                    dist_lev_punct = VALUES(dist_lev_punct),
                    dist_dlev = VALUES(dist_dlev),
                    dist_dlev_punct = VALUES(dist_dlev_punct)
            """,
            matchlist
        )

    def get_processed_matches(self) -> Iterable[SimbadDBMatch]:
        with self.connect() as conn:
            cursor = conn.cursor(MySQLdb.cursors.SSCursor)
            cursor.execute(
                '''
                    SELECT
                        sys_name,
                        sys_addr,
                        frame,
                        sys_ra,
                        sys_dec,
                        sys_dist,
                        simbad_oid,
                        simbad_main_id,
                        simbad_ident,
                        simbad_ra,
                        simbad_dec,
                        simbad_plx,
                        matched_name,
                        match_source,
                        dist_plx,
                        dist_ly,
                        dist_deg,
                        dist_jw,
                        dist_jw_punct,
                        dist_indel,
                        dist_indel_punct,
                        dist_hamming,
                        dist_hamming_punct,
                        dist_lev,
                        dist_lev_punct,
                        dist_dlev,
                        dist_dlev_punct
                    FROM sys_coord_matches_ident
                '''
            )

            for row in cursor:
                yield SimbadDBMatch(*row)
