from typing import Iterator
from .classes import *
from .interfaces import SystemQueryDatabase
from ..util import filter_match_name, calc_search_radius_ra_range
from collections.abc import Collection, Iterable
import sqlite3
import json
import logging
import sys
import re
from astropy.coordinates import angular_separation

logger = logging.getLogger(__name__)


class SystemQuerySqlite3(SystemQueryDatabase):
    conn: sqlite3.Connection

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def query_idents(self, names: set[str]) -> dict[str, set[SimbadEntry]]:
        cursor = self.conn.cursor()

        cursor.execute(
            '''
                SELECT
                    basic.oid,
                    basic.main_id,
                    ident.id,
                    basic.ra,
                    basic.`dec`,
                    basic.plx_value
                FROM JSON_EACH(?) sys_names
                JOIN simbad_ident ident ON ident.match_name = JSON_EXTRACT(sys_names.value, '$.name')
                JOIN simbad_basic basic ON basic.oid = ident.oidref
                WHERE basic.ra IS NOT NULL
                  AND basic.dec IS NOT NULL
            ''',
            (json.dumps([{'name': filter_match_name(n)} for n in names]),)
        )

        idents = {}

        for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in cursor:
            name = filter_match_name(sb_ident)

            entry = SimbadEntry(
                sb_oid,
                sb_main_id,
                sb_ident,
                sb_ra << u.deg,
                sb_dec << u.deg,
                sb_plx << u.mas if sb_plx is not None else None
            )

            idents.setdefault(name, set()).add(entry)

        return idents

    def get_simbad_idents(self) -> dict[str, set[SimbadEntry]]:
        bracketed_post2014_re = re.compile('^\\[[A-Z]+(201[4-9]|20[2-9][0-9])[a-z]*]')
        cursor = self.conn.cursor()

        print('Getting Simbad names')

        cursor.execute('''
            SELECT
                basic.oid,
                basic.main_id,
                ident.id,
                basic.ra,
                basic.`dec`,
                basic.plx_value
            FROM simbad_ident ident
            JOIN simbad_basic basic ON oid = oidref
            WHERE id NOT LIKE 'Gaia DR%'
              AND id NOT LIKE 'Gaia EDR%'
              AND id NOT LIKE 'TIC %'
              AND id NOT LIKE 'UCAC4 %'
              AND ra IS NOT NULL
              AND `dec` IS NOT NULL
        ''')

        idents = {}

        i = 0

        for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in cursor:
            if not bracketed_post2014_re.match(sb_ident):
                name = filter_match_name(sb_ident)

                entry = SimbadEntry(
                    sb_oid,
                    sb_main_id,
                    sb_ident,
                    sb_ra << u.deg,
                    sb_dec << u.deg,
                    sb_plx << u.mas if sb_plx is not None else None
                )

                idents.setdefault(name, set()).add(entry)

            i += 1

            if (i % 100) == 0:
                sys.stderr.write('.')
                sys.stderr.flush()

                if (i % 6400) == 0:
                    sys.stderr.write(f' {i} [{len(idents)}]\n')

        print(f'{len(idents)} idents')

        return idents

    def query_coords(self, matches: Collection[SimbadMatch]) -> Iterable[SimbadTableMatch|Iterable]:
        entries = set()

        sys_coords: list[dict] = []

        for match in matches:
            (search_radius, search_ra_range) = calc_search_radius_ra_range(match.sys_dist, match.sys_dec)
            sys_ra = float(match.sys_ra / u.deg)
            sys_dec = float(match.sys_dec / u.deg)
            sys_dist = float(match.sys_dist / u.lightyear)

            sys_coords.append({
                'sys_name': match.sys_name,
                'sys_addr': match.sys_addr,
                'frame': match.frame,
                'sys_ra': sys_ra,
                'sys_dec': sys_dec,
                'sys_dist': sys_dist,
                'search_radius': search_radius,
                'search_ra_range': search_ra_range
            })

        json_data = json.dumps(list(sys_coords))

        cursor = self.conn.cursor()
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
                    basic.dec,
                    basic.plx_value
                FROM (
                    SELECT
                        JSON_EXTRACT(j.value, '$.sys_name') AS sys_name,
                        JSON_EXTRACT(j.value, '$.sys_addr') AS sys_addr,
                        JSON_EXTRACT(j.value, '$.frame') AS frame,
                        JSON_EXTRACT(j.value, '$.sys_ra') AS sys_ra,
                        JSON_EXTRACT(j.value, '$.sys_dec') AS sys_dec,
                        JSON_EXTRACT(j.value, '$.sys_dist') AS sys_dist,
                        JSON_EXTRACT(j.value, '$.search_radius') AS search_radius,
                        JSON_EXTRACT(j.value, '$.search_ra_range') AS search_ra_range
                    FROM JSON_EACH(?) j
                ) sys_coords
                JOIN simbad_basic basic ON basic.`dec` BETWEEN sys_dec - search_radius AND sys_dec + search_radius
                JOIN simbad_ident ident ON ident.oidref = basic.oid
                WHERE ABS(sys_dec) > 90 - search_radius
                   OR basic.`ra` BETWEEN sys_ra - search_ra_range AND sys_ra + search_ra_range
                   OR basic.`ra` BETWEEN sys_ra - 360 - search_ra_range AND sys_ra - 360 + search_ra_range
                   OR basic.`ra` BETWEEN sys_ra + 360 - search_ra_range AND sys_ra + 360 + search_ra_range
            """,
            (json_data,)
        )

        for sys_name, sys_addr, frame, sys_ra, sys_dec, sys_dist, search_radius, sb_oid, sb_main_id, sb_id, sb_ra, sb_dec, sb_plx in cursor.fetchall():
            dist_deg = angular_separation(sys_ra, sys_dec, sb_ra, sb_dec) << u.deg
            if dist_deg.value < search_radius:
                entries.add(SimbadTableMatch(sys_name, sys_addr, frame, sys_ra, sys_dec, sys_dist, sb_oid, sb_main_id, sb_id, sb_ra, sb_dec, sb_plx))

        return entries

    def create_tables(self):
        self.conn.cursor().execute(
            """
                CREATE TABLE IF NOT EXISTS simbad_ident (
                    oidref INTEGER NOT NULL,
                    id TEXT NOT NULL,
                    update_date TEXT NOT NULL,
                    match_name TEXT NOT NULL
                ) STRICT
            """
        )
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_oidref ON simbad_ident (oidref)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_id ON simbad_ident (id)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_match_name ON simbad_ident (match_name, id)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_ident_update_date ON simbad_ident (update_date)')
        self.conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS UQ_simbad_ident_oidref_id ON simbad_ident (oidref, id)')

        self.conn.cursor().execute(
            """
                CREATE TABLE IF NOT EXISTS simbad_basic (
                    oid INTEGER NOT NULL UNIQUE,
                    main_id TEXT NOT NULL,
                    otype TEXT NULL,
                    ra REAL NULL,
                    `dec` REAL NULL,
                    plx_value REAL NULL,
                    update_date TEXT NOT NULL,
                    idcount INTEGER NOT NULL
                ) STRICT
            """
        )
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_basic_dec ON simbad_basic (`dec`)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_simbad_basic_update_date ON simbad_basic (update_date)')

        self.conn.cursor().execute(
            """
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
                ) STRICT
            """
        )
        self.conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS UQ_sys_coords_addr_frame ON sys_coords (sys_addr, frame)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coords_dec ON sys_coords (sys_dec)')

        self.conn.cursor().execute(
            """
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
                ) STRICT
            """
        )
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_name ON sys_coord_matches_ident (sys_name)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_addr ON sys_coord_matches_ident (sys_addr)')
        self.conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_sys_coord_matches_ident_oidref ON sys_coord_matches_ident (simbad_oid)')
        self.conn.cursor().execute(
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
        cursor = self.conn.cursor()
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
        cursor = self.conn.cursor()
        cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY oid DESC LIMIT 1')
        row = cursor.fetchone()

        if row is None:
            return SimbadOidDate(0, None, None, None)

        max_oid, last_date = row

        cursor = self.conn.cursor()
        cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY update_date DESC, oid DESC LIMIT 1')

        last_oid, max_date = cursor.fetchone()

        last_date = date.fromisoformat(last_date)
        max_date = date.fromisoformat(max_date)

        if last_oid == max_oid:
            return SimbadOidDate(0, last_date, max_date, max_oid)

        return SimbadOidDate(last_oid, last_date, max_date, max_oid)

    def get_last_ident_oidref_date(self) -> SimbadOidDate:
        cursor = self.conn.cursor()
        cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY oidref DESC LIMIT 1')
        row = cursor.fetchone()

        if row is None:
            return SimbadOidDate(0, None, None, None)

        max_oid, last_date = row

        cursor = self.conn.cursor()
        cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY update_date DESC, oidref DESC LIMIT 1')

        last_oid, max_date = cursor.fetchone()

        last_date = date.fromisoformat(last_date)
        max_date = date.fromisoformat(max_date)

        if last_oid == max_oid:
            return SimbadOidDate(0, last_date, max_date, max_oid)

        return SimbadOidDate(last_oid, last_date, max_date, max_oid)

    def insert_basic(self, entries: Iterable[SimbadBasic]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT OR REPLACE INTO simbad_basic
                (oid, main_id, otype, ra, dec, plx_value, update_date, idcount)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entries
        )

    def insert_idents(self, entries: Iterable[SimbadIdent|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT OR REPLACE INTO simbad_ident
                (oidref, id, update_date, match_name) VALUES (?, ?, ?, ?)
            """,
            entries
        )

    def insert_syscoords(self, coords: Iterable[SystemCoords|Iterable]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
                INSERT OR REPLACE INTO sys_coords (
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
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
            """,
            coords
        )

    def commit(self):
        self.conn.commit()

    def query_all_matches(self) -> Iterator[SimbadTableMatch|Iterable]:
        cursor = self.conn.cursor()
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
                    basic.dec,
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
            dist_deg = angular_separation(sys_ra, sys_dec, sb_ra, sb_dec) << u.deg
            if dist_deg.value < search_radius:
                yield SimbadTableMatch(sys_name, sys_addr, frame, sys_ra, sys_dec, sys_dist, sb_oid, sb_main_id, sb_id, sb_ra, sb_dec, sb_plx)

    def get_syscoords(self) -> Iterable[SystemCoords|Iterable]:
        cursor = self.conn.cursor()
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
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?
                )
            """,
            matchlist
        )

    def update_ident_match_names(self):
        print('Updating ident match names')

        i = 0
        n = 0

        while True:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                    SELECT id
                    FROM simbad_ident
                    WHERE match_name IS NULL
                    LIMIT 1000
                '''
            )
            rows = cursor.fetchall()

            if len(rows) == 0:
                sys.stderr.write(f' {i} [{n}]\n')
                break

            rows = [{'id': name, 'match_name': filter_match_name(name)} for name, in rows]

            cursor = self.conn.cursor()
            cursor.execute(
                '''
                    UPDATE simbad_ident INDEXED BY IX_simbad_ident_id
                    SET match_name = JSON_EXTRACT(match_names.value, '$.match_name')
                    FROM JSON_EACH(?) match_names
                    WHERE simbad_ident.id = JSON_EXTRACT(match_names.value, '$.id')
                      AND simbad_ident.match_name IS NULL
                ''',
                (json.dumps(rows),)
            )
            n += cursor.rowcount

            self.conn.commit()

            sys.stderr.write('.')
            sys.stderr.flush()

            i += 1

            if (i % 64) == 0:
                sys.stderr.write(f' {i} [{n}]\n')

    def get_processed_matches(self) -> Iterable[SimbadDBMatch]:
        cursor = self.conn.cursor()
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
