from .classes import *
from .interfaces import SystemQueryBase, SystemQueryDatabase
from ..util import filter_match_name, max_dist_ly, max_dist_deg
from astroquery.simbad import SimbadClass, Simbad
from astropy.table import QTable, Table
from collections.abc import Collection, Iterable
import random
from time import sleep
from datetime import timedelta
import numpy as np


class SystemQuerySimbad(SystemQueryBase):
    simbad: SimbadClass

    def __init__(self, simbad: SimbadClass) -> None:
        self.simbad = simbad

    def query_idents(self, names: set[str]) -> dict[str, set[SimbadEntry]]:
        names = Table(
            data=[names],
            names=['name'],
            dtypes=['S']
        )

        query = """
            SELECT
                basic.oid,
                basic.main_id,
                ident.id,
                basic.ra,
                basic.dec,
                basic.plx_value
            FROM TAP_UPLOAD.sys_names
            JOIN ident ON ident.id = sys_names.name
            JOIN basic ON basic.oid = ident.oidref
            WHERE basic.ra IS NOT NULL
              AND basic.dec IS NOT NULL
        """

        result_table = self.simbad.query_tap(query=query, sys_names=names)

        idents = {}

        for name, sb_oid, sb_main_id, sb_ra, sb_dec, sb_plx, sb_ident in result_table:
            entry = SimbadEntry(
                sb_oid,
                sb_main_id,
                sb_ident,
                sb_ra << u.deg,
                sb_dec << u.deg,
                sb_plx << u.mas if sb_plx is not None else None
            )

            idents.setdefault(filter_match_name(name), set()).add(entry)
            idents.setdefault(filter_match_name(sb_ident), set()).add(entry)

        return idents

    def query_coords(self, matches: Collection[SimbadMatch]) -> Iterable[SimbadTableMatch|Iterable]:
        coords = []

        for match in matches:
            coords.append((
                match.sys_name,
                match.sys_addr,
                match.frame,
                match.sys_ra << u.deg,
                match.sys_dec << u.deg,
                match.sys_dist << u.lightyear,
                max(max_dist_deg, (max_dist_ly * u.radian / (match.sys_dist << u.lightyear)) << u.deg)
            ))

        coords = QTable(
            rows=coords,
            names=('sys_name', 'sys_addr', 'frame', 'sys_ra', 'sys_dec', 'sys_dist', 'search_radius'),
            units=(None, None, None, u.deg, u.deg, u.lightyear, u.deg),
            dtypes=('S', 'i8', 'S', 'f8', 'f8', 'f8', 'f8')
        )

        query = """
            SELECT
                sys_coords.sys_name,
                sys_coords.sys_addr,
                sys_coords.frame,
                sys_coords.sys_ra,
                sys_coords.sys_dec,
                sys_coords.sys_dist,
                basic.oid,
                basic.main_id,
                ident.id,
                basic.ra,
                basic.dec,
                basic.plx_value
            FROM TAP_UPLOAD.sys_coords
            LEFT JOIN basic ON CONTAINS(POINT('ICRS', basic.ra, basic.dec), CIRCLE('ICRS', sys_coords.sys_ra, sys_coords.sys_dec, sys_coords.search_radius)) = 1
            LEFT JOIN ident ON ident.oidref = basic.oid
        """

        return self.simbad.query_tap(query=query, sys_coords=coords)


def fetch_all_simbad_basic(simbad: SimbadClass, dest: SystemQueryDatabase, start_oid: int, last_update: date|None):
    last_oid = start_oid

    if start_oid != 0:
        last_update = None

    row_count = 0

    while True:
        if last_update is None:
            where = f"WHERE basic.oid > {last_oid}"
        else:
            where = f"WHERE basic.oid > {last_oid} AND basic.update_date >= '{last_update}'"

        query = f'''
            SELECT
                basic.oid,
                basic.main_id,
                basic.otype,
                basic.ra,
                basic.dec,
                basic.plx_value,
                basic.update_date,
                idcount.idcount
            FROM basic
            LEFT JOIN (
                SELECT oidref, COUNT(*) AS idcount
                FROM ident
                GROUP BY oidref
            ) idcount ON idcount.oidref = basic.oid
            {where}
            ORDER BY oid
        '''

        delay = 5

        print(f'Fetching Simbad stars with oid > {last_oid} and update_date >= {last_update}')

        while True:
            try:
                result_table = simbad.query_tap(query=query, maxrec=100000)
                break
            except Exception as e:
                print(f'Error fetching stars: {e}')
                sleep(delay * random.uniform(1, 2))
                delay = min(delay * 2, 60)
                print('Retrying')

        entries: set[SimbadBasic] = set()

        for oid, main_id, otype, ra, dec, plx, update_date, idcount in result_table:
            entries.add(SimbadBasic(
                int(oid),
                str(main_id),
                str(otype) if otype is not np.ma.masked else None,
                float(ra) if ra is not np.ma.masked else None,
                float(dec) if dec is not np.ma.masked else None,
                float(plx) if plx is not np.ma.masked else None,
                str(update_date),
                int(idcount) if idcount is not np.ma.masked else 0
            ))

            if oid > last_oid:
                last_oid = oid

        if len(entries) == 0:
            break

        dest.insert_basic(entries)
        dest.commit()

        row_count += len(entries)

        print(f'Fetched {row_count} Simbad stars to oid={last_oid}')

        sleep(random.uniform(1, 2))


def fetch_all_simbad_ident(simbad: SimbadClass, dest: SystemQueryDatabase, start_oid: int, last_update: date|None):
    last_oid = start_oid

    if start_oid != 0:
        last_update = None

    row_count = 0

    while True:
        if last_update is None:
            where = f"WHERE basic.oid >= {last_oid}"
        else:
            where = f"WHERE basic.oid >= {last_oid} AND basic.update_date >= '{last_update}'"

        query = f'''
            SELECT
                ident.oidref,
                ident.id,
                basic.update_date
            FROM ident
            JOIN basic ON basic.oid = ident.oidref
            {where}
            ORDER BY oidref
        '''

        delay = 5

        print(f'Fetching Simbad idents with oidref >= {last_oid} and update_date > {last_update}')

        while True:
            try:
                result_table = simbad.query_tap(query=query, maxrec=100000)
                break
            except Exception as e:
                print(f'Error fetching idents: {e}')
                sleep(delay * random.uniform(1, 2))
                delay = min(delay * 2, 60)
                print('Retrying')

        entries: set[SimbadIdent] = set()
        oidents: set[SimbadIdent] = set()

        for oidref, ident, update_date in result_table:
            if oidref > last_oid:
                for ent in oidents:
                    entries.add(ent)

                last_oid = oidref
                oidents = set()

            oidents.add(SimbadIdent(
                int(oidref),
                str(ident),
                str(update_date),
                filter_match_name(str(ident))
            ))

        if len(entries) == 0:
            for ent in oidents:
                entries.add(ent)

            oidents = set()

        dest.insert_idents(entries)
        dest.commit()

        row_count += len(entries)

        print(f'Fetched {row_count} Simbad idents to oidref={last_oid}')

        if len(oidents) == 0:
            break

        sleep(random.uniform(1, 2))


def fetch_all_simbad_idents_basic(dest: SystemQueryDatabase):
    dest.create_tables()
    simbad = Simbad()

    last_oid, last_update, max_date, max_oid = dest.get_last_basic_oid_date()

    fetch_all_simbad_basic(simbad, dest, max_oid or 0, None)

    if max_date is not None:
        max_date = max_date - timedelta(days=30)

        fetch_all_simbad_basic(simbad, dest, 0, max_date)

    last_oid, last_update, max_date, max_oid = dest.get_last_ident_oidref_date()

    fetch_all_simbad_ident(simbad, dest, max_oid or 0, None)

    if max_date is not None:
        max_date = max_date - timedelta(days=30)

        fetch_all_simbad_ident(simbad, dest, 0, max_date)

    dest.commit()
