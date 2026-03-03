
# pip install astropy astroquery mysqlclient rapidfuzz frozendict pywikibot

import math
import json
import gzip
import astropy
from astropy import units as u
from astropy.coordinates import SkyCoord, CartesianRepresentation, SphericalRepresentation, Galactic, ICRS, FK4, angular_separation
from astropy.table import QTable, Table
import astroquery
from astroquery.simbad import Simbad
from astroquery.vizier import Vizier
from rapidfuzz import fuzz, utils
from rapidfuzz.distance import JaroWinkler, DamerauLevenshtein, Hamming, Indel, Levenshtein
from dataclasses import dataclass
from typing import Any
from collections.abc import Generator, Collection, Iterable, Sequence, Callable
from abc import abstractmethod
from typing import NamedTuple
from frozendict import frozendict
from time import sleep
import dataclasses
import numpy as np
import MySQLdb
import MySQLdb.cursors
import sqlite3
import itertools
import re
import traceback
import pdb
import urllib.request
import urllib.parse
import random
import os
import sys
import pywikibot
from datetime import date, timedelta


astroquery.__citation__
astropy.__citation__


max_dist_ly = 0.1 * u.lightyear
max_dist_deg = (6 * u.arcmin) << u.deg

space_re = re.compile('  +|-')
num_re = re.compile('[0-9+-]')

greek_letters = {
    'alpha': 'alf',
    'beta': 'bet',
    'gamma': 'gam',
    'delta': 'del',
    'epsilon': 'eps',
    'zeta': 'zet',
    'eta': 'eta',
    'theta': 'tet',
    'iota': 'iot',
    'kappa': 'kap',
    'lambda': 'lam',
    'mu': 'mu.',
    'nu': 'nu.',
    'xi': 'ksi',
    'omicron': 'omi',
    'pi': 'pi.',
    'rho': 'rho',
    'sigma': 'sig',
    'tau': 'tau',
    'upsilon': 'ups',
    'phi': 'phi',
    'chi': 'chi', # https://simbad.u-strasbg.fr/Pages/guide/chA.htx says khi
    'psi': 'psi',
    'omega': 'ome'
}

constellations = {
    'andromedae': 'And',
    'antliae': 'Ant',
    'apodis': 'Aps',
    'apus': 'Aps',
    'aquarii': 'Aqr',
    'aquarius': 'Aqr',
    'aquilae': 'Aql',
    'aquila': 'Aql',
    'arae': 'Ara',
    'arietis': 'Ari',
    'aurigae': 'Aur',
    'bootis': 'Boo',
    'caeli': 'Cae',
    'camelopardalis': 'Cam',
    'cancri': 'Cnc',
    'canum venaticorum': 'CVn',
    'canum veaticorum': 'CVn',
    'canis majoris': 'CMa',
    'canis major': 'CMa',
    'canis minoris': 'CMi',
    'capricorni': 'Cap',
    'carinae': 'Car',
    'cassiopeiae': 'Cas',
    'centauri': 'Cen',
    'cephei': 'Cep',
    'ceti': 'Cet',
    'chamaelontis': 'Cha',
    'circini': 'Cir',
    'columbae': 'Col',
    'comae berenices': 'Com',
    'coronae austrinae': 'CrA',
    'coronae borealis': 'CrB',
    'corvi': 'Crv',
    'crateris': 'Crt',
    'crucis': 'Cru',
    'cygni': 'Cyg',
    'delphini': 'Del',
    'doradus': 'Dor',
    'draconis': 'Dra',
    'equulei': 'Equ',
    'eridani': 'Eri',
    'fornacis': 'For',
    'geminorum': 'Gem',
    'gruis': 'Gru',
    'herculis': 'Her',
    'horologii': 'Hor',
    'hydrae': 'Hya',
    'hydri': 'Hyi',
    'indi': 'Ind',
    'lacertae': 'Lac',
    'leonis minoris': 'LMi',
    'leonis': 'Leo',
    'leporis': 'Lep',
    'librae': 'Lib',
    'libra': 'Lib',
    'lupi': 'Lup',
    'lyncis': 'Lyn',
    'lyrae': 'Lyr',
    'mensae': 'Men',
    'microscopii': 'Mic',
    'monocerotis': 'Mon',
    'muscae': 'Mus',
    'normae': 'Nor',
    'octantis': 'Oct',
    'ophiuchii': 'Oph',
    'ophiuchi': 'Oph',
    'orionis': 'Ori',
    'orion': 'Ori',
    'pavonis': 'Pav',
    'pegasi': 'Peg',
    'persei': 'Per',
    'phoenicis': 'Phe',
    'pictoris': 'Pic',
    'piscium': 'Psc',
    'piscis austrini': 'PsA',
    'puppis': 'Pup',
    'pyxidis': 'Pyx',
    'reticuli': 'Ret',
    'sagittae': 'Sge',
    'sagittarii': 'Sgr',
    'scorpii': 'Sco',
    'sculptoris': 'Scl',
    'scuti': 'Sct',
    'serpentis': 'Ser',
    'sextantis': 'Sex',
    'tauri': 'Tau',
    'telescopii': 'Tel',
    'trianguli australis': 'TrA',
    'trianguli': 'Tri',
    'tucanae': 'Tuc',
    'ursae majoris': 'UMa',
    'ursae minoris': 'UMi',
    'velorum': 'Vel',
    'virginis': 'Vir',
    'volantis': 'Vol',
    'vulpeculae': 'Vul'
}

r_greek = '|'.join((re.escape(v) for v in greek_letters.keys()))
r_grk = '|'.join((re.escape(v) for v in greek_letters.values()))
r_constel = '|'.join((re.escape(v) for v in constellations.keys()))
r_cst = '|'.join((re.escape(v) for v in constellations.values()))

r_bayer = f'(?P<bayer>[a-z]|{r_greek}|{r_grk})'
r_byn = '(?P<bayerN>[1-9])'
r_bynn = '(?P<bayerNN>[0-9][1-9]|[1-9][0-9])'
r_const = f'(?P<const>{r_constel}|{r_cst})'
r_num = '(?P<num>[1-9][0-9]*)'

viz_cat_cache: dict[str, list[Table]] = {}
known_systems = None


def calc_search_radius_ra_range(sys_dist: float|u.Quantity[u.lightyear], sys_dec: float|u.Quantity[u.deg]) -> tuple[float, float]:
    search_radius = float(max(max_dist_deg, (max_dist_ly * u.radian / (sys_dist << u.lightyear)) << u.deg) / u.deg)
    sys_dec = float((sys_dec << u.deg) / u.deg)
    sys_dist = float((sys_dist << u.lightyear) / u.lightyear)
    cos_dec_search = math.cos(math.radians(abs(sys_dec) + search_radius))

    if cos_dec_search < search_radius / 360:
        search_ra_range = 360.0
    else:
        search_ra_range = search_radius / math.cos(math.radians(abs(sys_dec) + search_radius))

    return (search_radius, search_ra_range)

@dataclass(frozen=True)
class CatQuery:
    source: str
    catalogue: str
    cat_filter: frozendict[str, str|int|float]
    result: str


@dataclass(frozen=True)
class MatchIdent:
    ident: str
    maxdist: float = 1.0
    is_alt_name: bool = False
    source: str|None = None


@dataclass(frozen=True)
class SimbadEntry:
    oid: int
    main_id: str
    ident: str
    ra: u.Quantity[u.deg]
    dec: u.Quantity[u.deg]
    plx: u.Quantity[u.mas]


@dataclass(frozen=True)
class SimbadMatch:
    sys_name: str
    sys_addr: int
    frame: str
    sys_ra: u.Quantity[u.deg]
    sys_dec: u.Quantity[u.deg]
    sys_dist: u.Quantity[u.lightyear]
    simbad: SimbadEntry|None = None
    matched_name: str|None = None
    match_source: str|None = None
    dist_plx: float|None = None
    dist_ly: float|None = None
    dist_deg: float|None = None
    dist_jw: float|None = None
    dist_jw_punct: float|None = None
    dist_indel: float|None = None
    dist_indel_punct: float|None = None
    dist_hamming: float|None = None
    dist_hamming_punct: float|None = None
    dist_lev: float|None = None
    dist_lev_punct: float|None = None
    dist_dlev: float|None = None
    dist_dlev_punct: float|None = None
    is_alt_name: bool = False


class SimbadDBMatch(NamedTuple):
    sys_name: str
    sys_addr: int
    frame: str
    sys_ra: float
    sys_dec: float
    sys_dist: float
    oid: int
    simbad_main_id: str
    simbad_ident: str
    simbad_ra: float
    simbad_dec: float
    simbad_plx: float
    matched_name: str|None = None
    match_source: str|None = None
    dist_plx: float|None = None
    dist_ly: float|None = None
    dist_deg: float|None = None
    dist_jw: float|None = None
    dist_jw_punct: float|None = None
    dist_indel: float|None = None
    dist_indel_punct: float|None = None
    dist_hamming: float|None = None
    dist_hamming_punct: float|None = None
    dist_lev: float|None = None
    dist_lev_punct: float|None = None
    dist_dlev: float|None = None
    dist_dlev_punct: float|None = None


class SimbadTableMatch(NamedTuple):
    sys_name: str
    sys_addr: int
    frame: str
    sys_ra: u.Quantity[u.deg]
    sys_dec: u.Quantity[u.deg]
    sys_dist: u.Quantity[u.lightyear]
    simbad_oid: int
    simbad_main_id: str
    simbad_ident: str
    simbad_ra: u.Quantity[u.deg]
    simbad_dec: u.Quantity[u.deg]
    simbad_plx: u.Quantity[u.mas]


class SimbadBasic(NamedTuple):
    oid: int
    main_id: str
    otype: str|None
    ra: float|None
    dec: float|None
    plx: float|None
    update_date: str
    ident_count: int


class SimbadIdent(NamedTuple):
    oidref: int
    id: str
    update_date: str
    match_name: str


class SystemXYZ(NamedTuple):
    sys_name: str
    sys_addr: int
    x: float
    y: float
    z: float


class SystemCoords(NamedTuple):
    sys_name: str
    sys_addr: int
    frame: str
    x: float
    y: float
    z: float
    sys_ra: float
    sys_dec: float
    sys_dist: float
    search_radius: float
    search_ra_range: float


class SystemQueryBase:
    @abstractmethod
    def query_idents(self, names: set[str]) -> dict[str, set[SimbadEntry]]:
        pass
    
    @abstractmethod
    def query_coords(self, matches: Collection[SimbadMatch]) -> Iterable[SimbadTableMatch|Iterable]:
        pass


class SystemQueryDatabase(SystemQueryBase):
    @abstractmethod
    def get_simbad_idents(self) -> dict[str, set[SimbadEntry]]:
        pass

    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def get_basic_ident_diff(self) -> list[int]:
        pass

    @abstractmethod
    def get_last_basic_oid_date(self) -> tuple[int, date, date, int]:
        pass

    @abstractmethod
    def get_last_ident_oidref_date(self) -> tuple[int, date, date, int]:
        pass

    @abstractmethod
    def insert_basic(self, basics: Iterable[SimbadBasic|Iterable]):
        pass
    
    @abstractmethod
    def insert_idents(self, idents: Iterable[SimbadIdent|Iterable]):
        pass

    @abstractmethod
    def insert_syscoords(self, coords: Iterable[SystemCoords|Iterable]):
        pass

    @abstractmethod
    def get_syscoords(self) -> Iterable[SystemCoords|Iterable]:
        pass

    @abstractmethod
    def query_all_matches(self) -> Iterable[SimbadTableMatch|Iterable]:
        pass
    
    @abstractmethod
    def insert_matches(self, matches: Iterable[SimbadDBMatch|Iterable]):
        pass


class SystemQuerySimbad(SystemQueryBase):
    simbad: Simbad

    def __init__(self, simbad: Simbad):
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

            idents.setdefault(space_re.sub(' ', utils.default_process(name.strip().lower())), set()).add(entry)
            idents.setdefault(space_re.sub(' ', utils.default_process(sb_ident.strip().lower())), set()).add(entry)

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


class SystemQueryMariaDB(SystemQueryDatabase):
    connopts: dict
    conn: MySQLdb.Connection

    def __init__(self, **kwargs):
        self.connopts = kwargs
        self.conn = self.connect()

    def connect(self):
        return MySQLdb.connect(**self.connopts)

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
                (json.dumps([{'name': space_re.sub(' ', utils.default_process(n.strip().lower()))} for n in names]),)
            )

            idents = {}

            for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in cursor:
                name = space_re.sub(' ', utils.default_process(sb_ident.strip().lower()))
                
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

            print(f'{len(simbad_idents)} idents')

            idents = {}

            for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in rows:
                name = space_re.sub(' ', utils.default_process(sb_ident.strip().lower()))
                
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

            sys_coords: list[dict] = []

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
                            float(sys_ra),
                            float(sys_dec),
                            float(sys_dist),
                            int(sb_oid),
                            str(sb_main_id),
                            str(sb_id),
                            float(sb_ra),
                            float(sb_dec),
                            float(sb_plx) if sb_plx is not None else None
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
                    )
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
                    )
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
                    )
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
                    )
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

    def get_last_basic_oid_date(self) -> tuple[int, date|None, date|None, int|None]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY oid DESC LIMIT 1')
            row = cursor.fetchone()

            if row is None:
                return (0, None, None, None)

            max_oid, last_date = row

            cursor = conn.cursor()
            cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY update_date DESC, oid DESC LIMIT 1')

            last_oid, max_date = cursor.fetchone()

            if last_oid == max_oid:
                return (0, last_date, max_date, max_oid)

            return (last_oid, last_date, max_date, max_oid)

    def get_last_ident_oidref_date(self) -> tuple[int, date|None, date|None, int|None]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY oidref DESC LIMIT 1')
            row = cursor.fetchone()

            if row is None:
                return (0, None, None, None)

            max_oid, last_date = row

            cursor = conn.cursor()
            cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY update_date DESC, oidref DESC LIMIT 1')

            last_oid, max_date = cursor.fetchone()

            if last_oid == max_oid:
                return (0, last_date, max_date, max_oid)

            return (last_oid, last_date, max_date, max_oid)

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
                        float(sys_ra),
                        float(sys_dec),
                        float(sys_dist),
                        int(sb_oid),
                        str(sb_main_id),
                        str(sb_id),
                        float(sb_ra),
                        float(sb_dec),
                        float(sb_plx) if sb_plx is not None else None
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

            rows = [{'id': name, 'match_name': space_re.sub(' ', utils.default_process(name.strip().lower()))} for name, in rows]

            cursor = self.conn.cursor()
            cursor.execute(
                '''
                    UPDATE simbad_ident ident
                    JOIN JSON_TABLE(%s, '$[*]' COLUMNS(
                        id VARCHAR(255) PATH '$.id',
                        match_name VARCHAR(255) PATH '$.match_name'
                    )) match_names ON ident.id = match_names.id
                    SET ident.match_name = match_names.match_name
                    WHERE ident.match_name IS NULL
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
            (json.dumps([{'name': space_re.sub(' ', utils.default_process(n.strip().lower()))} for n in names]),)
        )

        idents = {}

        for sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in cursor:
            name = space_re.sub(' ', utils.default_process(sb_ident.strip().lower()))
            
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
        bracketed_post2014_re = re.compile('^\\[[A-Z]+(201[4-9]|20[2-9][0-9])[a-z]*\\]')
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
                name = space_re.sub(' ', utils.default_process(sb_ident.strip().lower()))
                
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
                        JSON_EXTRACT(sys_coords_json, '$.sys_name') AS sys_name,
                        JSON_EXTRACT(sys_coords_json, '$.sys_addr') AS sys_addr,
                        JSON_EXTRACT(sys_coords_json, '$.frame') AS frame,
                        JSON_EXTRACT(sys_coords_json, '$.sys_ra') AS sys_ra,
                        JSON_EXTRACT(sys_coords_json, '$.sys_dec') AS sys_dec,
                        JSON_EXTRACT(sys_coords_json, '$.sys_dist') AS sys_dist,
                        JSON_EXTRACT(sys_coords_json, '$.search_radius') AS search_radius,
                        JSON_EXTRACT(sys_coords_json, '$.search_ra_range') AS search_ra_range
                    FROM JSON_EACH(%s)
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

    def get_last_basic_oid_date(self) -> tuple[int, date|None, date|None, int|None]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY oid DESC LIMIT 1')
        row = cursor.fetchone()

        if row is None:
            return (0, None, None, None)

        max_oid, last_date = row

        cursor = self.conn.cursor()
        cursor.execute('SELECT oid, update_date FROM simbad_basic ORDER BY update_date DESC, oid DESC LIMIT 1')

        last_oid, max_date = cursor.fetchone()

        last_date = date.fromisoformat(last_date)
        max_date = date.fromisoformat(max_date)

        if last_oid == max_oid:
            return (0, last_date, max_date, max_oid)

        return (last_oid, last_date, max_date, max_oid)

    def get_last_ident_oidref_date(self) -> tuple[int, date|None, date|None, int|None]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY oidref DESC LIMIT 1')
        row = cursor.fetchone()

        if row is None:
            return (0, None, None, None)

        max_oid, last_date = row

        cursor = self.conn.cursor()
        cursor.execute('SELECT oidref, update_date FROM simbad_ident ORDER BY update_date DESC, oidref DESC LIMIT 1')

        last_oid, max_date = cursor.fetchone()

        last_date = date.fromisoformat(last_date)
        max_date = date.fromisoformat(max_date)

        if last_oid == max_oid:
            return (0, last_date, max_date, max_oid)

        return (last_oid, last_date, max_date, max_oid)

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

    def query_all_matches(self) -> Generator[SimbadTableMatch|Iterable]:
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

            rows = [{'id': name, 'match_name': space_re.sub(' ', utils.default_process(name.strip().lower()))} for name, in rows]

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


def get_ed_known_systems(name_or_id: str|int) -> Generator[str]:
    global known_systems

    if known_systems is None:
        known_systems = {}
        if os.path.exists('known_systems.json'):
            try:
                with open('known_systems.json', 'rt') as f:
                    for system in json.load(f):
                        id64 = system.get('id64')
                        name = system.get('name')
                        altnames = system.get('altnames')
                        hd = system.get('hd')
                        hipp = system.get('hipp')
                        gliese = system.get('gliese')

                        if name is not None and (hd is not None or hipp is not None or gliese is not None):
                            known_systems.setdefault(name, []).append({'name': name, 'altnames': altnames, 'hd': hd, 'hipp': hipp, 'gliese': gliese })
                        if id64 is not None and (hd is not None or hipp is not None or gliese is not None or name is not None):
                            known_systems.setdefault(id64, []).append({'name': name, 'altnames': altnames, 'hd': hd, 'hipp': hipp, 'gliese': gliese })
            except:
                pass

    for system in known_systems.get(name_or_id, []):
        if (name := system.get('name')) is not None:
            yield name

        if (altnames := system.get('altnames')) is not None and isinstance(altnames, Iterable):
            for name in altname:
                if isinstance(name, str):
                    yield name

        if (hd := system.get('hd')) is not None:
            if isinstance(hd, int) or (isinstance(hd, str) and hd.isdigit()):
                yield f'HD {hd:>6}'
            elif isinstance(hd, str):
                yield hd

        if (hipp := system.get('hipp')) is not None:
            if isinstance(hipp, int) or (isinstance(hd, str) and hd.isdigit()):
                yield f'HIP {hipp}'
            elif isinstance(hipp, str):
                yield hipp

        if (gliese := system.get('gliese')) is not None:
            if isinstance(gliese, str) and gliese.lower().startswith('gl '):
                yield f'GJ {gliese[3:]}'


def get_vizier_cat(catname: str) -> Table:
    vc = viz_cat_cache.get(catname)

    if vc is None:
        vizier = Vizier(row_limit=-1)
        vc = viz_cat_cache.setdefault(catname, vizier.get_catalogs(catname))

    return vc[0]


def query_cat(cat: CatQuery) -> Generator[str|MatchIdent]:
    if cat.source == 'Vizier':
        tbl = get_vizier_cat(cat.catalogue)
        mask = None

        for n, v in cat.cat_filter.items():
            if mask is None:
                mask = tbl[n] == v
            else:
                mask &= tbl[n] == v

        for row in tbl[mask]:
            yield MatchIdent(cat.result.format(** { k: row.get(k) for k in row.keys() }), source=f'{cat.source}:{cat.catalogue}')


def s_bayer_p(m: re.Match) -> str:
    return greek_letters.get(m.group('bayer').lower(), m.group('bayer'))


def s_const(m: re.Match) -> str:
    return constellations.get(m.group('const').lower(), m.group('const'))


def s_bayer(m: re.Match) -> str:
    return f'* {s_bayer_p(m)} {s_const(m)}'


def s_bayer_n(m: re.Match) -> str:
    return f'* {s_bayer_p(m)}0{m.group('bayerN')} {s_const(m)}'


def s_bayer_nn(m: re.Match) -> str:
    return f'* {s_bayer_p(m)}{m.group('bayerNN')} {s_const(m)}'


def s_varstar(m: re.Match) -> str:
    return f'V* {m.group('var')} {s_const(m)}'


def s_flamsteed(m: re.Match) -> str:
    return f'* {m.group('num'):>3} {s_const(m)}'


def s_gould(m: re.Match) -> CatQuery:
    sn = int(m.group('num'))
    cn = s_const(m)

    return CatQuery('Vizier', 'V/135A/catalog', frozendict({ 'G': sn, 'cst': cn }), 'HD {HD}')


def s_gould_rev(m: re.Match) -> MatchIdent:
    return MatchIdent(f'* {m.group('num')}G {s_const(m)}', source='[Vizier:V/135A/catalog]')


patterns: list[tuple[re.Pattern, list[Callable[[re.Match], str|CatQuery|MatchIdent|None]]]] = [
    (re.compile('^.*$'),
     [lambda m: MatchIdent(f'NAME {m.group(0)}', 0.1),
      lambda m: MatchIdent(f'HIDDEN NAME {m.group(0)}', 0.1)]),
    (re.compile(f'^(?:[*] )?{r_bayer} {r_const}', re.IGNORECASE), [s_bayer]),
    (re.compile(f'^(?:[*] )?{r_bayer}[ -]?{r_byn} {r_const}', re.IGNORECASE), [s_bayer_n]),
    (re.compile(f'^(?:[*] )?{r_bayer}[ -]?{r_bynn} {r_const}', re.IGNORECASE), [s_bayer_nn]),
    (re.compile(f'^(?:[*] )?{r_num} {r_const}', re.IGNORECASE), [s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer} {r_const}', re.IGNORECASE), [s_bayer, s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer}[ -]?{r_byn} {r_const}', re.IGNORECASE), [s_bayer_n, s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer}[ -]?{r_bynn} {r_const}', re.IGNORECASE), [s_bayer_nn, s_flamsteed]),
    (re.compile(f'^(?:V?[*] )?{r_num} (?P<var>[A-Z]) {r_const}', re.IGNORECASE), [s_varstar, s_flamsteed]),
    (re.compile(f'^(?:V?[*] )?{r_num} (?P<var>[A-Z][A-Z]) {r_const}', re.IGNORECASE), [s_varstar, s_flamsteed]),
    (re.compile(f'^{r_num} G\\. ?{r_const}', re.IGNORECASE), [s_gould, s_gould_rev]),
    (re.compile(f'^(?:V?[*] )?(?P<var>[A-Z]) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?(?P<var>[A-Z][A-Z]) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?(?P<var>V[0-9]+) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?V0(?P<var>[1-9][0-9]+) {r_const}', re.IGNORECASE),
     [lambda m: f'V* V{m.group('var')} {s_const(m)}']),
    (re.compile('^(BAG|BAR|BRT|COO|CPO|DON|EGN|HDS|HJ|KUI|LDS|MET|RMK|RST|STF|WSI) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'** {m.group(1)} {m.group(2):>4}']),
    (re.compile('^(?:EM[*] )?(CDS|LkHA|GGA|GGR|MWC|StHA|VES) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'EM* {m.group(1)} {m.group(2):>4}']),
    (re.compile('^(AD95|BB2009|BBG2010|BBS2011|BJG2004|BSM2011|CPO2009|DBP2006|DM99|DML87|FHM2008|FMS2006|GFT2002|GHJ2008|GMB2010|GMM2008|GMM2009|GMW2007|GVS98|GZB2006|H97b|HD2002|HFR2007|HGM2009b|HRF2005|IHA2007|IHA2008|JBM2010|JVD2011|KAG2008|KW97|LAL96|MJD95|MKS2009|MMS2011|MSJ2009|MSR2009|OJV2009|OTS2008|OW94|PCB2009|PMD2009|PW2010|RBB2002|S87b|SHB2004|SHD2009|SNM2009|WBG2011|WMW2010|YSD2013) (.*)', re.IGNORECASE),
     [lambda m: f'[{m.group(1)}] {m.group(2)}']),

    (re.compile('^(?:Cl )?(NGC|Pismis|Trumpler|IC|Melotte) ([0-9]+) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'Cl {m.group(1)} {m.group(2):>4} {m.group(3):>5}']),
    (re.compile('^(?:Cl[*] )?(NGC|Trumpler|Blanco|Haffner|Melotte|IC|Stock) ([0-9]+) ([A-Z]+) ([0-9A-Z-]+)', re.IGNORECASE),
     [lambda m: f'Cl* {m.group(1)} {m.group(2):>4} {m.group(3):>6} {m.group(4):>7}']),

    (re.compile('^CFHT-BL-([0-9]+)', re.IGNORECASE),
     [lambda m: f'[MBS2007b] {m.group(0)}']),
    (re.compile('^S171 [0-9]+', re.IGNORECASE),
     [lambda m: f'[GMM2009] {m.group(0)}']),
    (re.compile('^DEN ([0-9]{4})([+-][0-9]{4})', re.IGNORECASE),
     [lambda m: f'DENIS J{m.group(1)}.0{m.group(2)}']),
    (re.compile('^GRS (.*)', re.IGNORECASE),
     [lambda m: f'Granat {m.group(1)}']),
    (re.compile('^(?:Gmb|GMB|Groombridge) (.*)', re.IGNORECASE),
     [lambda m: f'Gmb {m.group(1):>4}']),
    (re.compile('^Kruger (.*)', re.IGNORECASE),
     [lambda m: f'** KR {m.group(1):>4}']),
    (re.compile('^(?:GJ|Gl|Gliese|NN|Wo) (.*)', re.IGNORECASE),
     [lambda m: f'GJ {m.group(1)}']),
    (re.compile('^(?:Lalande) (.*)', re.IGNORECASE),
     [lambda m: f'LAL {m.group(1)}']),
    (re.compile('^KOI ([0-9]+)', re.IGNORECASE),
     [lambda m: f'KOI-{m.group(1)}']),
    (re.compile('^MOA-([0-9]{4}-BLG-[0-9]+)', re.IGNORECASE),
     [lambda m: f'MOA {m.group(1)}']),
    (re.compile('^OGLE-TR-([0-9]+)', re.IGNORECASE),
     [lambda m: f'OGLE-TR {m.group(1)}']),
    (re.compile('^LOrionis-(CFHT|SOC|MAD) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'LOri-{m.group(1)} {m.group(2):>3}']),
    (re.compile('^(Cyg|Nor|TrA)(?:ni)? (X-[1-9])', re.IGNORECASE),
     [lambda m: f'X {m.group(1)} {m.group(2)}']),
    (re.compile(f'(.*?) {r_const}', re.IGNORECASE),
     [lambda m: f'NAME {m.group(1)} {s_const(m)}',
      lambda m: f'HIDDEN NAME {m.group(1)} {s_const(m)}',
      lambda m: f'{m.group(1)} {s_const(m)}'])
]

def get_match_names(name: str) -> set[str|MatchIdent]:
    names = {name}

    for pattern, mangles in patterns:
        if match := pattern.match(name):
            for mangle in mangles:
                mangled = mangle(match)
                
                if isinstance(mangled, CatQuery):
                    mnames = list(query_cat(mangled))
                elif isinstance(mangled, MatchIdent):
                    mnames = [mangled]
                elif isinstance(mangled, str):
                    mnames = [name.replace(match.group(0), mangled)]
                else:
                    mnames = []

                for mname in mnames:
                    names.add(mname)

    return names


def add_fuzz_distances(match: SimbadMatch, simbad: SimbadEntry, name: str, is_alt_name: bool = False, source: str|None = None) -> SimbadMatch:
    dist_deg = angular_separation(match.sys_ra, match.sys_dec, simbad.ra, simbad.dec) << u.deg
    dist_ly = float((dist_deg << u.radian) * match.sys_dist / u.lightyear / u.radian)
    dist_deg = float(dist_deg / u.deg)
    lident = space_re.sub(' ', simbad.ident.strip().lower())
    xident = space_re.sub(' ', utils.default_process(simbad.ident.strip().lower()))
    lname = space_re.sub(' ', name.strip().lower())
    xname = space_re.sub(' ', utils.default_process(name.strip().lower()))

    sys_plx = 1000 * u.mas * u.parsec / (match.sys_dist << u.parsec)

    if simbad.plx is not None:
        dist_plx = float(abs(simbad.plx.value - sys_plx.value))
    else:
        dist_plx = None

    return dataclasses.replace(
        match,
        simbad=simbad,
        matched_name=name,
        match_source=source,
        dist_plx=round(dist_plx, 6) if dist_plx is not None else None,
        dist_ly=round(dist_ly, 6),
        dist_deg=round(dist_deg, 6),
        dist_jw=round(JaroWinkler.normalized_distance(lname, lident), 6),
        dist_indel=round(Indel.normalized_distance(lname, lident), 6),
        dist_dlev=round(DamerauLevenshtein.normalized_distance(lname, lident), 6),
        dist_hamming=round(Hamming.normalized_distance(lname, lident), 6),
        dist_lev=round(Levenshtein.normalized_distance(lname, lident), 6),
        dist_jw_punct=round(JaroWinkler.normalized_distance(xname, xident), 6),
        dist_indel_punct=round(Indel.normalized_distance(xname, xident), 6),
        dist_dlev_punct=round(DamerauLevenshtein.normalized_distance(xname, xident), 6),
        dist_hamming_punct=round(Hamming.normalized_distance(xname, xident), 6),
        dist_lev_punct=round(Levenshtein.normalized_distance(xname, xident), 6),
        is_alt_name=is_alt_name
    )


def get_wikipedia_starbox_simbad_reference(name: str) -> str|None:
    wiki_site = pywikibot.Site('en', 'wikipedia')
    wiki_page = pywikibot.Page(wiki_site, name)

    if wiki_page.exists():
        if wiki_page.isRedirectPage():
            wiki_page = wiki_page.getRedirectTarget()
        
        for t, p in wiki_page.templatesWithParams():
            if t.title() == 'Template:Starbox reference':
                for r in p:
                    n, v = r.split('=', 2)
                    if n.lower() == 'simbad':
                        return urllib.parse.unquote(v.replace('+',' '))


def filter_matches(sy_matches: set[SimbadMatch]) -> tuple[bool, set[SimbadMatch]]:
    if any(m.dist_ly < 0.1 and m.dist_indel == 0 and not m.is_alt_name for m in sy_matches):
        return (True, set((m for m in sy_matches if m.dist_ly < 0.1 and m.dist_indel == 0 and not m.is_alt_name)))
    elif any(m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10 and not m.is_alt_name for m in sy_matches):
        return (True, set((m for m in sy_matches if ((m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10) or m.dist_indel == 0) and not m.is_alt_name)))
    elif any(m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10 and not m.is_alt_name for m in sy_matches):
        return (True, set((m for m in sy_matches if ((m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10) or m.dist_indel == 0) and not m.is_alt_name)))
    elif any(m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
        return (True, set((m for m in sy_matches if ((m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1) or m.dist_indel == 0) and not m.is_alt_name)))
    elif any(m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
        return (True, set((m for m in sy_matches if ((m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1) or m.dist_indel == 0) and not m.is_alt_name)))
    return (False, sy_matches)


def get_rev_matches(source: str, simbad: SimbadEntry) -> Iterable[tuple[str,SimbadEntry]]:
    if source == '[Vizier:V/135A/catalog]':
        if simbad.ident.startswith('HD ') and len(simbad.ident) == 9:
            hdnum = simbad.ident[3:]
            tbl = get_vizier_cat('V/135A/catalog')

            mask = tbl['HD'] == int(hdnum.strip())

            for row in tbl[mask]:
                yield (f'[Vizier:V/135A/catalog(HD={hdnum})]', dataclasses.replace(
                    simbad,
                    ident=f'* {row['Name']}'
                ))
    else:
        yield (source, simbad)


def process_matches(rows: Iterable[SimbadTableMatch|Iterable], idents: dict[str, set[SimbadEntry]]) -> set[SimbadMatch]:
    matches = set()
    base_matches = {}

    for sy_name, sy_addr, sy_frame, sy_ra, sy_dec, sy_dist, sb_oid, sb_main_id, sb_ident, sb_ra, sb_dec, sb_plx in rows:
        entry = SimbadMatch(
            sy_name,
            sy_addr,
            sy_frame,
            sy_ra << u.deg,
            sy_dec << u.deg,
            sy_dist << u.lightyear,
            None
        )

        sys_matches = base_matches.setdefault((sy_name, sy_addr), {})
        sy_matches = sys_matches.get(entry)

        names = get_match_names(sy_name)

        for name in get_ed_known_systems(sy_name):
            names.add(MatchIdent(name, is_alt_name=True, source='known_systems'))

        for name in get_ed_known_systems(sy_addr):
            names.add(MatchIdent(name, is_alt_name=True, source='known_systems'))

        if sy_matches is None:
            sy_matches = sys_matches.setdefault(entry, {})

            for name in names:
                is_alt_name = False
                source = None

                if isinstance(name, MatchIdent):
                    is_alt_name = name.is_alt_name
                    source = name.source
                    name = name.ident

                for ident in idents.get(space_re.sub(' ', utils.default_process(name.strip().lower())), []):
                    sy_matches.setdefault((name, is_alt_name, source), set()).add(ident)

        if sb_oid is not None:
            sb_entry = SimbadEntry(
                sb_oid,
                sb_main_id,
                sb_ident,
                sb_ra << u.deg,
                sb_dec << u.deg,
                sb_plx << u.mas if sb_plx is not None else None
            )

            lident = space_re.sub(' ', sb_ident.lower())

            for name in names:
                max_dist = 1.0
                is_alt_name = False
                source = None

                if isinstance(name, MatchIdent):
                    max_dist = name.maxdist
                    is_alt_name = name.is_alt_name
                    source = name.source
                    name = name.ident

                lname = space_re.sub(' ', name.lower())

                dist_indel = Indel.normalized_distance(lname, lident)

                if dist_indel > max_dist:
                    continue

                sy_matches.setdefault((name, is_alt_name, source), set()).add(sb_entry)

    for na, entries in base_matches.items():
        sy_matches = set()
        sb_entries = set()

        for entry, names in entries.items():
            for (name, is_alt_name, source), nmatches in names.items():
                for sb_entry in nmatches:
                    sb_entries.add(sb_entry)

                    for src, sb_sub in get_rev_matches(source, sb_entry):
                        sy_matches.add(add_fuzz_distances(entry, sb_sub, name, is_alt_name, src))

        is_match, sy_matches = filter_matches(sy_matches)

        if not is_match and not any(min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
            sys.stderr.write(f'Querying Wikipedia for {na[0]}\n')
            wiki_simbad = get_wikipedia_starbox_simbad_reference(na[0])

            if wiki_simbad is not None:
                for entry, _ in entries.items():
                    for sb_entry in sb_entries:
                        sy_matches.add(add_fuzz_distances(entry, sb_entry, wiki_simbad, False, 'wikipedia'))
                
                is_match, sy_matches = filter_matches(sy_matches)

        if not is_match:
            if any(min(m.dist_indel, m.dist_jw) == 0 and not m.is_alt_name for m in sy_matches):
                min_dist_deg = min((m.dist_deg for m in sy_matches if min(m.dist_indel, m.dist_jw) == 0 and not m.is_alt_name))
                sys.stderr.write(f'System {na[0]} [{na[1]}] name match, dist_deg={min_dist_deg}\n')
            elif any(min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
                min_dist_deg = min((m.dist_deg for m in sy_matches if min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name))
                sys.stderr.write(f'System {na[0]} [{na[1]}] fuzzy name match, dist_deg={min_dist_deg}\n')
            else:
                sys.stderr.write(f'System {na[0]} [{na[1]}] no name match\n')

        sy_matches = set((next(iter(sorted(g, key=lambda m: m.dist_deg))) for _, g in itertools.groupby(sy_matches, key=lambda m: (m.simbad, m.matched_name))))

        for match in sy_matches:
            matches.add(match)

    return matches


def match_simbad_coords(matches: Collection[SimbadMatch], systemquery: SystemQueryBase) -> set[SimbadMatch]:
    idents = set()

    for match in matches:
        for name in get_match_names(match.sys_name):
            if isinstance(name, MatchIdent):
                name = name.ident

            idents.add(name)

        for name in get_ed_known_systems(match.sys_name):
            idents.add(name)

        for name in get_ed_known_systems(match.sys_addr):
            idents.add(name)

    idents = systemquery.query_idents(idents)

    result_table = systemquery.query_coords(matches)

    return process_matches(result_table, idents)


def match_simbad_coords_chunked(matches: Collection[SimbadMatch], systemquery: SystemQueryBase) -> set[SimbadMatch]:
    results: set[SimbadMatch] = set()

    for grp in itertools.batched(matches, 1000):
        for result in match_simbad_coords(grp, systemquery):
            results.add(result)
    
    return results


def match_simbad_xyz(systems: Collection[SystemXYZ], systemquery: SystemQueryBase) -> set[SimbadMatch]:
    coords: set[SimbadMatch] = set()

    for sysaddr, name, x, y, z in systems:
        if x is None or y is None or z is None or sysaddr is None:
            continue

        cart = CartesianRepresentation(z, -x, y, unit=u.lightyear)
        coord = SphericalRepresentation.from_cartesian(cart)

        if coord.distance < 3 * u.lightyear:
            continue

        icrs = SkyCoord(coord.lon, coord.lat, coord.distance, frame=Galactic).icrs
        fk4 = icrs.fk4
        fk4_icrs = SkyCoord(fk4.ra, fk4.dec, fk4.distance, frame=ICRS)

        coords.append(SimbadMatch(
            name,
            sysaddr,
            'icrs',
            u.Quantity(icrs.ra),
            u.Quantity(icrs.dec),
            coord.distance
        ))

        coords.append(SimbadMatch(
            name,
            sysaddr,
            'fk4_icrs',
            u.Quantity(fk4_icrs.ra),
            u.Quantity(fk4_icrs.dec),
            coord.distance
        ))

    return match_simbad_coords(coords, systemquery)


def match_simbad_xyz_chunked(systems: Collection[SystemXYZ], systemquery: SystemQueryBase) -> set[SimbadMatch]:
    results: set[SimbadMatch] = set()

    for grp in itertools.batched(systems, 1000):
        for result in match_simbad_xyz(grp, systemquery):
            results.add(result)

    return results


def match_simbad_syscoords(systems: Collection[SystemCoords], systemquery: SystemQueryBase) -> set[SimbadMatch]:
    coords = set()

    for sys_name, sys_addr, frame, x, y, z, sys_ra, sys_dec, sys_dist, search_radius, search_ra_range in systems:
        coords.add(SimbadMatch(
            sys_name,
            sys_addr,
            frame,
            sys_ra << u.deg,
            sys_dec << u.deg,
            sys_dist << u.lightyear
        ))

    return match_simbad_coords(coords, systemquery)


def save_matches_db(matches: set[SimbadMatch], systemquery: SystemQueryDatabase, matches_by_system: dict[tuple[int, str], dict]):
    matchlist: set[SimbadDBMatch] = set()

    for match in matches:
        matchlist.add(SimbadDBMatch(
            str(match.sys_name),
            int(match.sys_addr),
            str(match.frame),
            float((match.sys_ra << u.deg) / u.deg),
            float((match.sys_dec << u.deg) / u.deg),
            float((match.sys_dist << u.lyr) / u.lyr),
            int(match.simbad.oid),
            str(match.simbad.main_id),
            str(match.simbad.ident),
            float((match.simbad.ra << u.deg) / u.deg),
            float((match.simbad.dec << u.deg) / u.deg),
            float((match.simbad.plx << u.mas) / u.mas) if match.simbad.plx is not None else None,
            str(match.matched_name),
            str(match.match_source),
            match.dist_plx,
            match.dist_ly,
            match.dist_deg,
            match.dist_jw,
            match.dist_jw_punct,
            match.dist_indel,
            match.dist_indel_punct,
            match.dist_hamming,
            match.dist_hamming_punct,
            match.dist_lev,
            match.dist_lev_punct,
            match.dist_dlev,
            match.dist_dlev_punct
        ))

        # sysmatches = matches_by_system.setdefault((match.sys_addr, match.sys_name), {
        #     'sysaddr': match.sys_addr,
        #     'name': match.sys_name,
        #     'best_match': None,
        #     'matches': {}
        # })

        # name_matches = sysmatches.setdefault(match.matched_name, set())

        # name_matches.add(match)

        # totdist = min(match.dist_indel, match.dist_jw) + min(match.dist_ly, match.dist_deg) + min(0.05, match.dist_plx)

        # if min(match.dist_indel, match.dist_jw) < 0.1 and min(match.dist_ly, match.dist_deg) < 0.1:
        #     bmatch = sysmatches['best_match']

        #     if bmatch is None or totdist < min(bmatch.dist_indel, bmatch.dist_jw) + min(bmatch.dist_ly, bmatch.dist_deg) + min(0.05, bmatch.dist_plx):
        #         sysmatches['best_match'] = match
                        
        # name_matches.add(match)

    print(f'{len(matchlist)} rows in matches table')

    systemquery.create_tables()
    systemquery.insert_matches(matchlist)
    systemquery.commit()


def process_matches_db(systemquery: SystemQueryDatabase):
    print('Processing matches in DB')

    row_iter = list(systemquery.get_syscoords())

    rows = []
    sysaddrs = set()

    matchcount = 0

    matches_by_system = {}

    for row in row_iter:
        if row.sys_addr not in sysaddrs and len(rows) != 0 and (len(sysaddrs) % 10) == 0:
            sys.stderr.write(f'Processing {len(rows)} rows from {len(sysaddrs)} systems [{rows[0].sys_name} .. {rows[-1].sys_name}]\n')
            matches = match_simbad_syscoords(rows, systemquery)
            save_matches_db(matches, systemquery, matches_by_system)
            matchcount += len(matches)
            sys.stderr.write(f'Processed {matchcount} matches from {len(sysaddrs)} systems\n')
            rows = []

        rows.append(row)
        sysaddrs.add(row.sys_addr)

    sys.stderr.write(f'Processing {len(rows)} rows from {len(sysaddrs)} systems\n')
    matches = match_simbad_syscoords(rows, systemquery)
    save_matches_db(matches, systemquery, matches_by_system)
    matchcount += len(matches)
    sys.stderr.write(f'Processed {matchcount} matches from {len(sysaddrs)} systems\n')

    return matches_by_system


def fetch_all_simbad_basic(simbad: Simbad, dest: SystemQueryDatabase, start_oid: int, last_update: date|None):
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


def fetch_all_simbad_ident(simbad: Simbad, dest: SystemQueryDatabase, start_oid: int, last_update: date|None):
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
                space_re.sub(' ', utils.default_process(str(ident).strip().lower()))
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


def fetch_all_simbad_idents_basic(simbad: Simbad, dest: SystemQueryDatabase):
    dest.create_tables()

    last_oid, last_update, max_date, max_oid = dest.get_last_basic_oid_date()

    fetch_all_simbad_basic(simbad, dest, max_oid, None)

    if max_date is not None:
        max_date = max_date - timedelta(days=30)

        fetch_all_simbad_basic(simbad, dest, 0, max_date)

    last_oid, last_update, max_date, max_oid = dest.get_last_ident_oidref_date()

    fetch_all_simbad_ident(simbad, dest, max_oid, None)

    if max_date is not None:
        max_date = max_date - timedelta(days=30)

        fetch_all_simbad_ident(simbad, dest, 0, max_date)

    dest.commit()


def fetch_spansh_systems(systemquery: SystemQueryDatabase):
    pgsysre = re.compile('^([A-Za-z0-9.()\' -]+?) ([A-Z][A-Z]-[A-Z]) ([a-h])(?:([0-9]+)-|)([0-9]+)$', re.IGNORECASE)

    systems: set[SystemCoords] = set()

    if not os.path.exists('systems.json.gz'):
        print('Fetching systems from Spansh')

        with urllib.request.urlopen('https://downloads.spansh.co.uk/systems.json.gz') as sgz:
            with open('systems.json.gz', 'wb') as ogz:
                ogz.write(sgz.read())

    print('Processing systems from Spansh')

    i = 0

    with open('systems.json.gz', 'rb') as sgz:
        with gzip.open(sgz, 'rt', encoding='utf8') as gz:
            for line in gz:
                line = line.strip()

                if line[-1] == ',':
                    line = line[:-1]

                if line[0] != '{' or line[-1] != '}':
                    continue
                
                jline = json.loads(line)

                sys_name = str(jline['name'])
                sys_addr = int(jline['id64'])
                coords = jline['coords']
                x = float(coords['x'])
                y = float(coords['y'])
                z = float(coords['z'])

                if not pgsysre.match(sys_name) and (x, y, z) != (0, 0, 0):
                    cart = CartesianRepresentation(z, -x, y, unit=u.lightyear)
                    coord = SphericalRepresentation.from_cartesian(cart)

                    icrs = SkyCoord(coord.lon, coord.lat, coord.distance, frame=Galactic).icrs
                    fk4 = icrs.fk4
                    fk4_icrs = SkyCoord(fk4.ra, fk4.dec, fk4.distance, frame=ICRS)
                    sys_dist = coord.distance << u.lightyear
                    sys_dist = float(sys_dist / u.lightyear)
                    sys_ra = float((icrs.ra << u.deg) / u.deg)
                    sys_dec = float((icrs.dec << u.deg) / u.deg)
                    (search_radius, search_ra_range) = calc_search_radius_ra_range(sys_dist, sys_dec)

                    systems.add(SystemCoords(
                        sys_name,
                        sys_addr,
                        x,
                        y,
                        z,
                        'icrs',
                        sys_ra,
                        sys_dec,
                        sys_dist,
                        search_radius,
                        search_ra_range
                    ))

                    sys_ra = float((fk4_icrs.ra << u.deg) / u.deg)
                    sys_dec = float((fk4_icrs.dec << u.deg) / u.deg)
                    (search_radius, search_ra_range) = calc_search_radius_ra_range(sys_dist, sys_dec)

                    systems.add(SystemCoords(
                        sys_name,
                        sys_addr,
                        x,
                        y,
                        z,
                        'fk4_icrs',
                        sys_ra,
                        sys_dec,
                        sys_dist,
                        search_radius,
                        search_ra_range
                    ))

                i += 1

                if (i % 1000) == 0:
                    sys.stderr.write('.')
                    sys.stderr.flush()

                    if (i % 64000) == 0:
                        sys.stderr.write(f' {i} [{len(systems)}]\n')
                        sys.stderr.flush()

    sys.stderr.write(f' {i} [{len(systems)}]\n')
    sys.stderr.flush()

    print(f'Got {len(systems)} named systems from Spansh')

    systemquery.create_tables()
    systemquery.insert_syscoords(systems)
    systemquery.commit()


def main():
    try:
        #conn = sqlite3.connect('simbad.sqlite')
        systemquery = SystemQueryMariaDB(charset='utf8',read_default_file="~/.my.cnf",database='simbad')
        systemquery.create_tables()
        systemquery.update_ident_match_names()
        simbad = Simbad()
        #fetch_all_simbad_idents_basic(simbad, systemquery)
        #fetch_spansh_systems(systemquery)
        return process_matches_db(systemquery)
    except Exception:
        traceback.print_exc()
        pdb.post_mortem()


if __name__ == '__main__':
    main()
