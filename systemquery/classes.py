from dataclasses import dataclass
from frozendict import frozendict
from astropy import units as u
from typing import NamedTuple
from datetime import date


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
    is_simbad: bool = False
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


class SimbadOidDate(NamedTuple):
    last_oid: int
    last_date: date|None
    max_date: date|None
    max_oid: int|None
