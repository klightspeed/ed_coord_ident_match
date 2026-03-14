import unicodedata
import re
import rapidfuzz.utils
from astropy import units as u
import math
import bz2
import gzip


renamed_systems_sheet_uri = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS2Q8f9tWZIJz5S1z6Fv1pfNgFxywIfyZVJGkGFvQ4TZ9Si8UZ8GkGnuiMo8SZgB27tTchO3rCqA0fx/pub?output=tsv'

max_dist_ly = 0.1 * u.lightyear
max_dist_deg = (6 * u.arcmin) << u.deg

space_dash_re = re.compile('  +|-')
ident_re = re.compile(r'^[0-9A-Za-z\[\]_#* +-]+$')
space_re = re.compile('  +')
num_re = re.compile('[0-9+-]')
pg_sys_re = re.compile('^([A-Za-z0-9.()\' -]+?) ([A-Z][A-Z]-[A-Z]) ([a-h])(?:([0-9]+)-|)([0-9]+)$', re.IGNORECASE)

renamed_systems: dict[str|int, set[str]] | None = None


def calc_search_radius_ra_range(sys_dist: float|u.Quantity[u.lightyear], sys_dec: float|u.Quantity[u.deg]) -> tuple[float, float]:
    search_radius = float(max(max_dist_deg, (max_dist_ly * u.radian / (sys_dist << u.lightyear)) << u.deg) / u.deg)
    sys_dec = float((sys_dec << u.deg) / u.deg)
    cos_dec_search = math.cos(math.radians(abs(sys_dec) + search_radius))

    if cos_dec_search < search_radius / 360:
        search_ra_range = 360.0
    else:
        search_ra_range = search_radius / math.cos(math.radians(abs(sys_dec) + search_radius))

    return search_radius, search_ra_range


def filter_match_name(name: str) -> str:
    normalized = unicodedata.normalize('NFKD', name)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) not in ['Mn', 'Mc'])
    return space_dash_re.sub(' ', rapidfuzz.utils.default_process(normalized.strip().lower()))


def open_dump(filename: str):
    if filename.endswith('.json.bz2') or filename.endswith('.jsonl.bz2'):
        return bz2.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.json.gz') or filename.endswith('.jsonl.gz'):
        return gzip.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.json') or filename.endswith('.jsonl'):
        return open(filename, 'rt', encoding='utf-8')
    raise ValueError()
