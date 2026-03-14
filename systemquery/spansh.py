from .interfaces import SystemQueryDatabase
from .classes import SystemCoords
import shutil
from .. import config
from ..util import pg_sys_re, calc_search_radius_ra_range
import os
import urllib.request
import gzip
import json
from astropy.coordinates import SkyCoord, CartesianRepresentation, SphericalRepresentation, Galactic, ICRS
from astropy import units as u
import sys


def fetch_spansh_systems(systemquery: SystemQueryDatabase):
    systems: set[SystemCoords] = set()

    if not os.path.exists(os.path.join(config.cache_dir, 'systems.json.gz')):
        print('Fetching systems from Spansh')

        with urllib.request.urlopen('https://downloads.spansh.co.uk/systems.json.gz') as sgz:
            with open(os.path.join(config.cache_dir, 'systems.json.gz'), 'wb') as ogz:
                shutil.copyfileobj(sgz, ogz)

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

                if not pg_sys_re.match(sys_name) and (x, y, z) != (0, 0, 0):
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
                        'icrs',
                        x,
                        y,
                        z,
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
                        'fk4_icrs',
                        x,
                        y,
                        z,
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
