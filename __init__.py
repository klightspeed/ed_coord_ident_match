import astropy
import astroquery
import traceback
import pdb
import sys
import argparse
import sqlite3
import pywikibot.config
from .systemquery import *
from .wikidata import *
from . import config


astroquery.__citation__
astropy.__citation__


__all__ = [
    'SystemQueryBase',
    'SystemQueryDatabase',
    'SystemQuerySimbad',
    'SystemQueryMariaDB',
    'SystemQuerySqlite3',
    'WikiData',
    'SystemCoords',
    'SystemXYZ',
    'SimbadMatch',
    'SimbadEntry',
    'fetch_all_simbad_idents_basic',
    'fetch_spansh_systems',
    'process_matches_db',
    'match_simbad_xyz',
    'match_simbad_coords',
]


def main():
    arg_parser = argparse.ArgumentParser(
        description='Process system matches into database'
    )

    dbopt = arg_parser.add_mutually_exclusive_group()

    dbopt.add_argument(
        '--sqlite',
        action='store_const',
        const='sqlite3',
        dest='sysquery',
        help='Use SQLite3 database for Simbad cache and match data',
    )

    dbopt.add_argument(
        '--mariadb',
        action='store_const',
        const='mariadb',
        dest='sysquery',
        help='Use MariaDB database for Simbad cache and match data',
    )

    arg_parser.add_argument(
        '--simbad-db-file',
        type=str,
        default='simbad.sqlite',
        dest='simbad_db_filename',
        help='Database file for SQLite3 database'
    )

    arg_parser.add_argument(
        '--simbad-db-name',
        type=str,
        default='simbad',
        dest='simbad_db_name',
        help='Database name for MariaDB database'
    )

    arg_parser.add_argument(
        '--simbad-db-host',
        type=str,
        default=None,
        dest='simbad_db_host',
        help='Database host for MariaDB database'
    )

    arg_parser.add_argument(
        '--simbad-db-port',
        type=int,
        default=None,
        dest='simbad_db_port',
        help='MariaDB server port'
    )

    arg_parser.add_argument(
        '--simbad-db-user',
        type=str,
        default=None,
        dest='simbad_db_user',
        help='Override MariaDB username'
    )

    arg_parser.add_argument(
        '--simbad-db-password',
        type=str,
        default=None,
        dest='simbad_db_password',
        help='Override MariaDB password'
    )

    arg_parser.add_argument(
        '--fetch-simbad',
        action='store_true',
        dest='fetch_simbad',
        help='Fetch basic and ident tables from Simbad'
    )

    arg_parser.add_argument(
        '--fetch-spansh',
        action='store_true',
        dest='fetch_spansh',
        help='Fetch and process systems in Spansh systems dump'
    )

    arg_parser.add_argument(
        '--fetch-wikidata',
        action='store_true',
        dest='fetch_wikidata',
        help='Fetch and process pre-filtered Wikidata dump'
    )

    arg_parser.add_argument(
        '--process-matches-db',
        action='store_true',
        dest='process_matches_db',
        help='Process systems from Spansh and match to Simbad objects'
    )

    arg_parser.add_argument(
        '--filter-wikidata-dump',
        type=str,
        dest='filter_wikidata_dump',
        help='Filter specified local full Wikidata dump for entities with astronomical catalog identifiers to pre-filtered Wikidata dump'
    )

    arg_parser.add_argument(
        '--cache-directory',
        type=str,
        default='.',
        dest='cache_dir',
        help='Directory in which to store data and cache files. Requires at least 20GB of space, or 30GB if using SQLite database'
    )

    args = arg_parser.parse_args()

    try:
        config.cache_dir = args.cache_dir
        pywikibot.config.base_dir = args.cache_dir

        if args.filter_wikidata_dump is not None:
            filter_wikidata_dump(args.filter_wikidata_dump, args.cache_dir)

        wiki_data = WikiData(args.cache_dir)

        if args.fetch_wikidata:
            wiki_data.process_prefiltered_dump()

        if args.fetch_simbad or args.fetch_spansh or args.process_matches_db:
            if args.sysquery == 'sqlite3':
                conn = sqlite3.connect(args.simbad_db_filename)
                sys_query = SystemQuerySqlite3(conn)
            elif args.sysquery == 'mariadb':
                sys_query = SystemQueryMariaDB(
                    charset='utf8',
                    read_default_file="~/.my.cnf",
                    database=args.simbad_db_name,
                    host=args.simbad_db_host,
                    port=args.simbad_db_port,
                    user=args.simbad_db_user,
                    password=args.simbad_db_password,
                )
            else:
                sys.stderr.write('No source specified\n')
                exit(1)

            sys_query.create_tables()

            if args.fetch_simbad:
                fetch_all_simbad_idents_basic(sys_query)

            if args.fetch_spansh:
                fetch_spansh_systems(sys_query)

            if process_matches_db:
                process_matches_db(sys_query, wiki_data)
    except Exception as e:
        traceback.print_exc()

        if sys.version_info[0] == 3 and sys.version_info[1] >= 13:
            pdb.post_mortem(e)
        else:
            pdb.post_mortem()


if __name__ == '__main__':
    main()