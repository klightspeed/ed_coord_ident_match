from ed_coord_ident_match.systemquery.patterns import patterns
from ed_coord_ident_match.systemquery import CatQuery, MatchIdent, SimbadEntry
from astroquery.vizier import Vizier
from astropy.table import Table
from collections.abc import Iterable
import dataclasses

viz_cat_cache: dict[str, list[Table]] = {}


def get_vizier_cat(catname: str) -> Table:
    global viz_cat_cache

    vc = viz_cat_cache.get(catname)

    if vc is None:
        vizier = Vizier(row_limit=-1)
        vc = viz_cat_cache.setdefault(catname, vizier.get_catalogs(catname))

    return vc[0]


def query_cat(cat: CatQuery) -> Iterable[str|MatchIdent]:
    if cat.source == 'Vizier':
        tbl = get_vizier_cat(cat.catalogue)
        mask = None

        for n, v in cat.cat_filter.items():
            if mask is None:
                mask = tbl[n] == v
            else:
                mask &= tbl[n] == v

        for row in tbl[mask]:
            yield MatchIdent(cat.result.format(** { k: row.get(k) for k in row.keys() }), is_simbad=True, source=f'{cat.source}:{cat.catalogue}')


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
        yield source, simbad
