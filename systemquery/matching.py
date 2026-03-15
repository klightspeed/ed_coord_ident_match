from astropy import units as u
from astropy.coordinates import SkyCoord, CartesianRepresentation, SphericalRepresentation, Galactic, ICRS, angular_separation
from rapidfuzz.distance import JaroWinkler, DamerauLevenshtein, Hamming, Indel, Levenshtein
from collections.abc import Collection, Iterable
import dataclasses
import itertools
import sys
from ..util import filter_match_name, space_dash_re
from .classes import MatchIdent, SimbadEntry, SimbadMatch, SimbadDBMatch, SimbadTableMatch, SystemCoords, SystemXYZ
from .interfaces import SystemQueryBase, SystemQueryDatabase
from .match_name import get_match_names, get_rev_matches
from .renamed_systems import get_ed_renamed_systems
from .known_systems import get_ed_known_systems
from ..wikidata import WikiData


def add_fuzz_distances(match: SimbadMatch, simbad: SimbadEntry, name: str, is_alt_name: bool = False, source: str|None = None) -> SimbadMatch:
    dist_deg = angular_separation(match.sys_ra, match.sys_dec, simbad.ra, simbad.dec) << u.deg
    dist_ly = float((dist_deg << u.radian) * match.sys_dist / u.lightyear / u.radian)
    dist_deg = float(dist_deg / u.deg)
    lident = space_dash_re.sub(' ', simbad.ident.strip().lower())
    xident = filter_match_name(simbad.ident)
    lname = space_dash_re.sub(' ', name.strip().lower())
    xname = filter_match_name(name)

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


def filter_matches(sy_matches: set[SimbadMatch], allow_deg_match: bool = False) -> tuple[bool, set[SimbadMatch]]:
    if any(m.dist_ly < 0.1 and m.dist_indel == 0 and not m.is_alt_name for m in sy_matches):
        return True, set((m for m in sy_matches if m.dist_ly < 0.1 and m.dist_indel == 0 and not m.is_alt_name))
    elif any(m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10 and not m.is_alt_name for m in sy_matches):
        return True, set((m for m in sy_matches if ((m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10) or m.dist_indel == 0) and not m.is_alt_name))
    elif allow_deg_match and any(m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10 and not m.is_alt_name for m in sy_matches):
        return True, set((m for m in sy_matches if (m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and m.dist_plx is not None and m.dist_plx < 10) or m.dist_indel == 0 or m.dist_ly < 0.1))
    elif any(m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
        return True, set((m for m in sy_matches if ((m.dist_ly < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1) or m.dist_indel == 0) and not m.is_alt_name))
    elif allow_deg_match and any(m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name for m in sy_matches):
        return True, set((m for m in sy_matches if (m.dist_deg < 0.1 and min(m.dist_indel, m.dist_jw) < 0.1) or m.dist_indel == 0 or m.dist_ly < 0.1))
    return False, sy_matches


def process_matches(rows: Iterable[SimbadTableMatch|Iterable],
                    idents: dict[str, set[SimbadEntry]],
                    systemquery: SystemQueryBase,
                    wikidata: WikiData
                   ) -> set[SimbadMatch]:
    matches = set()
    base_matches = {}
    match_counts = {}

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
        sys_mcounts = match_counts.setdefault((sy_name, sy_addr), set())
        sy_matches = sys_matches.get(entry)

        names = get_match_names(sy_name)

        for name in get_ed_known_systems(sy_name):
            names.add(MatchIdent(name, is_alt_name=True, source='known_systems'))

        for name in get_ed_known_systems(sy_addr):
            names.add(MatchIdent(name, is_alt_name=True, source='known_systems'))

        for name in get_ed_renamed_systems(sy_addr):
            names.add(MatchIdent(name, source='renamed_systems'))

        if sy_matches is None:
            sy_matches = sys_matches.setdefault(entry, {})

            for name in names:
                is_alt_name = False
                source = None

                if isinstance(name, MatchIdent):
                    is_alt_name = name.is_alt_name
                    source = name.source
                    name = name.ident

                for ident in idents.get(filter_match_name(name), []):
                    sy_matches.setdefault((name, is_alt_name, source), set()).add(ident)
                    sys_mcounts.add((name, is_alt_name, source, ident))

        if sb_oid is not None:
            sb_entry = SimbadEntry(
                sb_oid,
                sb_main_id,
                sb_ident,
                sb_ra << u.deg,
                sb_dec << u.deg,
                sb_plx << u.mas if sb_plx is not None else None
            )

            lident = space_dash_re.sub(' ', sb_ident.lower())

            for name in names:
                max_dist = 1.0
                is_alt_name = False
                source = None

                if isinstance(name, MatchIdent):
                    max_dist = name.maxdist
                    is_alt_name = name.is_alt_name
                    source = name.source
                    name = name.ident

                lname = space_dash_re.sub(' ', name.lower())

                dist_indel = Indel.normalized_distance(lname, lident)

                if dist_indel > max_dist:
                    continue

                sy_matches.setdefault((name, is_alt_name, source), set()).add(sb_entry)
                sys_mcounts.add((name, is_alt_name, source, sb_entry))

    for na, entries in base_matches.items():
        sy_matches = set()
        sb_entries = set()
        sy_names = {}

        sys_mcounts = match_counts.get(na, set())

        sys.stderr.write(f'Checking system {na[0]} [{na[1]}] ({len(sys_mcounts)} permutations to check)\n')

        for entry, names in entries.items():
            for (name, is_alt_name, source), nmatches in names.items():
                sy_names.setdefault(name, set()).add((is_alt_name, False, source))

                for sb_entry in nmatches:
                    sb_entries.add(sb_entry)
                    sy_names.setdefault(sb_entry.ident, set()).add((is_alt_name, True, source))

                    for src, sb_sub in get_rev_matches(source, sb_entry):
                        sy_names.setdefault(sb_sub.ident, set()).add((is_alt_name, True, src))
                        sy_matches.add(add_fuzz_distances(entry, sb_sub, name, is_alt_name, src))

        is_match, sy_matches = filter_matches(sy_matches)

        if not is_match:
            wiki_names = {}

            for sy_name, sources in sy_names.items():
                sys.stderr.write(f'Querying Wikidata for {sy_name}\n')

                if any(not is_alt_name for is_alt_name, _, _ in sources):
                    sources = set(((is_alt_name, is_simbad, source) for is_alt_name, is_simbad, source in sources if not is_alt_name))

                if any(not is_simbad for _, is_simbad, _ in sources):
                    for wiki_simbad, wiki_item_id, wiki_alias in wikidata.search_entities_by_name(sy_name):
                        for name in get_match_names(wiki_simbad):
                            if isinstance(name, MatchIdent):
                                name = name.ident

                            wiki_sources = wiki_names.setdefault((name, wiki_item_id, wiki_alias), set())

                            for is_alt_name, is_simbad, source in sources:
                                wiki_sources.add((is_alt_name, is_simbad, source))

                if any(is_simbad for _, is_simbad, _ in sources):
                    for wiki_simbad, wiki_item_id, wiki_alias in wikidata.search_entities_by_ident(sy_name):
                        for name in get_match_names(wiki_simbad):
                            if isinstance(name, MatchIdent):
                                name = name.ident

                            wiki_sources = wiki_names.setdefault((name, wiki_item_id, wiki_alias), set())

                            for is_alt_name, is_simbad, source in sources:
                                wiki_sources.add((is_alt_name, is_simbad, source))

            if len(wiki_names) > 0:
                sys.stderr.write(f'Checking {len(wiki_names)} names from Wikidata')
                w_names = set()
                w_aliases = {}

                for (wiki_simbad, wiki_item_id, wiki_alias), wiki_sources in wiki_names.items():
                    for check_name, sources in sy_names.items():
                        if any((not is_simbad for _, is_simbad, _ in sources)):
                            name = f'NAME {check_name}'
                            ident = f'NAME {wiki_alias}'
                            lname = space_dash_re.sub(' ', name.lower())
                            lident = space_dash_re.sub(' ', ident.lower())
                            dist_indel = Indel.normalized_distance(lname, lident)

                            if dist_indel < 0.2:
                                if isinstance(wiki_simbad, MatchIdent):
                                    wiki_simbad = wiki_simbad.ident

                                w_names.add(wiki_simbad)

                                w_by_name = w_aliases.setdefault(filter_match_name(wiki_simbad), {})
                                w_sources = w_by_name.setdefault((wiki_simbad, wiki_item_id, wiki_alias, check_name), set())

                                for is_alt_name, is_simbad, source in sources:
                                    if not is_simbad:
                                        w_sources.add((is_alt_name, source))

                                for is_alt_name, is_simbad, source in wiki_sources:
                                    if not is_simbad:
                                        w_sources.add((is_alt_name, source))

                sys.stderr.write(f'Checking {len(w_names)} idents from Wikidata')

                wiki_idents = systemquery.query_idents(w_names)

                for _, ident_entries in wiki_idents.items():
                    for sb_entry in ident_entries:
                        sb_entries.add(sb_entry)

                for entry, _ in entries.items():
                    for sb_entry in sb_entries:
                        w_by_name = w_aliases.get(filter_match_name(sb_entry.ident))

                        if w_by_name is None:
                            continue

                        lident = space_dash_re.sub(' ', sb_entry.ident.lower())

                        for (name, item_id, alias, check_name), sources in w_by_name.items():
                            if any(not is_alt_name for is_alt_name, _ in sources):
                                sources = set(((is_alt_name, source) for is_alt_name, source in sources if not is_alt_name))

                            lname = space_dash_re.sub(' ', name.lower())

                            if lname == lident:
                                name = f'NAME {check_name}'
                                ident = f'NAME {alias}'
                                sb_sub = dataclasses.replace(sb_entry, ident=ident)

                                for name_is_alt_name, name_source in sources:
                                    source = f'wikidata({item_id}/alias={alias})'

                                    if name_source is not None:
                                        source = name_source + ' -> ' + source

                                    sy_matches.add(add_fuzz_distances(entry, sb_sub, name, name_is_alt_name, source))

                is_match, sy_matches = filter_matches(sy_matches, True)

        min_dist_deg = min([m.dist_deg for m in sy_matches if min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name] or [None])
        min_dist_ly = min([m.dist_ly for m in sy_matches if min(m.dist_indel, m.dist_jw) < 0.1 and not m.is_alt_name] or [None])
        min_exact_dist_deg = min([m.dist_deg for m in sy_matches if min(m.dist_indel, m.dist_jw) == 0 and not m.is_alt_name] or [None])
        by_dist = sorted((m for m in sy_matches if min(m.dist_indel, m.dist_jw) < 0.2 and min(m.dist_ly, m.dist_deg) < 1 and not m.is_alt_name), key=lambda m: min(m.dist_ly, m.dist_deg) + min(m.dist_indel, m.dist_jw))

        if min_dist_deg == min_exact_dist_deg:
            sys.stderr.write(f'System {na[0]} [{na[1]}] name match, dist_deg={min_dist_deg}, dist_ly={min_dist_ly}\n')
        elif min_dist_deg is not None:
            sys.stderr.write(f'System {na[0]} [{na[1]}] fuzzy name match, dist_deg={min_dist_deg}, dist_ly={min_dist_ly}\n')
        elif len(by_dist) > 0:
            sys.stderr.write(f'System {na[0]} [{na[1]}] no name match, nearest={by_dist[0].simbad.ident}, dist_deg={by_dist[0].dist_deg}, dist_ly={by_dist[0].dist_ly}\n')
        else:
            sys.stderr.write(f'System {na[0]} [{na[1]}] no fuzzy matches\n')

        sy_matches = set((next(iter(sorted(g, key=lambda m: m.dist_deg))) for _, g in itertools.groupby(sy_matches, key=lambda m: (m.simbad, m.matched_name, m.match_source))))

        for match in sy_matches:
            matches.add(match)

    return matches


def match_simbad_coords(matches: Collection[SimbadMatch],
                        systemquery: SystemQueryBase,
                        wikidata: WikiData
                       ) -> set[SimbadMatch]:
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

        for name in get_ed_renamed_systems(match.sys_addr):
            idents.add(name)

    idents = systemquery.query_idents(idents)

    result_table = systemquery.query_coords(matches)

    return process_matches(result_table, idents, systemquery, wikidata)


def match_simbad_coords_chunked(matches: Collection[SimbadMatch],
                                systemquery: SystemQueryBase,
                                wikidata: WikiData
                                ) -> set[SimbadMatch]:
    results: set[SimbadMatch] = set()

    for grp in itertools.batched(matches, 1000):
        for result in match_simbad_coords(grp, systemquery, wikidata):
            results.add(result)

    return results


def match_simbad_xyz(systems: Collection[SystemXYZ],
                     systemquery: SystemQueryBase,
                     wikidata: WikiData
                     ) -> set[SimbadMatch]:
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

        coords.add(SimbadMatch(
            name,
            sysaddr,
            'icrs',
            u.Quantity(icrs.ra),
            u.Quantity(icrs.dec),
            coord.distance
        ))

        coords.add(SimbadMatch(
            name,
            sysaddr,
            'fk4_icrs',
            u.Quantity(fk4_icrs.ra),
            u.Quantity(fk4_icrs.dec),
            coord.distance
        ))

    return match_simbad_coords(coords, systemquery, wikidata)


def match_simbad_xyz_chunked(systems: Collection[SystemXYZ],
                             systemquery: SystemQueryBase,
                             wikidata: WikiData
                            ) -> set[SimbadMatch]:
    results: set[SimbadMatch] = set()

    for grp in itertools.batched(systems, 1000):
        for result in match_simbad_xyz(grp, systemquery, wikidata):
            results.add(result)

    return results


def match_simbad_syscoords(systems: Collection[SystemCoords],
                           systemquery: SystemQueryBase,
                           wikidata: WikiData
                          ) -> set[SimbadMatch]:
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

    return match_simbad_coords(coords, systemquery, wikidata)


def save_matches_db(matches: set[SimbadMatch],
                    systemquery: SystemQueryDatabase
                   ):
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

    print(f'{len(matchlist)} rows in matches table')

    systemquery.create_tables()
    systemquery.insert_matches(matchlist)
    systemquery.commit()


def process_matches_db(systemquery: SystemQueryDatabase,
                       wikidata: WikiData
                      ):
    print('Processing matches in DB')

    row_iter = list(systemquery.get_syscoords())

    rows = []
    sysaddrs = set()
    already_processed = set()
    matchcount = 0

    for row in systemquery.get_processed_matches():
        already_processed.add(row.sys_addr)
        sysaddrs.add(row.sys_addr)
        matchcount += 1

    for row in row_iter:
        if row.sys_addr not in already_processed:
            if row.sys_addr not in sysaddrs and len(rows) != 0 and (len(sysaddrs) % 10) == 0:
                sys.stderr.write(f'Processing {len(rows)} rows from {len(sysaddrs)} systems [{rows[0].sys_name} .. {rows[-1].sys_name}]\n')
                matches = match_simbad_syscoords(rows, systemquery, wikidata)
                sys.stderr.write(f'Saving {len(matches)} matches from {len(sysaddrs)} systems\n')
                save_matches_db(matches, systemquery)
                matchcount += len(matches)
                sys.stderr.write(f'Processed {matchcount} matches from {len(sysaddrs)} systems\n')
                rows = []

            rows.append(row)
            sysaddrs.add(row.sys_addr)

    sys.stderr.write(f'Processing {len(rows)} rows from {len(sysaddrs)} systems\n')
    matches = match_simbad_syscoords(rows, systemquery, wikidata)
    save_matches_db(matches, systemquery)
    matchcount += len(matches)
    sys.stderr.write(f'Processed {matchcount} matches from {len(sysaddrs)} systems\n')


