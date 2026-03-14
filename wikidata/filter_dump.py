import pywikibot
import pywikibot.pagegenerators
import gzip
import json
import os
import sys
from . import constants, wikiutils
from ..util import open_dump


def entry_has_ident(entity: dict, astro_cats: set[str]):
    wiki_id = entity.get('id')
    wiki_claims = entity.get('claims', {})

    if isinstance(wiki_id, str) and isinstance(wiki_claims, dict):
        for claim_simbad in wiki_claims.get(constants.P_SIMBAD_ID, []):
            if isinstance(claim_simbad, pywikibot.Claim):
                claim_simbad = claim_simbad.toJSON()

            catcode = wikiutils.get_claim_value_extid(claim_simbad)

            if catcode is None:
                continue

            return True

        for claim_catcode in wiki_claims.get(constants.P_CATCODE, []):
            if isinstance(claim_catcode, pywikibot.Claim):
                claim_catcode = claim_catcode.toJSON()

            catcode = wikiutils.get_claim_value_string(claim_catcode)

            if catcode is None:
                continue

            for catalog in claim_catcode.get('qualifiers', {}).get(constants.P_CATALOG, []):
                catalog_id = wikiutils.get_snak_entity_id(catalog)

                if catalog_id in astro_cats:
                    return True

            if constants.Q_SIMBAD in wikiutils.get_claim_stated_in(claim_catcode):
                return True

    return False


def filter_wikidata_dump(filename: str, out_dir: str):
    site = pywikibot.Site("wikidata", "wikidata")

    astro_cats = set()

    sys.stderr.write('Retrieving astronomical catalogues\n')
    for item in wikiutils.get_astronomical_catalogues(site):
        astro_cats.add(item.title())

    sys.stderr.write('Processing wikidata dump\n')
    ent_num = 0
    n_with_ident = 0

    with gzip.open(os.path.join(out_dir, 'wikidata-astro-with-ident.jsonl.gz.tmp'), 'wt', encoding='utf-8') as f_with_ident, \
         open_dump(filename) as f:
        for line in f:
            line = line.strip()

            if len(line) > 2 and line[0] == '{':
                if line[-1] == ',':
                    line = line[:-1]

                j = json.loads(line)

                if entry_has_ident(j, astro_cats):
                    f_with_ident.write(line + '\n')
                    n_with_ident += 1

            ent_num += 1

            if (ent_num % 1000) == 0:
                sys.stderr.write('.')
                sys.stderr.flush()

                if (ent_num % 64000) == 0:
                    sys.stderr.write(f' {ent_num} [with_ident={n_with_ident}]\n')

    sys.stderr.write(f' {ent_num} [with_ident={n_with_ident}]\n')
    os.rename(
        os.path.join(out_dir, 'wikidata-astro-with-ident.jsonl.gz.tmp'),
        os.path.join(out_dir, 'wikidata-astro-with-ident.jsonl.gz')
    )
