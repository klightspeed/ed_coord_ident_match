from ..util import filter_match_name, open_dump, ident_re, space_re
import os.path
import sqlite3
import pywikibot
import pywikibot.site
import pywikibot.config
import pywikibot.pagegenerators
import sys
import logging
from collections.abc import Iterator
import json
from . import wikiutils, dbutils, constants
from .classes import WikiDataEntry, WikiDataIdent, WikiDataAliasInfo
from time import sleep
import random
import urllib.request
import shutil

logger = logging.getLogger(__name__)


PREFILTERED_WIKIDATA_URL = 'https://edgalaxydata.space/wikidata/2026-03-03/wikidata-astro-with-ident.jsonl.gz'


class WikiData:
    site: pywikibot.site.BaseSite
    astro_types: set[str] | None = None
    astro_cats: set[str] | None = None
    cache_dir: str
    use_astro: bool = False
    conn: sqlite3.Connection|None = None

    def __init__(self, cache_dir: str|None = None):
        self.site = pywikibot.Site("wikidata", "wikidata")
        self.astro_types = None
        self.astro_cats = None
        self.cache_dir = cache_dir

    def process_wikidata_entity(self, entity: dict) -> WikiDataEntry|None:
        if self.astro_types is None:
            self.astro_types = set()

            sys.stderr.write('Retrieving astronomical object types\n')
            for item in wikiutils.get_astronomical_object_types(self.site):
                self.astro_types.add(item.title())

        if self.astro_cats is None:
            self.astro_cats = set()

            sys.stderr.write('Retrieving astronomical catalogues\n')
            for item in wikiutils.get_astronomical_catalogues(self.site):
                self.astro_cats.add(item.title())

        wiki_id = entity.get('id')
        wiki_labels = entity.get('labels', {})
        wiki_aliases = entity.get('aliases', {})
        wiki_claims = entity.get('claims', {})

        if isinstance(wiki_id, str) and isinstance(wiki_labels, dict) and isinstance(wiki_aliases, dict) and isinstance(wiki_claims, dict):
            aliases: dict[str, WikiDataAliasInfo] = {}
            idents: dict[str, WikiDataAliasInfo] = {}
            ent_data: WikiDataEntry = {
                'id': wiki_id,
                'labels': {k: v['value'] for k, v in wiki_labels.items()},
                'lang_aliases': {k: [vv['value'] for vv in v] for k, v in wiki_aliases.items()},
                'types': [],
                'idents': [],
                'aliases': [],
                'simbad_idents': [],
                'coords': {}
            }

            for lang, label in wiki_labels.items():
                label = label.get('value')
                if label is not None:
                    aliases.setdefault(label, {'name': label}).setdefault('langs', []).append(lang)
            
            for lang, alias_items in wiki_aliases.items():
                for label in alias_items:
                    label = label.get('value')
                    if label is not None:
                        aliases.setdefault(label, {'name': label}).setdefault('langs', []).append(lang)

            for claim_ra in wiki_claims.get(constants.P_RA, []):
                if isinstance(claim_ra, pywikibot.Claim):
                    claim_ra = claim_ra.toJSON()

                ra = wikiutils.get_claim_value_degrees(claim_ra)

                if ra is None:
                    continue

                for reference in wikiutils.get_claim_stated_in(claim_ra):
                    ent_data['coords'].setdefault(reference, {})['ra'] = ra

            for claim_dec in wiki_claims.get(constants.P_DEC, []):
                if isinstance(claim_dec, pywikibot.Claim):
                    claim_dec = claim_dec.toJSON()

                dec = wikiutils.get_claim_value_degrees(claim_dec)

                if dec is None:
                    continue

                for reference in wikiutils.get_claim_stated_in(claim_dec):
                    ent_data['coords'].setdefault(reference, {})['dec'] = dec

            for claim_simbad in wiki_claims.get(constants.P_SIMBAD_ID, []):
                if isinstance(claim_simbad, pywikibot.Claim):
                    claim_simbad = claim_simbad.toJSON()

                catcode = wikiutils.get_claim_value_extid(claim_simbad)

                if catcode is None:
                    continue

                aliases.setdefault(catcode, {'name': catcode})['simbad'] = True
                idents.setdefault(catcode, {'name': catcode})['simbad'] = True
                ent_data['simbad_idents'].append(catcode)

            for claim_instance_of in wiki_claims.get(constants.P_INSTANCE_OF, []):
                if isinstance(claim_instance_of, pywikibot.Claim):
                    claim_instance_of = claim_instance_of.toJSON()

                instance_of_id = wikiutils.get_claim_entity_id(claim_instance_of)

                if instance_of_id in self.astro_types:
                    ent_data['types'].append(instance_of_id)

            for claim_catcode in wiki_claims.get(constants.P_CATCODE, []):
                if isinstance(claim_catcode, pywikibot.Claim):
                    claim_catcode = claim_catcode.toJSON()

                catcode = wikiutils.get_claim_value_string(claim_catcode)

                if catcode is None:
                    continue

                aliases.setdefault(catcode, {'name': catcode})

                for catalog in claim_catcode.get('qualifiers', {}).get(constants.P_CATALOG, []):
                    catalog_id = wikiutils.get_snak_entity_id(catalog)

                    if catalog_id in self.astro_cats:
                        aliases.setdefault(catcode, {'name': catcode}).setdefault('cats', []).append(catalog_id)
                        idents.setdefault(catcode, {'name': catcode}).setdefault('cats', []).append(catalog_id)

                if constants.Q_SIMBAD in wikiutils.get_claim_stated_in(claim_catcode):
                    aliases.setdefault(catcode, {'name': catcode})['simbad'] = True
                    idents.setdefault(catcode, {'name': catcode})['simbad'] = True
            
            if len(idents) > 0:
                ent_data['idents'] = list(idents.values())
                ent_data['aliases'] = list(aliases.values())

                return ent_data
                
        return None

    def process_wikidata_dump(self, filename: str):
        with sqlite3.connect(os.path.join(self.cache_dir, "wikidata-astro.sqlite")) as conn:
            dbutils.create_tables(conn)
    
            sys.stderr.write('Processing wikidata dump\n')
            entnum = 0
            n_with_ident = 0
            n_aliases = 0
            n_idents = 0
            save_aliases = []
            save_idents = []
    
            with open_dump(filename) as f:
                for line in f:
                    line = line.strip()
    
                    if len(line) > 2 and line[0] == '{':
                        if line[-1] == ',':
                            line = line[:-1]
    
                        j = json.loads(line)
    
                        entry = self.process_wikidata_entity(j)
    
                        if entry is not None:
                            wiki_id = entry['id']
                            n_with_ident += 1
    
                            for alias_info in entry['aliases']:
                                alias = alias_info['name']
                                info = dict(alias_info)
                                del info['name']
                                match_name = filter_match_name(alias)
                                save_aliases.append((wiki_id, alias, json.dumps(info), match_name))
                            
                            for ident_info in entry['idents']:
                                ident = ident_info['name']
                                info = dict(ident_info)
                                del info['name']
                                save_idents.append((wiki_id, ident, json.dumps(info)))
    
                            n_aliases += len(entry['aliases'])
                            n_idents += len(entry['idents'])
    
                        entnum += 1
    
                        if (entnum % 1000) == 0:
                            sys.stderr.write('.')
                            sys.stderr.flush()
    
                            cursor = conn.cursor()
                            cursor.executemany('INSERT OR REPLACE INTO wikidata_aliases (item_id, alias, aliasinfo, match_name) VALUES (?, ?, ?, ?)', save_aliases)
    
                            cursor = conn.cursor()
                            cursor.executemany('INSERT OR REPLACE INTO wikidata_simbad (item_id, ident, identinfo) VALUES (?, ?, ?)', save_idents)
    
                            conn.commit()
    
                            save_aliases = []
                            save_idents = []
    
                            if (entnum % 64000) == 0:
                                sys.stderr.write(f' {entnum} [with_ident={n_with_ident}; aliases={n_aliases}; idents={n_idents}]\n')
    
            sys.stderr.write(f' {entnum} [with_ident={n_with_ident}; aliases={n_aliases}; idents={n_idents}]\n')
    
            cursor = conn.cursor()
            cursor.executemany('INSERT OR REPLACE INTO wikidata_aliases (item_id, alias, aliasinfo, match_name) VALUES (?, ?, ?, ?)', save_aliases)
    
            cursor = conn.cursor()
            cursor.executemany('INSERT OR REPLACE INTO wikidata_simbad (item_id, ident, ident_info) VALUES (?, ?, ?)', save_idents)
    
            conn.commit()

    def add_item(self, item_id: str, conn: sqlite3.Connection):
        sys.stderr.write(f'Fetching wikidata item {item_id}\n')
        item = pywikibot.ItemPage(self.site, item_id)
        delay = 15

        while True:
            try:
                item_data = item.get()
                break
            except Exception as e:
                sys.stderr.write(f'Error retrieving item: {e}\n')
                sleep(delay * random.uniform(0, 15))
                delay = min(delay * 2, 120)
                sys.stderr.write('Retrying\n')

        if item_data is None:
            return

        entry = self.process_wikidata_entity(item_data)

        if entry is not None:
            save_aliases = []
            save_idents = []

            wiki_id = entry['id']

            for alias_info in entry['aliases']:
                alias = alias_info['name']
                info = dict(alias_info)
                del info['name']
                match_name = filter_match_name(alias)
                save_aliases.append((wiki_id, alias, json.dumps(info), match_name))
            
            for ident_info in entry['idents']:
                ident = ident_info['name']
                info = dict(ident_info)
                del info['name']
                save_idents.append((wiki_id, ident, json.dumps(info)))

            cursor = conn.cursor()
            cursor.executemany('INSERT OR REPLACE INTO wikidata_aliases (item_id, alias, aliasinfo, match_name) VALUES (?, ?, ?, ?)', save_aliases)

            cursor = conn.cursor()
            cursor.executemany('INSERT OR REPLACE INTO wikidata_simbad (item_id, ident, ident_info) VALUES (?, ?, ?)', save_idents)

            conn.commit()

    def add_items(self, item_ids: list[str|None], conn: sqlite3.Connection):
        cursor = conn.cursor()
        cursor.execute(
            '''
                SELECT DISTINCT item_id
                FROM wikidata_aliases
                WHERE item_id IN (
                    SELECT value
                    FROM JSON_EACH(?)
                )
            ''',
            (json.dumps(list(item_ids)), )
        )

        fetched_items = set((v for v, in cursor))

        for item_id in item_ids:
            if item_id is not None and item_id not in fetched_items:
                self.add_item(item_id, conn)

    def search_entity_ids_by_ident(self, name: str) -> Iterator[pywikibot.ItemPage]:
        if not ident_re.match(name):
            return iter([])

        query = f'''
        SELECT ?item WHERE {{
            ?item p:P528 ?catname .
            ?catname (ps:P528) "{space_re.sub(' ', name)}" .
        }}
        '''

        return pywikibot.pagegenerators.WikidataSPARQLPageGenerator(query, site=self.site)

    def get_cache_connection(self) -> tuple[bool, sqlite3.Connection]:
        if self.conn is None:
            if os.path.exists('wikidata-astro.sqlite'):
                self.conn = sqlite3.connect('wikidata-astro.sqlite')
                self.use_astro = True
            else:
                self.conn = sqlite3.connect('wikidata.sqlite')
                self.use_astro = False

        return self.use_astro, self.conn

    def search_entities_by_ident(self, name: str) -> set[WikiDataIdent]:
        use_astro, conn = self.get_cache_connection()

        if use_astro:
            return dbutils.get_entities_by_ident(name, conn)

        sys.stderr.write(f'Querying Wikidata for ident {name}\n')

        dbutils.create_tables(conn)

        cursor = conn.cursor()
        cursor.execute('SELECT item_id FROM wikidata_idents WHERE source = ?', (f'cat:{name}',))
        item_ids = set((item_id for item_id, in cursor))

        if len(item_ids) == 0:
            entries = dbutils.get_entities_by_ident(name, conn)

            if len(entries) > 0:
                return entries

            sys.stderr.write(f'Fetching wikidata item ids for name {name}\n')
            add_idents = []

            delay = 15

            while True:
                try:
                    for item in self.search_entity_ids_by_ident(name):
                        item_id = item.title()
                        add_idents.append((f'cat:{name}', item_id))
                        item_ids.add(item_id)
                    break
                except Exception as e:
                    sys.stderr.write(f'Error executing query: {e}\n')
                    sleep(delay * random.uniform(0, 15))
                    delay = min(delay * 2, 120)
                    sys.stderr.write('Retrying\n')

            sys.stderr.write(f'Got {len(item_ids)} items for name {name}\n')

            if len(item_ids) == 0:
                item_ids.add(None)
                add_idents.append((f'cat:{name}', None))

            cursor = conn.cursor()
            cursor.executemany('INSERT INTO wikidata_idents (source, item_id) VALUES (?, ?)', add_idents)
            conn.commit()

        self.add_items([item_id for item_id in item_ids if item_id is not None], conn)

        return dbutils.get_entities_by_ident(name, conn)

    def search_entities_by_name(self, name: str) -> set[WikiDataIdent]:
        use_astro, conn = self.get_cache_connection()

        if use_astro:
            return dbutils.get_entities_by_name(name, conn)

        sys.stderr.write(f'Querying Wikidata for name {name}\n')

        dbutils.create_tables(conn)

        cursor = conn.cursor()
        cursor.execute('SELECT item_id FROM wikidata_idents WHERE source = ?', (f'search:{name}',))
        item_ids = set((item_id for item_id, in cursor))

        if len(item_ids) == 0:
            add_idents = set()

            delay = 15

            while True:
                try:
                    for result in self.site.search_entities(name, 'mul', total=50):
                        item_id = result['id']
                        add_idents.add((f'search:{name}', item_id))
                        item_ids.add(item_id)

                    break
                except Exception as e:
                    sys.stderr.write(f'Error executing search: {e}\n')
                    sleep(delay * random.uniform(0, 15))
                    delay = min(delay * 2, 120)
                    sys.stderr.write('Retrying\n')

            if len(item_ids) == 0:
                item_ids.add(None)
                add_idents.add((f'cat:{name}', None))

            cursor = conn.cursor()
            cursor.executemany('INSERT INTO wikidata_idents (source, item_id) VALUES (?, ?)', list(add_idents))
            conn.commit()

        self.add_items([item_id for item_id in item_ids if item_id is not None], conn)

        return dbutils.get_entities_by_name(name, conn)

    def process_prefiltered_dump(self):
        if not os.path.exists(os.path.join(self.cache_dir, 'wikidata-astro-with-ident.jsonl.gz')):
            print('Fetching pre-filtered WikiData dump')

            with urllib.request.urlopen(PREFILTERED_WIKIDATA_URL) as sgz:
                with open(os.path.join(self.cache_dir, 'wikidata-astro-with-ident.jsonl.gz.tmp'), 'wb') as ogz:
                    shutil.copyfileobj(sgz, ogz)
            
            os.rename(
                os.path.join(self.cache_dir, 'wikidata-astro-with-ident.jsonl.gz.tmp'),
                os.path.join(self.cache_dir, 'wikidata-astro-with-ident.jsonl.gz')
            )
        
        self.process_wikidata_dump(os.path.join(self.cache_dir, 'wikidata-astro-with-ident.jsonl.gz'))
