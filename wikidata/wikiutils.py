from .constants import P_STATED_IN
import pywikibot
import pywikibot.site
import pywikibot.pagegenerators
from collections.abc import Iterator

def get_snak_entity_id(snak: dict) -> str|None:
    if snak.get('datatype') != 'wikibase-item':
        return None

    datavalue = snak.get('datavalue', {})

    if datavalue.get('type') != 'wikibase-entityid':
        return None

    value = datavalue.get('value')

    if value.get('entity-type') != 'item':
        return None

    return value.get('id', f'Q{value.get('numeric-id')}')


def get_snak_value_string(snak: dict) -> str|None:
    if snak.get('datatype') != 'string':
        return None
    
    return snak.get('datavalue', {}).get('value')


def get_snak_value_extid(snak: dict) -> str|None:
    if snak.get('datatype') not in ('string', 'external-id'):
        return None
    
    return snak.get('datavalue', {}).get('value')


def get_snak_value_degrees(snak: dict) -> str|None:
    if snak.get('datatype') != 'quantity':
        return None
    
    dataval = snak.get('datavalue', {})

    if dataval.get('type') != 'quantity':
        return None
    
    value = dataval.get('value', {})

    # noinspection HttpUrlsUsage
    if value.get('unit') != 'http://www.wikidata.org/entity/Q28390':
        return None
    
    return value.get('amount')


def get_claim_entity_id(claim: dict) -> str|None:
    return get_snak_entity_id(claim.get('mainsnak', {}))


def get_claim_value_string(claim: dict) -> str|None:
    return get_snak_value_string(claim.get('mainsnak', {}))


def get_claim_value_extid(claim: dict) -> str|None:
    return get_snak_value_extid(claim.get('mainsnak', {}))


def get_claim_value_degrees(claim: dict) -> str|None:
    return get_snak_value_degrees(claim.get('mainsnak', {}))


def get_claim_stated_in(claim: dict) -> list[str]:
    stated_in = []

    for reference in claim.get('references', []):
        for stated_in_snak in reference.get('snaks', {}).get(P_STATED_IN, []):
            stated_in_id = get_snak_entity_id(stated_in_snak)

            if stated_in_id is not None:
                stated_in.append(stated_in_id)
    
    return stated_in


def get_astronomical_object_types(site: pywikibot.site.BaseSite) -> Iterator[pywikibot.ItemPage]:
    query = '''
        SELECT DISTINCT ?item WHERE {
        ?item p:P279 ?statement.
        ?statement (ps:P279/(wdt:P279*)) wd:Q6999.
        }
    '''

    return pywikibot.pagegenerators.WikidataSPARQLPageGenerator(query, site=site)


def get_astronomical_catalogues(site: pywikibot.site.BaseSite) -> Iterator[pywikibot.ItemPage]:
    query = '''
        SELECT DISTINCT ?item WHERE {
        ?item p:P31 ?statement0.
        ?statement0 (ps:P31/(wdt:P279*)) wd:Q605175.
        }
    '''

    return pywikibot.pagegenerators.WikidataSPARQLPageGenerator(query, site=site)
