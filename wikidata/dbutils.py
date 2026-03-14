import sqlite3
from .classes import WikiDataIdent
from ..util import filter_match_name


def create_tables(conn: sqlite3.Connection):
    conn.cursor().execute('CREATE TABLE IF NOT EXISTS wikidata_idents (source TEXT NOT NULL, item_id TEXT NULL) STRICT')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_idents_item_id ON wikidata_idents (item_id)')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_idents_source ON wikidata_idents (source)')

    conn.cursor().execute('CREATE TABLE IF NOT EXISTS wikidata_aliases (item_id TEXT NOT NULL, alias TEXT NOT NULL, aliasinfo TEXT NOT NULL, match_name TEXT NOT NULL) STRICT')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_item_id ON wikidata_aliases (item_id)')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_alias ON wikidata_aliases (alias)')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_aliases_match_name ON wikidata_aliases (match_name)')
    conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS IX_wikidata_aliases_item_id_alias ON wikidata_aliases (item_id, alias)')

    conn.cursor().execute('CREATE TABLE IF NOT EXISTS wikidata_simbad (item_id TEXT NOT NULL, ident TEXT NOT NULL, identinfo TEXT NOT NULL) STRICT')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_simbad_item_id ON wikidata_simbad (item_id)')
    conn.cursor().execute('CREATE INDEX IF NOT EXISTS IX_wikidata_simbad_ident ON wikidata_simbad (ident)')
    conn.cursor().execute('CREATE UNIQUE INDEX IF NOT EXISTS IX_wikidata_simbad_item_id_ident ON wikidata_simbad (item_id, ident)')


def get_entities_by_ident(name: str, conn: sqlite3.Connection) -> set[WikiDataIdent]:
    create_tables(conn)
    cursor = conn.cursor()
    cursor.execute(
        '''
            SELECT DISTINCT s.ident, s.item_id, a.alias
            FROM wikidata_aliases a
            JOIN wikidata_simbad s ON s.item_id = a.item_id
            WHERE s.ident = ?
        ''',
        (name, )
    )

    return set((WikiDataIdent(ident, item_id, alias) for ident, item_id, alias in cursor))


def get_entities_by_name(name: str, conn: sqlite3.Connection) -> set[WikiDataIdent]:
    create_tables(conn)
    cursor = conn.cursor()
    cursor.execute(
        '''
            SELECT DISTINCT s.ident, s.item_id, a.alias
            FROM wikidata_aliases a
            JOIN wikidata_simbad s ON s.item_id = a.item_id
            WHERE a.match_name = ?
        ''',
        (filter_match_name(name), )
    )

    return set((WikiDataIdent(ident, item_id, alias) for ident, item_id, alias in cursor))
