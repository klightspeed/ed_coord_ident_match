from typing import NamedTuple, TypedDict, Required


class WikiDataAliasInfo(TypedDict, total=False):
    name: Required[str]
    simbad: bool
    cats: list[str]
    langs: list[str]


class WikiDataCoords(TypedDict, total=False):
    ra: str
    dec: str


class WikiDataEntry(TypedDict):
    id: str
    labels: dict[str, str]
    lang_aliases: dict[str, list[str]]
    types: list[str]
    aliases: list[WikiDataAliasInfo]
    idents: list[WikiDataAliasInfo]
    simbad_idents: list[str]
    coords: dict[str, WikiDataCoords]


class WikiDataIdent(NamedTuple):
    ident: str
    item_id: str
    alias: str
