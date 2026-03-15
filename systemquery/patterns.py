import re
from .classes import CatQuery, MatchIdent
from collections.abc import Callable
from frozendict import frozendict


greek_letters = {
    'alpha': 'alf',
    'beta': 'bet',
    'gamma': 'gam',
    'delta': 'del',
    'epsilon': 'eps',
    'zeta': 'zet',
    'eta': 'eta',
    'theta': 'tet',
    'iota': 'iot',
    'kappa': 'kap',
    'lambda': 'lam',
    'mu': 'mu.',
    'nu': 'nu.',
    'xi': 'ksi',
    'omicron': 'omi',
    'pi': 'pi.',
    'rho': 'rho',
    'sigma': 'sig',
    'tau': 'tau',
    'upsilon': 'ups',
    'phi': 'phi',
    'chi': 'chi', # https://simbad.u-strasbg.fr/Pages/guide/chA.htx says khi
    'psi': 'psi',
    'omega': 'ome'
}

constellations = {
    'andromedae': 'And',
    'antliae': 'Ant',
    'apodis': 'Aps',
    'apus': 'Aps',
    'aquarii': 'Aqr',
    'aquarius': 'Aqr',
    'aquilae': 'Aql',
    'aquila': 'Aql',
    'arae': 'Ara',
    'arietis': 'Ari',
    'aurigae': 'Aur',
    'bootis': 'Boo',
    'caeli': 'Cae',
    'camelopardalis': 'Cam',
    'cancri': 'Cnc',
    'canum venaticorum': 'CVn',
    'canum veaticorum': 'CVn',
    'canis majoris': 'CMa',
    'canis major': 'CMa',
    'canis minoris': 'CMi',
    'capricorni': 'Cap',
    'carinae': 'Car',
    'cassiopeiae': 'Cas',
    'centauri': 'Cen',
    'cephei': 'Cep',
    'ceti': 'Cet',
    'chamaelontis': 'Cha',
    'circini': 'Cir',
    'columbae': 'Col',
    'comae berenices': 'Com',
    'coronae austrinae': 'CrA',
    'coronae borealis': 'CrB',
    'corvi': 'Crv',
    'crateris': 'Crt',
    'crucis': 'Cru',
    'cygni': 'Cyg',
    'delphini': 'Del',
    'doradus': 'Dor',
    'draconis': 'Dra',
    'equulei': 'Equ',
    'eridani': 'Eri',
    'fornacis': 'For',
    'geminorum': 'Gem',
    'gruis': 'Gru',
    'herculis': 'Her',
    'horologii': 'Hor',
    'hydrae': 'Hya',
    'hydri': 'Hyi',
    'indi': 'Ind',
    'lacertae': 'Lac',
    'leonis minoris': 'LMi',
    'leonis': 'Leo',
    'leporis': 'Lep',
    'librae': 'Lib',
    'libra': 'Lib',
    'lupi': 'Lup',
    'lyncis': 'Lyn',
    'lyrae': 'Lyr',
    'mensae': 'Men',
    'microscopii': 'Mic',
    'monocerotis': 'Mon',
    'muscae': 'Mus',
    'normae': 'Nor',
    'octantis': 'Oct',
    'ophiuchii': 'Oph',
    'ophiuchi': 'Oph',
    'orionis': 'Ori',
    'orion': 'Ori',
    'pavonis': 'Pav',
    'pegasi': 'Peg',
    'persei': 'Per',
    'phoenicis': 'Phe',
    'pictoris': 'Pic',
    'piscium': 'Psc',
    'piscis austrini': 'PsA',
    'puppis': 'Pup',
    'pyxidis': 'Pyx',
    'reticuli': 'Ret',
    'sagittae': 'Sge',
    'sagittarii': 'Sgr',
    'scorpii': 'Sco',
    'sculptoris': 'Scl',
    'scuti': 'Sct',
    'serpentis': 'Ser',
    'sextantis': 'Sex',
    'tauri': 'Tau',
    'telescopii': 'Tel',
    'trianguli australis': 'TrA',
    'trianguli': 'Tri',
    'tucanae': 'Tuc',
    'ursae majoris': 'UMa',
    'ursae minoris': 'UMi',
    'velorum': 'Vel',
    'virginis': 'Vir',
    'volantis': 'Vol',
    'vulpeculae': 'Vul'
}

r_greek = '|'.join((re.escape(v) for v in greek_letters.keys()))
r_grk = '|'.join((re.escape(v) for v in greek_letters.values()))
r_constel = '|'.join((re.escape(v) for v in constellations.keys()))
r_cst = '|'.join((re.escape(v) for v in constellations.values()))

r_bayer = f'(?P<bayer>[a-z]|{r_greek}|{r_grk})'
r_byn = '(?P<bayerN>[1-9])'
r_bynn = '(?P<bayerNN>[0-9][1-9]|[1-9][0-9])'
r_const = f'(?P<const>{r_constel}|{r_cst})'
r_num = '(?P<num>[1-9][0-9]*)'


def s_bayer_p(m: re.Match) -> str:
    return greek_letters.get(m.group('bayer').lower(), m.group('bayer'))


def s_const(m: re.Match) -> str:
    return constellations.get(m.group('const').lower(), m.group('const'))


def s_bayer(m: re.Match) -> str:
    return f'* {s_bayer_p(m)} {s_const(m)}'


def s_bayer_n(m: re.Match) -> str:
    return f'* {s_bayer_p(m)}0{m.group('bayerN')} {s_const(m)}'


def s_bayer_nn(m: re.Match) -> str:
    return f'* {s_bayer_p(m)}{m.group('bayerNN')} {s_const(m)}'


def s_varstar(m: re.Match) -> str:
    return f'V* {m.group('var')} {s_const(m)}'


def s_flamsteed(m: re.Match) -> str:
    return f'* {m.group('num'):>3} {s_const(m)}'


def s_gould(m: re.Match) -> CatQuery:
    sn = int(m.group('num'))
    cn = s_const(m)

    return CatQuery('Vizier', 'V/135A/catalog', frozendict({ 'G': sn, 'cst': cn }), 'HD {HD}')


def s_gould_rev(m: re.Match) -> MatchIdent:
    return MatchIdent(f'* {m.group('num')}G {s_const(m)}', source='[Vizier:V/135A/catalog]')


patterns: list[tuple[re.Pattern, list[Callable[[re.Match], str|CatQuery|MatchIdent|None]]]] = [
    (re.compile(r'^(?:NAME )?(.*)$'),
     [lambda m: MatchIdent(f'NAME {m.group(1)}', 0.1),
      lambda m: MatchIdent(f'HIDDEN NAME {m.group(1)}', 0.1)]),
    (re.compile(f'^(?:[*] )?{r_bayer} {r_const}', re.IGNORECASE), [s_bayer]),
    (re.compile(f'^(?:[*] )?{r_bayer}[ -]?{r_byn} {r_const}', re.IGNORECASE), [s_bayer_n]),
    (re.compile(f'^(?:[*] )?{r_bayer}[ -]?{r_bynn} {r_const}', re.IGNORECASE), [s_bayer_nn]),
    (re.compile(f'^(?:[*] )?{r_num} {r_const}', re.IGNORECASE), [s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer} {r_const}', re.IGNORECASE), [s_bayer, s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer}[ -]?{r_byn} {r_const}', re.IGNORECASE), [s_bayer_n, s_flamsteed]),
    (re.compile(f'^(?:[*] )?{r_num} {r_bayer}[ -]?{r_bynn} {r_const}', re.IGNORECASE), [s_bayer_nn, s_flamsteed]),
    (re.compile(f'^(?:V?[*] )?{r_num} (?P<var>[A-Z]) {r_const}', re.IGNORECASE), [s_varstar, s_flamsteed]),
    (re.compile(f'^(?:V?[*] )?{r_num} (?P<var>[A-Z][A-Z]) {r_const}', re.IGNORECASE), [s_varstar, s_flamsteed]),
    (re.compile(f'^{r_num} G\\. ?{r_const}', re.IGNORECASE), [s_gould, s_gould_rev]),
    (re.compile(f'^(?:V?[*] )?(?P<var>[A-Z]) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?(?P<var>[A-Z][A-Z]) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?(?P<var>V[0-9]+) {r_const}', re.IGNORECASE), [s_varstar]),
    (re.compile(f'^(?:V?[*] )?V0(?P<var>[1-9][0-9]+) {r_const}', re.IGNORECASE),
     [lambda m: f'V* V{m.group('var')} {s_const(m)}']),
    (re.compile('^(?:[*][*] )?(BAG|BAR|BRT|COO|CPO|DON|EGN|HDS|HJ|KUI|LDS|MET|RMK|RST|STF|WSI) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'** {m.group(1)} {m.group(2):>4}']),
    (re.compile('^(?:EM[*] )?(CDS|LkHA|GGA|GGR|MWC|StHA|VES) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'EM* {m.group(1)} {m.group(2):>4}']),
    (re.compile('^(AD95|BB2009|BBG2010|BBS2011|BJG2004|BSM2011|CPO2009|DBP2006|DM99|DML87|FHM2008|FMS2006|GFT2002|GHJ2008|GMB2010|GMM2008|GMM2009|GMW2007|GVS98|GZB2006|H97b|HD2002|HFR2007|HGM2009b|HRF2005|IHA2007|IHA2008|JBM2010|JVD2011|KAG2008|KW97|LAL96|MJD95|MKS2009|MMS2011|MSJ2009|MSR2009|OJV2009|OTS2008|OW94|PCB2009|PMD2009|PW2010|RBB2002|S87b|SHB2004|SHD2009|SNM2009|WBG2011|WMW2010|YSD2013) (.*)', re.IGNORECASE),
     [lambda m: f'[{m.group(1)}] {m.group(2)}']),

    (re.compile(r'^(?:Cl )?(NGC|Pismis|Trumpler|Blanco|IC|Melotte) ([0-9]+) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'Cl {m.group(1)} {m.group(2):>4} {m.group(3):>5}']),
    (re.compile(r'^(?:Cl[*] )?(NGC|Trumpler|Blanco|Haffner|Melotte|IC|Stock) ([0-9]+) ([A-Z]+) ([0-9A-Z-]+)', re.IGNORECASE),
     [lambda m: f'Cl* {m.group(1)} {m.group(2):>4} {m.group(3):>6} {m.group(4):>7}']),

    (re.compile(r'^(?:\[MBS2007b] )?(CFHT-BL-[0-9]+)', re.IGNORECASE),
     [lambda m: f'[MBS2007b] {m.group(1)}']),
    (re.compile(r'^(?:\[GMM2009] )?(S171 [0-9]+)', re.IGNORECASE),
     [lambda m: f'[GMM2009] {m.group(1)}']),
    (re.compile(r'^DEN ([0-9]{4})([+-][0-9]{4})', re.IGNORECASE),
     [lambda m: f'DENIS J{m.group(1)}.0{m.group(2)}']),
    (re.compile(r'^GRS (.*)', re.IGNORECASE),
     [lambda m: f'Granat {m.group(1)}']),
    (re.compile(r'^(?:Gmb|GMB|Groombridge) (.*)', re.IGNORECASE),
     [lambda m: f'Gmb {m.group(1):>4}']),
    (re.compile(r'^Kruger (.*)', re.IGNORECASE),
     [lambda m: f'** KR {m.group(1):>4}']),
    (re.compile(r'^(?:GJ|Gl|Gliese|NN|Wo) (.*)', re.IGNORECASE),
     [lambda m: f'GJ {m.group(1)}']),
    (re.compile(r'^Lalande (.*)', re.IGNORECASE),
     [lambda m: f'LAL {m.group(1)}']),
    (re.compile(r'^KOI ([0-9]+)', re.IGNORECASE),
     [lambda m: f'KOI-{m.group(1)}']),
    (re.compile(r'^MOA-([0-9]{4}-BLG-[0-9]+)', re.IGNORECASE),
     [lambda m: f'MOA {m.group(1)}']),
    (re.compile(r'^OGLE-TR-([0-9]+)', re.IGNORECASE),
     [lambda m: f'OGLE-TR {m.group(1)}']),
    (re.compile(r'^LOrionis-(CFHT|SOC|MAD) ([0-9]+)', re.IGNORECASE),
     [lambda m: f'LOri-{m.group(1)} {m.group(2):>3}']),
    (re.compile(r'^(Cyg|Nor|TrA)(?:ni)? (X-[1-9])', re.IGNORECASE),
     [lambda m: f'X {m.group(1)} {m.group(2)}']),
    (re.compile(f'(.*?) {r_const}', re.IGNORECASE),
     [lambda m: f'NAME {m.group(1)} {s_const(m)}',
      lambda m: f'HIDDEN NAME {m.group(1)} {s_const(m)}',
      lambda m: f'{m.group(1)} {s_const(m)}']),
    (re.compile('EES2009 Persei (J[0-9]+[+-][0-9]+)', re.IGNORECASE),
     [lambda m: f'2MASS {m.group(1)}'])
]
