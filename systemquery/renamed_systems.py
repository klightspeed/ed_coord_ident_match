from collections.abc import Iterable
import os
import shutil
import urllib.request
from .. import config
from ..util import num_re, pg_sys_re

renamed_systems_sheet_uri = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS2Q8f9tWZIJz5S1z6Fv1pfNgFxywIfyZVJGkGFvQ4TZ9Si8UZ8GkGnuiMo8SZgB27tTchO3rCqA0fx/pub?output=tsv'
renamed_systems: dict[str|int, set[str]] | None = None


def get_ed_renamed_systems(sys_addr: int) -> Iterable[str]:
    global renamed_systems

    if renamed_systems is None:
        renamed_systems = {}

        if not os.path.exists(os.path.join(config.cache_dir, 'renamed_systems.txt')):
            with urllib.request.urlopen(renamed_systems_sheet_uri) as sheet:
                with open(os.path.join(config.cache_dir, 'renamed_systems.txt.tmp'), 'wb') as f:
                    shutil.copyfileobj(sheet, f)

            os.rename(
                os.path.join(config.cache_dir, 'renamed_systems.txt.tmp'),
                os.path.join(config.cache_dir, 'renamed_systems.txt')
            )

        with open(os.path.join(config.cache_dir, 'renamed_systems.txt'), 'rt') as f:
            for line in f:
                line = line.strip()
                parts = line.split('\t')

                if len(parts) == 4 and num_re.match(parts[2]):
                    renamed_from, renamed_to, m_sys_addr, _ = parts
                    m_sys_addr = int(m_sys_addr)
                    renamed_systems.setdefault(m_sys_addr, set()).add(renamed_from)
                    renamed_systems.setdefault(m_sys_addr, set()).add(renamed_to)

    for name in renamed_systems.get(sys_addr, set()):
        if not pg_sys_re.match(name):
            yield name
