from collections.abc import Iterable
import os
import json


known_systems = None


def get_ed_known_systems(name_or_id: str|int) -> Iterable[str]:
    global known_systems

    if known_systems is None:
        known_systems = {}
        if os.path.exists('known_systems.json'):
            try:
                with open('known_systems.json', 'rt') as f:
                    for system in json.load(f):
                        id64 = system.get('id64')
                        name = system.get('name')
                        altnames = system.get('altnames')
                        hd = system.get('hd')
                        hipp = system.get('hipp')
                        gliese = system.get('gliese')

                        if name is not None and (hd is not None or hipp is not None or gliese is not None):
                            known_systems.setdefault(name, []).append({'name': name, 'altnames': altnames, 'hd': hd, 'hipp': hipp, 'gliese': gliese })
                        if id64 is not None and (hd is not None or hipp is not None or gliese is not None or name is not None):
                            known_systems.setdefault(id64, []).append({'name': name, 'altnames': altnames, 'hd': hd, 'hipp': hipp, 'gliese': gliese })
            except Exception:
                pass

    for system in known_systems.get(name_or_id, []):
        if (name := system.get('name')) is not None:
            yield name

        if (altnames := system.get('altnames')) is not None and isinstance(altnames, Iterable):
            for name in altnames:
                if isinstance(name, str):
                    yield name

        if (hd := system.get('hd')) is not None:
            if isinstance(hd, int) or (isinstance(hd, str) and hd.isdigit()):
                yield f'HD {hd:>6}'
            elif isinstance(hd, str):
                yield hd

        if (hipp := system.get('hipp')) is not None:
            if isinstance(hipp, int) or (isinstance(hd, str) and hd.isdigit()):
                yield f'HIP {hipp}'
            elif isinstance(hipp, str):
                yield hipp

        if (gliese := system.get('gliese')) is not None:
            if isinstance(gliese, str) and gliese.lower().startswith('gl '):
                yield f'GJ {gliese[3:]}'
