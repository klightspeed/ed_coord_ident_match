from collections.abc import Collection, Iterable
from abc import abstractmethod, ABC
from .classes import *


class SystemQueryBase(ABC):
    @abstractmethod
    def query_idents(self, names: set[str]) -> dict[str, set[SimbadEntry]]:
        pass

    @abstractmethod
    def query_coords(self, matches: Collection[SimbadMatch]) -> Iterable[SimbadTableMatch|Iterable]:
        pass


class SystemQueryDatabase(SystemQueryBase):
    @abstractmethod
    def get_simbad_idents(self) -> dict[str, set[SimbadEntry]]:
        pass

    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def get_basic_ident_diff(self) -> list[int]:
        pass

    @abstractmethod
    def get_last_basic_oid_date(self) -> SimbadOidDate:
        pass

    @abstractmethod
    def get_last_ident_oidref_date(self) -> SimbadOidDate:
        pass

    @abstractmethod
    def insert_basic(self, basics: Iterable[SimbadBasic|Iterable]):
        pass

    @abstractmethod
    def insert_idents(self, idents: Iterable[SimbadIdent|Iterable]):
        pass

    @abstractmethod
    def insert_syscoords(self, coords: Iterable[SystemCoords|Iterable]):
        pass

    @abstractmethod
    def get_syscoords(self) -> Iterable[SystemCoords|Iterable]:
        pass

    @abstractmethod
    def query_all_matches(self) -> Iterable[SimbadTableMatch|Iterable]:
        pass

    @abstractmethod
    def insert_matches(self, matches: Iterable[SimbadDBMatch|Iterable]):
        pass

    @abstractmethod
    def get_processed_matches(self) -> Iterable[SimbadDBMatch]:
        pass
