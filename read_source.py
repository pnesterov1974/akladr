from collections.abc import Iterator
from pathlib import Path

from dbf_light import Dbf


class SourceFile:
    # def __init__(self, source_filepath: str, field_list: list) -> None:
    #     self.__source_filepath = source_filepath
    #     self.__field_list = field_list

    def __init__(self, source_filepath: str, field_mapping: dict) -> None:
        self.__source_filepath = source_filepath
        self.__field_list = list(field_mapping.keys())

    def __iter__(self) -> Iterator[dict]:
        with Dbf.open(self.__source_filepath) as dbf:
            for r in dbf:  # try here
                d = dict()
                for fl in self.__field_list:
                    d[fl] = getattr(r, fl)
                yield d

    def probe_data(self, row_count: int = 5):
        with Dbf.open(self.__source_filepath) as dbf:
            for i, r in enumerate(dbf, 1):
                print(r, type(r))
                d = dict()
                for fl in self.__field_list:
                    a = getattr(r, fl)
                    d[fl] = getattr(r, fl)
                print(d)
                if i > row_count:
                    return


# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    socrbase = SourceFile(
        source_filepath=(
            Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "dbf" / "SOCRBASE.DBF"
            ),
            field_mapping={
                "level": "Level", 
                "scname": 'ScName', 
                "socrname": "SocrName", 
                "kod_t_st": "KodTST"
            }
        )
    for r in socrbase:
        print(r)
