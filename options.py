from pathlib import Path

from model_mssql import (
    SocrBase,
    TRUNCATE_SocrBase_SQL,
    AltNames,
    TRUNCATE_AltNames_SQL,
    Kladr,
    TRUNCATE_Kladr_SQL,
    Street,
    TRUNCATE_Street_SQL,
    Doma,
    TRUNCATE_Doma_SQL,
    NameMap,
    TRUNCATE_NameMap_SQL,
)

SOURCE_FOLDERPATH = (
    Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "dbf"
)


class OptionBase:
    CAPTION = ''
    SOURCE_FILEPATH = SOURCE_FOLDERPATH
    FIELD_MAPPING = {}
    TARGET_SQLA = None
    PRELIMINARY_SQL = None
    RECORD_PACK_FOR_INSERT = 0


class SocBaseOption(OptionBase):
    CAPTION = "SocrBase"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "SOCRBASE.DBF"
    FIELD_MAPPING = {
        "level": "Level",
        "scname": "ScName",
        "socrname": "SocrName",
        "kod_t_st": "KodTST",
    }
    TARGET_SQLA = SocrBase
    PRELIMINARY_SQL = TRUNCATE_SocrBase_SQL
    RECORD_PACK_FOR_INSERT = 1000


class AltNamesOption:
    CAPTION = "AltNames"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "ALTNAMES.DBF"
    FIELD_MAPPING = {"oldcode": "OldCode", "newcode": "NewCode", "level": "Level"}
    TARGET_SQLA = AltNames
    PRELIMINARY_SQL = TRUNCATE_AltNames_SQL
    RECORD_PACK_FOR_INSERT = 10000


class KladrOption:
    CAPTION = "Kladr"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "KLADR.DBF"
    FIELD_MAPPING = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
        "status": "Status",
    }
    TARGET_SQLA = Kladr
    PRELIMINARY_SQL = TRUNCATE_Kladr_SQL
    RECORD_PACK_FOR_INSERT = 100000


class StreetOption:
    CAPTION = "Street"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "STREET.DBF"
    FIELD_MAPPING = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    TARGET_SQLA = Street
    PRELIMINARY_SQL = TRUNCATE_Street_SQL
    RECORD_PACK_FOR_INSERT = 100000


class DomaOption:
    CAPTION = "Doma"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "DOMA.DBF"
    FIELD_MAPPING = {
        "name": "Name",
        "korp": "Korp",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    TARGET_SQLA = Doma
    PRELIMINARY_SQL = TRUNCATE_Doma_SQL
    RECORD_PACK_FOR_INSERT = 100000


class NameMapOption:
    CAPTION = "NameMap"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "NAMEMAP.DBF"
    FIELD_MAPPING = {
        "code": "Code",
        "name": "Name",
        "shname": "ShName",
        "scname": "ScName",
    }
    TARGET_SQLA = NameMap
    PRELIMINARY_SQL = TRUNCATE_NameMap_SQL
    RECORD_PACK_FOR_INSERT = 100000

KladrObjects = [
        SocBaseOption, AltNamesOption, KladrOption, StreetOption, DomaOption, NameMapOption
    ]

# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass
