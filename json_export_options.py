from pathlib import Path

TARGET_FOLDERPATH = (
    Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
)

SOURCE_FOLDERPATH = (
    Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "dbf"
)

class JsonImportOption:
    CAPTION = ""
    SOURCE_FILEPATH = None
    TARGET_FILENAME = None
    FIELD_MAPPING = {}
    RECORD_PACK_FOR_INSERT = 0

class SocrBaseJsonImportOption(JsonImportOption):
    CAPTION = "SocrBase"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "SOCRBASE.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'socrbase.json'
    FIELD_MAPPING = {
        "level": "Level",
        "scname": "ScName",
        "socrname": "SocrName",
        "kod_t_st": "KodTST",
    }
    RECORD_PACK_FOR_INSERT = 1000

class AltNamesJsonImportOption(JsonImportOption):
    CAPTION = "AltNames"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "ALTNAMES.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'altnames.json'
    FIELD_MAPPING = {
        "oldcode": "OldCode", 
        "newcode": "NewCode", 
        "level": "Level"
    }
    RECORD_PACK_FOR_INSERT = 10000

class KladrJsonImportOption(JsonImportOption):
    CAPTION = "Kladr"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "KLADR.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'kladr.json'
    FIELD_MAPPING  = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
        "status": "Status",
    }
    RECORD_PACK_FOR_INSERT = 100000

class StreetJsonImportOption(JsonImportOption):
    CAPTION = "Street"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "STREET.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'street.json'
    FIELD_MAPPING  = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    RECORD_PACK_FOR_INSERT = 100000

class DomaJsonImportOption(JsonImportOption):
    CAPTION = "Doma"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "DOMA.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'doma.json'
    FIELD_MAPPING  = {
        "name": "Name",
        "korp": "Korp",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    RECORD_PACK_FOR_INSERT = 100000

class NameMapJsonImportOption(JsonImportOption):
    CAPTION = "NameMap"
    SOURCE_FILEPATH = SOURCE_FOLDERPATH / "NAMEMAP.DBF"
    TARGET_FILENAME = TARGET_FOLDERPATH / 'namemap.json'
    FIELD_MAPPING  = {
        "code": "Code",
        "name": "Name",
        "shname": "ShName",
        "scname": "ScName",
    }
    RECORD_PACK_FOR_INSERT = 100000


# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass