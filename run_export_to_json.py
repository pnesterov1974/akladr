from pathlib import Path

from read_source import SourceFile
from write_json import WriteToJson

from options import (
    SOURCE_FOLDERPATH
)

def run_socrbase_json() -> int:
    print("Начало загрузки SocrBase в json")
    fm = {
        "level": "Level",
        "scname": "ScName",
        "socrname": "SocrName",
        "kod_t_st": "KodTST",
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    socrbase = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "SOCRBASE.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=socrbase, target_filepath=json_folderpath / 'socrbase.json')
    recs = wj.dump_to_json()
    print(f"Загрузка SocrBase в в json завершена, всего загружено {recs}")
    return recs

def run_altnames_json() -> int:
    print("Начало загрузки AltNames в json")
    fm = {
        "oldcode": "OldCode", 
        "newcode": "NewCode", 
        "level": "Level"
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    source = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "ALTNAMES.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=source, target_filepath=json_folderpath / 'altnames.json')
    recs = wj.dump_to_json()
    print(f"Загрузка AltNames в json завершена, всего загружено {recs}")
    return recs

def run_kladr_json() -> int:
    print("Начало загрузки Kladr в json")
    fm = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
        "status": "Status",
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    source = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "KLADR.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=source, target_filepath=json_folderpath / 'kladr.json')
    recs = wj.dump_to_json()
    print(f"Загрузка Kladr в json завершена, всего загружено {recs}")
    return recs


def run_street_json() -> int:
    print("Начало загрузки Street в json")
    fm = {
        "name": "Name",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    source = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "STREET.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=source, target_filepath=json_folderpath / 'street.json')
    recs = wj.dump_to_json()
    print(f"Загрузка Street в json завершена, всего загружено {recs}")
    return recs

def run_doma_json() -> int:
    print("Начало загрузки Doma в json")
    fm = {
        "name": "Name",
        "korp": "Korp",
        "socr": "Socr",
        "code": "Code",
        "index": "Index",
        "gninmb": "Gninmb",
        "uno": "Uno",
        "ocatd": "Ocatd",
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    source = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "DOMA.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=source, target_filepath=json_folderpath / 'doma.json')
    recs = wj.dump_to_json()
    print(f"Загрузка Doma в json завершена, всего загружено {recs}")
    return recs

def run_namemap_json() -> int:
    print("Начало загрузки NameMap в json")
    fm = {
        "code": "Code",
        "name": "Name",
        "shname": "ShName",
        "scname": "ScName",
    }
    json_folderpath = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "json"
    )
    source = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "NAMEMAP.DBF",
        field_mapping=fm,
    )
    wj = WriteToJson(source=source, target_filepath=json_folderpath / 'namemap.json')
    recs = wj.dump_to_json()
    print(f"Загрузка NameMap в json завершена, всего загружено {recs}")
    return recs

# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass