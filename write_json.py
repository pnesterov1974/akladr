import json
from pathlib import Path

from read_source import SourceFile

class WriteToJson:

    def __init__(self, source: SourceFile, target_filepath: str) -> None:
        self.__source = source
        self.__target = target_filepath

    def dump_to_json(self) -> int:
        #Path("data.json").write_text(json.dumps(data))
        s = [r for r in self.__source]
        with open(self.__target, 'w', encoding='utf-8') as f:
            json.dump(s, f, ensure_ascii=False, indent=4)
        print(len(s))
        return len(s)

    def test_json(self) -> None:
        with open(self.__target, 'r', encoding='utf-8') as f:
            s = json.load(f)
        print(type(s), len(s))


# ---------------------------------------------------------------------------------------
if __name__ == "__main__": #pass
    SOURCE_FOLDERPATH = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "dbf"
    )
    sf = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / "SOCRBASE.DBF",
        field_mapping={
            "level": "Level",
            "scname": "ScName",
            "socrname": "SocrName",
            "kod_t_st": "KodTST",
        }
    )
    JSON_FOLDERPATH = (
        Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr"
    )
    wtj = WriteToJson(source=sf, target_filepath= JSON_FOLDERPATH / 'socrbase.json')
    wtj.dump_to_json()
    wtj.test_json()

