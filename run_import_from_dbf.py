import logging

from worker import Worker
from options import (
    SocBaseOption,
    AltNamesOption,
    KladrOption,
    StreetOption,
    DomaOption,
    NameMapOption,
    KladrObjects,
    SOURCE_FOLDERPATH
)

MSSQL_ENGINE_STR = r"mssql+pymssql://sa:Exptsci123@192.168.1.78/kladr2"

def run_socrbase(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки SocrBase")
    w = Worker(option=SocBaseOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка SocrBase завершена, всего загружено {rows}")
    return rows

def run_altnames(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки AltNames")
    w = Worker(option=AltNamesOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка AltNames завершена, всего загружено {rows}")
    return rows

def run_kladr(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Kladr")
    w = Worker(option=KladrOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка Kladr завершена, всего загружено {rows}")
    return rows

def run_street(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Street")
    w = Worker(option=StreetOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка Street завершена, всего загружено {rows}")
    return rows


def run_doma(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Doma")
    w = Worker(option=DomaOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка Doma завершена, всего загружено {rows}")
    return rows


def run_namemap(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки NameMap")
    w = Worker(option=NameMapOption, engine_str=MSSQL_ENGINE_STR, logger=logger)
    rows = w()
    logger.debug(f"Загрузка NameMap завершена, всего загружено {rows}")
    return rows


# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass