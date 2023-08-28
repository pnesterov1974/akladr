import logging
import multiprocessing
import concurrent.futures as cf
import time
from pathlib import Path
from datetime import datetime

from read_source import SourceFile
from write_target import Target
#from utils import timeit
#from dbe import DbEngine
from loggers import (
    one_logger,
    socrbase_logger,
    altnames_logger,
    kladr_logger,
    street_logger,
    doma_logger,
    namemap_logger,
)
from model_mssql import (
    metadata,
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
from sqlalchemy import create_engine, text

SOURCE_FOLDERPATH = p = (
    Path("/") / "home" / "pnesterov" / "my_dev" / "files" / "kladr" / "dbf"
)

SOCRBASE_FILE = "SOCRBASE.DBF"
ALTNAMES_FILE = "ALTNAMES.DBF"
KLADR_FILE = "KLADR.DBF"
STREET_FILE = "STREET.DBF"
DOMA_FILE = "DOMA.DBF"
NAMEMAP_FILE = "NAMEMAP.DBF"

SOCRBASE_FIELDLIST = ["level", "scname", "socrname", "kod_t_st"]
ALTNAMES_FIELDLIST = ["oldcode", "newcode", "level"]
KLADR_FIELDLIST = ["name", "socr", "code", "index", "gninmb", "uno", "ocatd", "status"]
STREET_FIELDLIST = ["name", "socr", "code", "index", "gninmb", "uno", "ocatd"]
DOMA_FIELDLIST = ["name", "korp", "socr", "code", "index", "gninmb", "uno", "ocatd"]
NAMEMAP_FIIELDLIST = ["code", "name", "shname", "scname"]

MSSQL_ENGINE_STR = r"mssql+pymssql://sa:Exptsci123@192.168.1.78/kladr2"

# TODO:
#      store_pack_to_db - rows affected
#      Mapping {sourceFieldName: targetDbFieldName}
#      t-f
#      + logging
# sql - check data
# check exception in process
# engine in separate class
# timeit via decorator
# concurrent.futures vs mulpiprocessing
# concurrent.futures vs threading
# logging on screen


def run_socrbase(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки SocrBase")
    socrbase = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / SOCRBASE_FILE,
        field_list=SOCRBASE_FIELDLIST,
    )
    target = Target(
        source=socrbase,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=SocrBase,
        preliminary_sql=TRUNCATE_SocrBase_SQL,
        insert_pack_reccount=10000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка SocrBase завершена, всего загружено {rows}")
    return rows


def run_altnames(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки AltNames")
    altnames = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / ALTNAMES_FILE, field_list=ALTNAMES_FIELDLIST
    )
    target = Target(
        source=altnames,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=AltNames,
        preliminary_sql=TRUNCATE_AltNames_SQL,
        insert_pack_reccount=10000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка AltNames завершена, всего загружено {rows}")
    return rows


def run_kladr(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Kladr")
    kladr = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / KLADR_FILE, field_list=KLADR_FIELDLIST
    )
    target = Target(
        source=kladr,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=Kladr,
        preliminary_sql=TRUNCATE_Kladr_SQL,
        insert_pack_reccount=50000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка Kladr завершена, всего загружено {rows}")
    return rows


def run_street(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Street")
    street = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / STREET_FILE, field_list=STREET_FIELDLIST
    )
    target = Target(
        source=street,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=Street,
        preliminary_sql=TRUNCATE_Street_SQL,
        insert_pack_reccount=100000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка Street завершена, всего загружено {rows}")
    # print(type(target.insert_paks), len(target.insert_paks))
    # for r in target.insert_paks:
    #     print(type(r), len(r))
    return rows

def run_doma(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Doma")
    doma = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / DOMA_FILE, field_list=DOMA_FIELDLIST
    )
    target = Target(
        source=doma,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=Doma,
        preliminary_sql=TRUNCATE_Doma_SQL,
        insert_pack_reccount=200000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка Doma завершена, всего загружено {rows}")
    # print(type(target.insert_paks), len(target.insert_paks))
    # for r in target.insert_paks:
    #     print(type(r), len(r))
    return rows


def run_namemap(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки NameMap")
    namemap = SourceFile(
        source_filepath=SOURCE_FOLDERPATH / NAMEMAP_FILE, field_list=NAMEMAP_FIIELDLIST
    )
    target = Target(
        source=namemap,
        engine_str=MSSQL_ENGINE_STR,
        sqla_target=NameMap,
        preliminary_sql=TRUNCATE_NameMap_SQL,
        insert_pack_reccount=10000,
        logger=logger,
    )
    rows = target.process_sourcefile(exec_preliminary_sql=True)
    logger.debug(f"Загрузка NameMap завершена, всего загружено {rows}")
    return rows


def check_metatada(logger: logging.Logger) -> int:
    try:
        logger.debug("Первичное подключение к БД и создание метаданных")
        engine = create_engine(MSSQL_ENGINE_STR, echo=False)
        metadata.create_all(engine)
        #dbEngine.init_dbe(engine_str=MSSQL_ENGINE_STR)
        logger.debug("Первичное подключение к БД и создание метаданных успешно")
        return True
    except Exception as ex:
        err_msg = "Ошибка первичного подключения к БД и создания метаданных"
        logger.error(err_msg)
        raise ValueError("=== \t".join[err_msg, (str(ex))])


# @timeit
def run_synch() -> None:
    one_logger.debug("Начало синхронного импорта...")
    t0 = datetime.now()
    check_metatada(logger=one_logger)
    run_socrbase(logger=one_logger)
    run_altnames(logger=one_logger)
    run_kladr(logger=one_logger)
    run_street(logger=one_logger)
    run_doma(logger=one_logger)
    run_namemap(logger=one_logger)
    t1 = datetime.now()
    td = t1 - t0
    one_logger.debug(f"Синхронный импорт завершен. Общее время {td}")
    print(f"Синхронный импорт завершен. Общее время {td}")


def run_asynch() -> None:
    print(f"Старт асинхронного импорта...")
    t0 = datetime.now()
    cpu_count = multiprocessing.cpu_count()
    print(f'cpu_count = {cpu_count}')
    check_metatada(logger=one_logger)

    pool = multiprocessing.Pool()
    tasks = []

    socrbase_task = pool.apply_async(run_socrbase, args={socrbase_logger})
    print('Запуск процесса для SocrBase')
    tasks.append(socrbase_task)
    altnames_task = pool.apply_async(run_altnames, args={altnames_logger})
    print('Запуск процесса для AltNames')
    tasks.append(altnames_task)
    kladr_task = pool.apply_async(run_kladr, args={kladr_logger})
    print('Запуск процесса для Kladr')
    tasks.append(kladr_task)
    street_task = pool.apply_async(run_street, args={street_logger})
    print('Запуск процесса для Street')
    tasks.append(street_task)
    doma_task = pool.apply_async(run_doma, args={doma_logger})
    print('Запуск процесса для Doma')
    tasks.append(doma_task)
    namemap_task = pool.apply_async(run_namemap, args={namemap_logger})
    print('Запуск процесса для NameMap')
    tasks.append(namemap_task)

    pool.close()  # закрываем все процессы
    pool.join() # собираем все резудьтаты

    tt = [t.get() for t in tasks]
    print(tt)
    t1 = datetime.now()
    td = t1 - t0
    print(f"Асинхронный импорт завершен. Общее время {td}")

def run_asynch_2() -> None:
    print(f"Старт асинхронного импорта...")
    t0 = time.perf_counter()
    cpu_count = multiprocessing.cpu_count()
    print(f'cpu_count = {cpu_count}')
    check_metatada(logger=one_logger)

    tasks = []

    with cf.ProcessPoolExecutor() as executor:
        psocrbase = executor.submit(run_socrbase, socrbase_logger)
        print('Запуск процесса для SocrBase')
        tasks.append(psocrbase)
        paltnames = executor.submit(run_altnames, altnames_logger)
        print('Запуск процесса для AltNames')
        tasks.append(paltnames)
        pkladr = executor.submit(run_kladr, kladr_logger)
        print('Запуск процесса для Kladr')
        tasks.append(pkladr)
        pstreet = executor.submit(run_street, street_logger)
        print('Запуск процесса для Street')
        tasks.append(pstreet)
        pdoma = executor.submit(run_doma, doma_logger)
        print('Запуск процесса для Doma')
        tasks.append(pdoma)
        pnamemap = executor.submit(run_namemap, namemap_logger)
        print('Запуск процесса для NameMap')
        tasks.append(pnamemap)

        for f in cf.as_completed(tasks):
            print(f.result())
    
    t1 = time.perf_counter()
    td = t1 - t0
    print(f"Асинхронный импорт завершен. Общее время {td}")


def main():
    #run_synch()  #Общее время 0:43:23.036586
    #run_asynch()  #Общее время 0:33:15.940001
    run_asynch_2()   #33,766666667

# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()