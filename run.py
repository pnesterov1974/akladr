import logging
import multiprocessing
import concurrent.futures as cf
import time
from pathlib import Path
from datetime import datetime

from read_source import SourceFile
from write_target import Target
from worker import Worker
from options import (
    SocBaseOption,
    AltNamesOption,
    KladrOption,
    StreetOption,
    DomaOption,
    NameMapOption
)
from loggers import (
    one_logger,
    socrbase_logger,
    altnames_logger,
    kladr_logger,
    street_logger,
    doma_logger,
    namemap_logger,
)
from model_mssql import metadata
from sqlalchemy import create_engine


MSSQL_ENGINE_STR = r"mssql+pymssql://sa:Exptsci123@192.168.1.78/kladr2"

# TODO:
#      store_pack_to_db - rows affected
#      Mapping {sourceFieldName: targetDbFieldName}, OPTIONS
#      WORKER
#      t-f
#      + logging
# sql - check data
# check exception in process
# engine in separate class
# timeit via decorator
# concurrent.futures vs mulpiprocessing
# concurrent.futures vs threading
# logging on screen
# run_ ^^^   vs class
# dump to json

def run_socrbase(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки SocrBase")
    w = Worker(
        option=SocBaseOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка SocrBase завершена, всего загружено {rows}")
    return rows

def run_altnames(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки AltNames")
    w = Worker(
        option=AltNamesOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка AltNames завершена, всего загружено {rows}")
    return rows

def run_kladr(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Kladr")
    w = Worker(
        option=KladrOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка Kladr завершена, всего загружено {rows}")
    return rows

def run_street(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Street")
    w = Worker(
        option=StreetOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка Street завершена, всего загружено {rows}")
    return rows

def run_doma(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки Doma")
    w = Worker(
        option=DomaOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка Doma завершена, всего загружено {rows}")
    return rows

def run_namemap(logger: logging.Logger) -> int:
    logger.debug("Начало загрузки NameMap")
    w = Worker(
        option=NameMapOption,
        engine_str=MSSQL_ENGINE_STR,
        logger=logger
    )
    rows = w()
    logger.debug(f"Загрузка NameMap завершена, всего загружено {rows}")
    return rows

def check_metatada(logger: logging.Logger) -> int:
    try:
        logger.debug("Первичное подключение к БД и создание метаданных")
        engine = create_engine(MSSQL_ENGINE_STR, echo=False)
        metadata.create_all(engine)
        logger.debug("Первичное подключение к БД и создание метаданных успешно")
        return True
    except Exception as ex:
        err_msg = "Ошибка первичного подключения к БД и создания метаданных"
        logger.error(err_msg)
        raise ValueError("=== \t".join[err_msg, (str(ex))])


def run_sync() -> None:
    one_logger.debug("Начало синхронного импорта...")
    t0 = datetime.now()
    check_metatada(logger=one_logger)
    print(run_socrbase(logger=one_logger))
    print(run_altnames(logger=one_logger))
    print(run_kladr(logger=one_logger))
    print(run_street(logger=one_logger))
    print(run_doma(logger=one_logger))
    print(run_namemap(logger=one_logger))
    t1 = datetime.now()
    td = t1 - t0
    one_logger.debug(f"Синхронный импорт завершен. Общее время {td}")
    print(f"Синхронный импорт завершен. Общее время {td}")

# def run_asynch() -> None:
#     print(f"Старт асинхронного импорта...")
#     t0 = datetime.now()
#     cpu_count = multiprocessing.cpu_count()
#     print(f'cpu_count = {cpu_count}')
#     check_metatada(logger=one_logger)

#     pool = multiprocessing.Pool()
#     tasks = []

#     socrbase_task = pool.apply_async(run_socrbase, args={socrbase_logger})
#     print('Запуск процесса для SocrBase')
#     tasks.append(socrbase_task)
#     altnames_task = pool.apply_async(run_altnames, args={altnames_logger})
#     print('Запуск процесса для AltNames')
#     tasks.append(altnames_task)
#     kladr_task = pool.apply_async(run_kladr, args={kladr_logger})
#     print('Запуск процесса для Kladr')
#     tasks.append(kladr_task)
#     street_task = pool.apply_async(run_street, args={street_logger})
#     print('Запуск процесса для Street')
#     tasks.append(street_task)
#     doma_task = pool.apply_async(run_doma, args={doma_logger})
#     print('Запуск процесса для Doma')
#     tasks.append(doma_task)
#     namemap_task = pool.apply_async(run_namemap, args={namemap_logger})
#     print('Запуск процесса для NameMap')
#     tasks.append(namemap_task)

#     pool.close()  # закрываем все процессы
#     pool.join() # собираем все резудьтаты

#     tt = [t.get() for t in tasks]
#     print(tt)
#     t1 = datetime.now()
#     td = t1 - t0
#     print(f"Асинхронный импорт завершен. Общее время {td}")

def run_asynch() -> None:
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
    run_asynch()  #Общее время 0:33:15.940001
    #run_asynch_2()   #33,766666667

# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()