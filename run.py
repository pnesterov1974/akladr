import logging
import multiprocessing
import concurrent.futures as cf
import time
from datetime import datetime

from sqlalchemy import create_engine

from logg import KladrLogs
from worker import Worker
from options import (
    SocBaseOption,
    AltNamesOption,
    KladrOption,
    StreetOption,
    DomaOption,
    NameMapOption,
    KladrObjects,
)

from model_mssql import metadata


MSSQL_ENGINE_STR = r"mssql+pymssql://sa:Exptsci123@192.168.1.78/kladr2"

# TODO:
#      store_pack_to_db - rows affected (video from yt)
# общий логгер + id запуска + logging on screen
# sql - check data
# t-f check exception in process
# engine in separate class
# time vs datetime on performance check
# async await
# dump to json
# async run all => pikl sqla_engime
# multiprocess futures vs pool
# _store_to_db_dufferl


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


def run_synch_all() -> int:
    kl = KladrLogs(one_log=True, one_log_subname='kladr_one', options=None)
    kl.one_logger.debug("Начало синхронной загрузки данных")
    print("Начало синхронной загрузки данных")
    check_metatada(logger=kl.one_logger)
    total_rows = 0
    t0 = datetime.now()
    for op in KladrObjects:
        print(f"Начало загрузки {op.CAPTION}")
        kl.one_logger.debug(f"Начало загрузки {op.CAPTION}")
        t1 = datetime.now()
        w = Worker(
            option=op,
            engine_str=MSSQL_ENGINE_STR,
            logger=kl.one_logger
            #logger=kl.select_logger(op.CAPTION),
        )
        rows = w()
        total_rows += rows
        td1 = datetime.now() - t1
        print(
            f"Загрузка {op.CAPTION} завершена, всего загружено {rows}, длительность {td1}"
        )
        kl.one_logger.debug(f"Загрузка {op.CAPTION} завершена, всего загружено {rows}, длительность {td1}")
    td0 = datetime.now() - t0
    print(f"Загрузка завершена, общая длительность {td0}")
    return total_rows


def run_asynch_pool() -> None:
    print(f"Старт асинхронного импорта...")
    kl = KladrLogs(
        one_log=False, one_log_subname=None, options=KladrObjects
    )
    t0 = datetime.now()
    cpu_count = multiprocessing.cpu_count()
    print(f'cpu_count = {cpu_count}')
    #check_metatada(logger=one_logger)

    pool = multiprocessing.Pool()
    tasks = []

    socrbase_task = pool.apply_async(run_socrbase, args={kl.select_logger(SocBaseOption.CAPTION)})
    print('Запуск процесса для SocrBase')
    tasks.append(socrbase_task)
    altnames_task = pool.apply_async(run_altnames, args={kl.select_logger(AltNamesOption.CAPTION)})
    print('Запуск процесса для AltNames')
    tasks.append(altnames_task)
    kladr_task = pool.apply_async(run_kladr, args={kl.select_logger(KladrOption.CAPTION)})
    print('Запуск процесса для Kladr')
    tasks.append(kladr_task)
    #street_task = pool.apply_async(run_street, args={kl.select_logger(StreetOption.CAPTION)})
    #print('Запуск процесса для Street')
    #tasks.append(street_task)
    #doma_task = pool.apply_async(run_doma, args={kl.select_logger(DomaOption.CAPTION)})
    #print('Запуск процесса для Doma')
    #tasks.append(doma_task)
    #namemap_task = pool.apply_async(run_namemap, args={kl.select_logger(NameMapOption.CAPTION)})
    #print('Запуск процесса для NameMap')
    #tasks.append(namemap_task)

    pool.close()  # закрываем все процессы
    pool.join() # собираем все резудьтаты

    tt = [t.get() for t in tasks]
    print(tt)
    t1 = datetime.now()
    td = t1 - t0
    print(f"Асинхронный импорт завершен. Общее время {td}")


def run_asynch_futures() -> None:
    print(f"Старт асинхронного импорта...")
    kl = KladrLogs(
        one_log=False, one_log_subname=None, options=KladrObjects
    )
    t0 = time.perf_counter()
    cpus = multiprocessing.cpu_count()
    print(f"cpu_count = {cpus}")
    #check_metatada(logger=one_logger)

    tasks = []

    with cf.ProcessPoolExecutor() as executor:
        psocrbase = executor.submit(run_socrbase, kl.select_logger(SocBaseOption.CAPTION))
        print("Запуск процесса для SocrBase")
        tasks.append(psocrbase)
        paltnames = executor.submit(run_altnames, kl.select_logger(AltNamesOption.CAPTION))
        print("Запуск процесса для AltNames")
        tasks.append(paltnames)
        pkladr = executor.submit(run_kladr, kl.select_logger(KladrOption.CAPTION))
        print("Запуск процесса для Kladr")
        tasks.append(pkladr)
        #pstreet = executor.submit(run_street, kl.select_logger(StreetOption.CAPTION))
        #print("Запуск процесса для Street")
        #tasks.append(pstreet)
        #pdoma = executor.submit(run_doma, kl.select_logger(DomaOption.CAPTION))
        #print("Запуск процесса для Doma")
        #tasks.append(pdoma)
        pnamemap = executor.submit(run_namemap, kl.select_logger(NameMapOption.CAPTION))
        print("Запуск процесса для NameMap")
        tasks.append(pnamemap)

        for f in cf.as_completed(tasks):
            print(f.result())

    t1 = time.perf_counter()
    td = t1 - t0
    print(f"Асинхронный импорт завершен. Общее время {td}")


def run_asynch_all() -> None: #########################
    print(f"Старт асинхронного импорта...")
    t0 = time.perf_counter()
    cpu_count = cpu_count()
    print(f"cpu_count = {cpu_count}")
    # check_metatada(logger=one_logger)

    kl = KladrLogs(
        one_log=False, one_log_subname=None, options=KladrObjects
    )
    tasks = []
    with cf.ProcessPoolExecutor() as executor:
        for op in KladrObjects:
            w = Worker(
                option=op,
                engine_str=MSSQL_ENGINE_STR,
                # logger=kl.one_logger
                logger=kl.select_logger(op.CAPTION),
            )
            p = executor.submit(w, kl.select_logger(op.CAPTION))
            #psocrbase = executor.submit(run_socrbase, socrbase_logger)
            print(f"Запуск процесса для {op.CAPTION}")
            tasks.append(p)

        for f in cf.as_completed(tasks):
            print(f.result())

    t1 = time.perf_counter()
    td = t1 - t0
    print(f"Асинхронный импорт завершен. Общее время {td}")


def main():
    # check_metatada(logger=one_logger)
    # print(run_synch_all())  # Общее время 0:43:23.036586
    # run_asynch()  #Общее время 0:33:15.940001
    # run_asynch_2()   #33,766666667
    run_synch_all()

# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
