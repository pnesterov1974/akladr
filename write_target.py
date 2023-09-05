import threading
from datetime import datetime

from sqlalchemy import create_engine, Table 
from sqlalchemy.sql.elements import TextClause

from read_source import SourceFile


class Target:
    def __init__(
        self,
        source: SourceFile,
        engine_str: str,
        sqla_target: Table,
        preliminary_sql: TextClause,
        insert_pack_reccount: int,
        field_mapping: dict,
        logger=None,
    ) -> None:
        self.__engine = create_engine(engine_str, echo=False)
        self.__source = source
        self.__sqla_target = sqla_target
        self.__preliminary_sql = preliminary_sql
        self.__field_mapping = field_mapping
        self.__insert_pack_reccount = insert_pack_reccount
        self.__insert_packs = None
        self.__threads = None
        if not logger:
            raise ValueError("No logger for action")
        else:
            self.__logger = logger

    def __exec_preliminary_sql(self) -> None:
        with self.__engine.connect() as conn:
            with conn.begin():
                try:
                    conn.execute(self.__preliminary_sql)
                except Exception as ex:
                    err_msg = "Ошибка при выполнении preliminary_sql"
                    raise ValueError("=== \t".join[err_msg, (str(ex))])

    def __store_insert_pack_to_db(self, records: list) -> None:
        t0 = datetime.now()
        with self.__engine.connect() as conn:
            with conn.begin():
                stmt = self.__sqla_target.insert()
                conn.execute(stmt, records)
                t1 = datetime.now()
                store_pack = t1 - t0
                self.__logger.debug(
                    f"Запись пачки размером {len(records)} в бд: {store_pack}"
                )

    def __store_insert_pack_to_db_threaded(self, idx: int) -> None:
        t0 = datetime.now()
        with self.__engine.connect() as conn:
            with conn.begin():
                stmt = self.__sqla_target.insert()
                conn.execute(stmt, self.__insert_packs[idx])
                t1 = datetime.now()
                store_pack = t1 - t0
                self.__logger.debug(
                    f"Запись пачки размером {len(self.__insert_packs[idx])} в бд: {store_pack}"
                )

    def dump_to_json(self, json_filepath: str):
        pass

    def __append_insert_pack(self, pack: list) -> int:
        self.__insert_packs.append(pack)
        idx = len(self.__insert_packs) - 1
        return idx
    
    def __thread_insert_pack(self, idx: int):
        t = threading.Thread(
            target=self.__store_insert_pack_to_db_threaded, args={idx}
        )
        self.__threads.append(t)
        t.start()

    def process_sourcefile_threaded(self, exec_preliminary_sql=False) -> int:
        self.__insert_packs = list()
        self.__threads = list()
        if exec_preliminary_sql:
            self.__exec_preliminary_sql()
        t0 = datetime.now()
        rl = list()
        rows_total = 0
        for r in self.__source:
            rl.append(r)
            if len(rl) >= self.__insert_pack_reccount:
                t1 = datetime.now()
                pack_select = t1 - t0
                print(f"размер массива {len(rl)} сбор за время {pack_select}")
                pidx = self.__append_insert_pack(pack=rl)
                self.__thread_insert_pack(pidx)
                rows_total += len(rl)
                print(f"Объект {self.__sqla_target}, Всего загружено {rows_total}")
                # self.__logger.debug(f'Объект {self.__sqla_target}, Всего загружено {rows_total}')
                t0 = datetime.now()
                rl = list()
        if len(rl) > 0:
            t1 = datetime.now()
            pack_select = t1 - t0
            print(f"размер массива {len(rl)} сбор за время {pack_select}")
            pidx = self.__append_pack_new(pack=rl)
            self.__thread_insert_pack(pidx)
            rows_total += len(rl)
            print(f"Объект {self.__sqla_target}, Всего загружено {rows_total}")
            # self.__logger.debug(f'Объект {self.__sqla_target}, Всего загружено {rows_total}')
        print(f'Жду завершения всех потоков для {self.__sqla_target}')
        for t in self.__threads:
            t.join()
        return rows_total

    def process_sourcefile(self, exec_preliminary_sql=False) -> int:
        if exec_preliminary_sql:
            self.__exec_preliminary_sql()
        t0 = datetime.now()
        rl = list()
        rows_total = 0
        for r in self.__source:
            rr = {self.__field_mapping[k]: v for k, v in r.items()}
            rl.append(rr)
            if len(rl) >= self.__insert_pack_reccount:
                t1 = datetime.now()
                pack_select = t1 - t0
                # print(f'размер массива {len(rl)} сбор за время {pack_select}')
                self.__logger.debug(
                    f"размер массива {len(rl)} сбор за время {pack_select}"
                )
                self.__store_insert_pack_to_db(records=rl)
                rows_total += len(rl)
                # print(f'Объект {self.__sqla_target}, Всего загружено {rows_total}')
                self.__logger.debug(
                    f"Объект {self.__sqla_target}, Всего загружено {rows_total}"
                )
                t0 = datetime.now()
                rl = list()
        if len(rl) > 0:
            t1 = datetime.now()
            pack_select = t1 - t0
            # print(f'размер массива {len(rl)} сбор за время {pack_select}')
            self.__logger.debug(f"размер массива {len(rl)} сбор за время {pack_select}")
            self.__store_insert_pack_to_db(records=rl)
            rows_total += len(rl)
            # print(f'Объект {self.__sqla_target}, Всего загружено {rows_total}')
            self.__logger.debug(
                f"Объект {self.__sqla_target}, Всего загружено {rows_total}"
            )
        return rows_total


# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    pass
