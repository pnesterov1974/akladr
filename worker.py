import logging

from read_source import SourceFile
from write_target import Target
from options import OptionBase

class Worker:

    def __init__(self,
                 option: OptionBase,
                 engine_str: str,
                 logger: logging.Logger
                ) -> None:
        self.__logger = logger
        self.__source = SourceFile(
            source_filepath=option.SOURCE_FILEPATH, 
            field_mapping=option.FIELD_MAPPING
            )
        self.__target = Target(
            source=self.__source,
            engine_str=engine_str,
            sqla_target=option.TARGET_SQLA,
            preliminary_sql=option.PRELIMINARY_SQL,
            insert_pack_reccount=option.RECORD_PACK_FOR_INSERT,
            field_mapping=option.FIELD_MAPPING,
            logger=self.__logger
            )
        
    def __call__(self):  # __cal__(self, asyncly=False)
        rows = self.__target.process_sourcefile(exec_preliminary_sql=True)
        self.__logger.debug(f"Загрузка SocrBase завершена, всего загружено {rows}")
        return rows

# ---------------------------------------------------------------------------------------
if __name__ == "__main__": pass