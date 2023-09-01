import logging

from clf import CurrentLogFile
#from options import OptionBase

class KladrLogs:

    @staticmethod
    def init_logger(logger: logging.Logger):
        std_formatter = logging.Formatter(
            fmt='{asctime} {levelname:8} {module:10} {process:10} {funcName:15} {message}', style='{'
        )
        logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='one')
        logger.setLevel('DEBUG')
        file_handler= logging.FileHandler(logfile.log_filepath, mode='a', encoding='utf-8')
        file_handler.setFormatter(std_formatter)
        logger.addHandler(file_handler)

    def __init__(self, one_log: bool, one_log_subname: str, options: list) -> None:
        if one_log:
            self.__loggers = None
            self.__one_logger = logging.getLogger(one_log_subname)
            KladrLogs.init_logger(self.__one_logger)
        else:
            self.__one_logger = None
            self.__loggers = {}
            for op in options:
                self.__loggers[op.CAPTION] = logging.getLogger(op.CAPTION)
                KladrLogs.init_logger(self.__loggers[op.CAPTION])

    one_logger = property(lambda self: self.__one_logger)
    loggers = property(lambda self: self.__loggers)
    

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass