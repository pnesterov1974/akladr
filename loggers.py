import logging
from clf import CurrentLogFile

std_formatter = logging.Formatter(
            fmt='{asctime} {levelname:8} {module:10} {process:10} {funcName:15} {message}', style='{'
        )

one_logger = logging.getLogger('akladr_log')
one_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='one')
one_logger.setLevel('DEBUG')
one_file_handler= logging.FileHandler(one_logfile.log_filepath, mode='a', encoding='utf-8')
one_file_handler.setFormatter(std_formatter)
one_logger.addHandler(one_file_handler)

socrbase_logger = logging.getLogger('socrbase_log')
socrbase_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='socrbase')
socrbase_logger.setLevel('DEBUG')
socrbase_file_handler = logging.FileHandler(socrbase_logfile.log_filepath, mode='a', encoding='utf-8')
socrbase_file_handler.setFormatter(std_formatter)
socrbase_logger.addHandler(socrbase_file_handler)

altnames_logger = logging.getLogger('altnames_log')
altnames_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='altnames')
altnames_logger.setLevel('DEBUG')
altnames_file_handler = logging.FileHandler(altnames_logfile.log_filepath, mode='a', encoding='utf-8')
altnames_file_handler.setFormatter(std_formatter)
altnames_logger.addHandler(altnames_file_handler)

kladr_logger = logging.getLogger('kladr_log')
kladr_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='kladr')
kladr_logger.setLevel('DEBUG')
kladr_file_handler = logging.FileHandler(kladr_logfile.log_filepath, mode='a', encoding='utf-8')
kladr_file_handler.setFormatter(std_formatter)
kladr_logger.addHandler(kladr_file_handler)

street_logger = logging.getLogger('street_log')
street_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='street')
street_logger.setLevel('DEBUG')
street_file_handler = logging.FileHandler(street_logfile.log_filepath, mode='a', encoding='utf-8')
street_file_handler.setFormatter(std_formatter)
street_logger.addHandler(street_file_handler)

doma_logger = logging.getLogger('doma_log')
doma_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='doma')
doma_logger.setLevel('DEBUG')
doma_file_handler = logging.FileHandler(doma_logfile.log_filepath, mode='a', encoding='utf-8')
doma_file_handler.setFormatter(std_formatter)
doma_logger.addHandler(doma_file_handler)

namemap_logger = logging.getLogger('namemap_log')
namemap_logfile = CurrentLogFile(days_to_keep=5, silent=False, add_symbol='namemap')
namemap_logger.setLevel('DEBUG')
namemap_file_handler = logging.FileHandler(namemap_logfile.log_filepath, mode='a', encoding='utf-8')
namemap_file_handler.setFormatter(std_formatter)
namemap_logger.addHandler(namemap_file_handler)

# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass