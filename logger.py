# Complete setup_logging function with idempotence

import os
import yaml
import logging
from tqdm import tqdm

# Цветовые коды для вывода в консоль
COLORS = {
    'INFO': '\033[92m',
    'WARNING': '\033[93m',
    'ERROR': '\033[91m',
    'HEADER': '\033[97m'
}
RESET = '\033[0m'


class TqdmLoggingHandler(logging.Handler):
    """
    Обработчик логов, который печатает через tqdm.write()
    с сохранением цветовых кодов.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            color = COLORS.get(record.levelname, '')
            tqdm.write(f"{color}{msg}{RESET}")
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging():
    """
    Настраивает корневой логгер на основании конфигурации в config/config.yaml:
      - log_file: путь к файлу логов ошибок
      - console_level: уровень логирования для консоли
      - file_level: уровень логирования для файла

    Идемпотентна: при повторных вызовах не добавляет дублирующиеся хендлеры.
    Возвращает объект logging.getLogger().
    """
    root = logging.getLogger()
    if getattr(root, '_setup_done', False):
        return root

    config_path = os.environ.get('ETL_CONFIG_PATH', 'config/config.yaml')
    log_file = 'data/etl_error.log'
    console_level = logging.INFO
    file_level = logging.ERROR

    # Читаем секцию global.logging из конфига, если есть
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        logging_cfg = cfg.get('global', {}).get('logging', {})
        log_file = logging_cfg.get('log_file', log_file)
        console_level = getattr(
            logging,
            logging_cfg.get('console_level', 'INFO').upper(),
            console_level
        )
        file_level = getattr(
            logging,
            logging_cfg.get('file_level', 'ERROR').upper(),
            file_level
        )
    except Exception:
        pass

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    fh.setLevel(file_level)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root.addHandler(fh)

    ch = TqdmLoggingHandler()
    ch.setLevel(console_level)
    ch.setFormatter(logging.Formatter('[%(levelname)s] - %(message)s'))
    root.addHandler(ch)

    root._setup_done = True
    return root

def _logger_header(self, oracle_table: str, postgres_table: str) -> None:
    """
    Логирует заголовок вида:
    ------------------ ORACLE.TBL -> POSTGRES.TBL ------------------
    с цветом HEADER.
    """
    header = f"------------------ {oracle_table} -> {postgres_table} ------------------"
    # self — это ваш logger, уже настроенный через setup_logging()
    self.info(f"{COLORS['HEADER']}{header}{RESET}")

# "Вшиваем" метод в класс Logger
logging.Logger.header = _logger_header
