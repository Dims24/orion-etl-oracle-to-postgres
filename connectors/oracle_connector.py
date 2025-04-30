import os
import yaml
import oracledb
from typing import Iterator, Optional, Tuple, Any
from connectors.base import BaseConnector
import logging

from logger import setup_logging

# Initialize logging (idempotent)
setup_logging()

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("ETL_CONFIG_PATH", "config/config.yaml")

class OracleConnector(BaseConnector):
    """
    Коннектор для Oracle. Параметры подключения из config/config.yaml.
    """

    def __init__(self):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                full_cfg = yaml.safe_load(f)
            oracle_cfg = full_cfg.get('global', {}) \
                                 .get('connectors', {}) \
                                 .get('oracle', {})
            logger.info("Loaded Oracle connector config from %s", CONFIG_PATH)
        except Exception as ex:
            logger.error("Failed to load config %s: %s", CONFIG_PATH, ex)
            raise RuntimeError(f"Не удалось загрузить конфиг {CONFIG_PATH}: {ex}")

        self.client_lib_dir = oracle_cfg.get('client_lib_dir')
        self.user = oracle_cfg.get('user')
        self.password = oracle_cfg.get('password')
        self.host = oracle_cfg.get('host')
        self.port = oracle_cfg.get('port')
        self.service_name = oracle_cfg.get('service_name')
        self.conn = None

    def connect(self) -> None:
        # Инициализация клиента Oracle, если доступно
        init_fn = getattr(oracledb, "init_oracle_client", None)
        if callable(init_fn) and self.client_lib_dir:
            try:
                init_fn(lib_dir=self.client_lib_dir)
                logger.debug("Oracle instant client initialized from %s", self.client_lib_dir)
            except getattr(oracledb, "ProgrammingError", Exception):
                logger.debug("Oracle client init ignored")

        dsn = f"{self.host}:{self.port}/{self.service_name}"
        logger.info("Connecting to Oracle: %s", dsn)
        self.conn = oracledb.connect(user=self.user, password=self.password, dsn=dsn)
        logger.info("Oracle connection established")

    def fetch(
        self,
        query: str,
        batch_size: Optional[int] = None
    ) -> Iterator[dict]:
        """
        Выполнить произвольный SELECT-запрос и вернуть словари.
        :param query: полный SQL SELECT запрос
        :param batch_size: размер выборки; если None - построчно
        """
        if not self.conn:
            raise RuntimeError("OracleConnector: соединение не установлено.")
        cursor = self.conn.cursor()
        logger.info("Запрос в Oracle: %s", query)
        cursor.execute(query)
        col_names = [desc[0] for desc in cursor.description]

        if batch_size:
            logger.debug("Fetching in batches of %s", batch_size)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(col_names, row))
        else:
            for row in cursor:
                yield dict(zip(col_names, row))

        cursor.close()
        logger.debug("Cursor closed after fetch")

    def execute(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> Any:
        if not self.conn:
            raise RuntimeError("OracleConnector: соединение не установлено.")
        cursor = self.conn.cursor()
        logger.info("Oracle statement: %s | params=%s", query, params)
        cursor.execute(query, params or ())
        if not query.strip().lower().startswith("select"):
            self.conn.commit()
            logger.debug("Oracle DML committed")
            cursor.close()
            return None
        result = cursor.fetchall()
        logger.debug("Fetched %d rows", len(result))
        cursor.close()
        return result

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
                logger.info("Oracle connection closed")
            except Exception as ex:
                logger.error("Error closing Oracle connection: %s", ex)
            finally:
                self.conn = None
