import os
import yaml
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Iterator, Optional, Tuple, Any
from connectors.base import BaseConnector
import logging

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("ETL_CONFIG_PATH", "config/config.yaml")

class PostgresConnector(BaseConnector):
    """
    Коннектор для PostgreSQL, реализующий BaseConnector.
    Параметры подключения берутся из config/config.yaml.
    """

    def __init__(self):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                full_cfg = yaml.safe_load(f)
            pg_cfg = full_cfg.get('global', {})                              .get('connectors', {})                              .get('postgres', {})
        except Exception as ex:
            logger.error("Не удалось загрузить конфигурацию %s: %s", CONFIG_PATH, ex)
            raise RuntimeError(f"Не удалось загрузить конфиг {CONFIG_PATH}: {ex}")

        self.user = pg_cfg.get('user')
        self.password = pg_cfg.get('password')
        self.host = pg_cfg.get('host')
        self.port = pg_cfg.get('port')
        self.database = pg_cfg.get('database')
        self.conn = None

    def connect(self) -> None:
        logger.info("Подключение к Postgres: user=%s host=%s port=%s database=%s",
                    self.user, self.host, self.port, self.database)
        try:
            self.conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            logger.info("Установлено соединение с Postgres")
        except Exception as ex:
            logger.error("Не удалось подключиться к Postgres: %s", ex)
            raise

    def fetch(
        self,
        table: str,
        columns: List[str],
        batch_size: Optional[int] = None
    ) -> Iterator[dict]:
        if self.conn is None:
            raise RuntimeError("PostgresConnector: соединение не установено.")
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        cols_str = ", ".join([f'"{col}"' for col in columns])
        query = f'SELECT {cols_str} FROM "{table}"'
        logger.info("Postgres query: %s", query)
        cursor.execute(query)

        if batch_size:
            logger.debug("Выборка партиями %s", batch_size)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)
        else:
            for row in cursor:
                yield dict(row)

        cursor.close()
        logger.debug("Cursor closed after fetch")

    def execute(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> Any:
        if self.conn is None:
            raise RuntimeError("PostgresConnector: соединение не установено.")
        cursor = self.conn.cursor()
        logger.debug("Выполнение инструкции Postgres: %s | params=%s", query, params)
        cursor.execute(query, params or ())
        if not query.strip().lower().startswith("select"):
            self.conn.commit()
            logger.debug("Postgres DML committed")
            cursor.close()
            return None
        result = cursor.fetchall()
        logger.debug("Выбрано %d строк", len(result))
        cursor.close()
        return result

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
                logger.info("Соединение с Postgres закрыто")
            except Exception as ex:
                logger.error("Ошибка при закрытии соединения с Postgres: %s", ex)
            finally:
                self.conn = None

    def get_table_columns(self, schema: str, table: str) -> List[str]:
        """
        Возвращает список колонок таблицы в PostgreSQL из information_schema.
        """
        query =  """
                 SELECT column_name
                 FROM information_schema.columns
                 WHERE table_schema = %s
                   AND table_name = %s
                 ORDER BY ordinal_position
                     """
        rows = self.execute(query, (schema, table))
        # execute возвращает список кортежей
        return [row[0] for row in rows]
