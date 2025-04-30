# etl_framework/context.py
import logging

from connectors import OracleConnector
from connectors import PostgresConnector
from mappings.parser import TableConfig


class ExecutionContext:
    """
    Передаётся всем плагинам, содержит:
      - table_cfg: текущая таблица
      - batch_id: порядковый номер батча (int)
      - logger: логгер с автоматическим добавлением названия таблицы и batсh_id
    """

    def __init__(
        self,
        table_cfg: TableConfig,
        batch_id: int,
        ora_conn: OracleConnector,
        pg_conn: PostgresConnector,
    ):
        self.table_cfg = table_cfg
        self.batch_id = batch_id
        self.ora_conn = ora_conn
        self.pg_conn = pg_conn
        self.logger = logging.getLogger(f"{__name__}.{table_cfg.source_table}")

    def debug(self, msg, *args):   self.logger.debug(f"[batch {self.batch_id}] " + msg, *args)
    def info(self, msg, *args):   self.logger.info(f"[batch {self.batch_id}] " + msg, *args)
    def warning(self, msg, *args): self.logger.warning(f"[batch {self.batch_id}] " + msg, *args)
    def error(self, msg, *args): self.logger.error(f"[batch {self.batch_id}] " + msg, *args)
    def header(self, oracle_table: str, postgres_table: str, *args): self.logger.header(oracle_table, postgres_table, *args)
