import re
from typing import Iterator, List
from core import ExecutionContext
from plugin_interfaces.fetcher_interface import FetcherPlugin
import logging

class_name = "DefaultFetcher"

class DefaultFetcher(FetcherPlugin):
    """
    Дефолтный плагин для выборки данных из Oracle.
    Формирует SQL SELECT по колонкам + WHERE.
    При ошибке ORA-00904 удаляет отсутствующее поле и ретраит запрос.
    """
    name = "DefaultFetcher"

    def __init__(self, additional_fields: dict = None):
        # Дополнительные поля здесь не используются, но могут быть учтены
        self.additional_fields = additional_fields or {}

    def fetch(
        self,
        ctx: ExecutionContext,
        batch_size: int
    ) -> Iterator[dict]:
        # Инициализация списка колонок
        cols: List[str] = [m.source for m in ctx.table_cfg.mappings]

        schema = ctx.table_cfg.source_schema
        table = ctx.table_cfg.source_table
        where_clause = f" WHERE {ctx.table_cfg.where}" if getattr(ctx.table_cfg, 'where', None) else ""

        # Регулярка для поиска ошибки отсутствия колонки
        pattern = re.compile(r"ORA-00904: \"([^\"]+)\"")

        attempt = 0
        while True:
            attempt += 1
            cols_str = ", ".join(cols)
            query = f"SELECT {cols_str} FROM {schema}.{table}{where_clause}"
            logging.debug(f"Попытка {attempt}: {query}")
            try:
                for row in ctx.ora_conn.fetch(query, batch_size=batch_size):
                    yield row
                # Успешно завершили выборку
                return
            except Exception as e:
                msg = str(e)
                m = pattern.search(msg)
                if m:
                    missing = m.group(1)
                    logging.error(f"Поле '{missing}' отсутствует в Oracle, удаляем и повторяем запрос")
                    # Удаляем отсутствующее поле
                    if missing in cols:
                        cols.remove(missing)
                        logging.warning(f"Поле '{missing}' удалено из запроса")
                        if not cols:
                            logging.error(f"Не осталось колонок для таблицы {table}, прекращаем выборку")
                            return
                        continue
                # Любая другая ошибка
                logging.error(f"Ошибка выборки данных: {msg}")
                raise


