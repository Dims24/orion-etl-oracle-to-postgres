from core import register_auto_mapping, ExecutionContext
from plugin_interfaces.auto_mapping_interface import AutoMappingPlugin
from connectors.postgres_connector import PostgresConnector
from mappings.parser import TableConfig, MappingRule

class_name = "DefaultAutoMapping"

@register_auto_mapping
class DefaultAutoMapping(AutoMappingPlugin):
    """
    Если для таблицы mappings пуст, берём список колонок из Postgres
    и создаём MappingRule с source=имя_колонки, target=имя_колонки.
    """
    def __init__(self, pg_conn: PostgresConnector):
        # Получаем соединение к Postgres для чтения метаданных
        self.pg = pg_conn

    def apply(self, ctx: ExecutionContext, table_cfg: TableConfig) -> None:
        ctx.info("Запускаю авто-маппинг для таблицы %s", ctx.table_cfg.source_table)
        if table_cfg.mappings:
            ctx.debug("mappings уже заданы, пропускаю")
            return

        cols = self.pg.get_table_columns(table_cfg.target_schema, table_cfg.target_table)
        table_cfg.mappings = [ MappingRule(source=c, target=c) for c in cols ]
        ctx.info("Добавил %d правил 1:1", len(cols))
