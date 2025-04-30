import io
import re
from typing import Any, Dict, List, Tuple
from psycopg2 import sql
from psycopg2.extras import execute_values

from core import register_loader
from plugin_interfaces import LoaderPlugin
from core import ExecutionContext
from mappings.parser import MappingRule

class_name = "DefaultLoader"

@register_loader
class DefaultLoader(LoaderPlugin):
    """
        Loader-плагин, который:
          1) Перед загрузкой создаёт колонки {target}_tmp того же типа,
             что lookup.key_column.
          2) Загружает батчи (COPY/INSERT) как обычно.
          3) После всей загрузки делает UPDATE … FROM …, переносит значения
             из tmp в настоящий target и удаляет tmp-колонки.
        """

    def pre_load(self, ctx: ExecutionContext, batch_id: int = 0) -> None:
        tbl = ctx.table_cfg.target_table
        pg = ctx.pg_conn  # ваш PostgresConnector
        conn = pg.conn
        truncate_flag = True if batch_id == 0 else False

        self_rules = [
            r for r in ctx.table_cfg.mappings
            if r.lookup and r.lookup.table == tbl
        ]
        if not self_rules and truncate_flag is False:
            return
        with conn.cursor() as cur:
            if truncate_flag:
                try:
                    truncate_sql = f'TRUNCATE TABLE "{tbl}" RESTART IDENTITY CASCADE;'
                    cur.execute(truncate_sql)
                    ctx.info(f"Таблица {tbl} очищена перед вставкой данных.")
                except Exception as e:
                    ctx.error(f"Ошибка при очистке таблицы {tbl}: {e}")
                    return
            for rule in self_rules:
                key_col = rule.lookup.key_column
                # получаем data_type
                cur.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_name = %s
                    """,
                    (tbl, key_col)
                )
                row = cur.fetchone()
                if not row:
                    ctx.error("Не нашёл информацию о колонке %s.%s", tbl, key_col)
                    continue

                data_type = row[0]
                tmp_col = f"{rule.target}_tmp"

                cur.execute(
                    sql.SQL("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} {dt}")
                    .format(
                        t=sql.Identifier('public', tbl),
                        c=sql.Identifier(tmp_col),
                        dt=sql.SQL(data_type)
                    )
                )
                ctx.info("Создана временная колонка %s.%s %s", tbl, tmp_col, data_type)


            conn.commit()

    def load_batch(self, ctx: ExecutionContext, rows: List[Dict[str, Any]]) -> None:
        """
        Вставляет батч строк через psycopg2.extras.execute_values
        """
        if not rows:
            return

        tbl = ctx.table_cfg.target_table
        conn = ctx.pg_conn.conn

        # Берём список колонок из первого row
        columns = list(rows[0].keys())
        # Генерим SQL
        insert_sql = sql.SQL("INSERT INTO {t} ({cols}) VALUES %s").format(
            t=sql.Identifier('public', tbl),
            cols=sql.SQL(', ').join(sql.Identifier(c) for c in columns)
        )
        # Формируем список кортежей значений
        values = [
            tuple(row.get(col) for col in columns)
            for row in rows
        ]

        with conn.cursor() as cur:
            # execute_values гораздо быстрее, чем executemany
            execute_values(cur, insert_sql.as_string(conn), values, page_size=1000)
        conn.commit()
        ctx.info("Загружен батч %d строк в %s", len(rows), tbl)

    def finalize_table(self, ctx: ExecutionContext) -> None:
        tbl = ctx.table_cfg.target_table
        pg = ctx.pg_conn
        conn = pg.conn

        self_rules = [
            r for r in ctx.table_cfg.mappings
            if r.lookup and r.lookup.table == tbl
        ]
        if not self_rules:
            return

        with conn.cursor() as cur:
            for rule in self_rules:
                src_tmp = f"{rule.target}_tmp"
                tgt = rule.target
                lookup = rule.lookup.key_column
                valcol = rule.lookup.value_column or lookup

                cur.execute(
                    sql.SQL("""
                            UPDATE {t} AS target
                            SET {tgt} = source.{val}
                            FROM {t} AS source
                            WHERE target.{src_tmp} = source.{lookup}
                              AND source.{val} IS NOT NULL
                            """).format(
                        t=sql.Identifier('public', tbl),
                        tgt=sql.Identifier(tgt),
                        val=sql.Identifier(valcol),
                        src_tmp=sql.Identifier(src_tmp),
                        lookup=sql.Identifier(lookup)
                    )
                )
                ctx.info("Self-lookup выполнен: %s ← %s via %s", tgt, src_tmp, lookup)

                cur.execute(
                    sql.SQL("ALTER TABLE {t} DROP COLUMN IF EXISTS {c}")
                    .format(
                        t=sql.Identifier('public', tbl),
                        c=sql.Identifier(src_tmp)
                    )
                )
                ctx.info("Удалена временная колонка %s.%s", tbl, src_tmp)

            conn.commit()

