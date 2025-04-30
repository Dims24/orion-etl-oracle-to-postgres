import json
from psycopg2 import sql
from typing import Any, Dict, List

from core import register_transform
from plugin_interfaces import TransformPlugin
from core import ExecutionContext
from mappings.parser import MappingRule

class_name = "DefaultLookup"

@register_transform
class DefaultLookup(TransformPlugin):
    """
    Комбинированный lookup-плагин:
      1) Для каждого правила rule.lookup, где lookup.table != target_table —
         подтягивает value_column из внешней таблицы в Postgres.
      2) Для каждого правила rule.lookup, где lookup.table == target_table —
         накапливает все записи в батче и в finalize_batch() подставляет
         значения из тех же записей (self-lookup).
    """
    def __init__(self):
        # Буфер всех строк текущего батча
        self._buffer: List[Dict[str, Any]] = []
        # Правила self-lookup и внешний lookup
        self._self_rules: List[MappingRule]     = []
        self._external_rules: List[MappingRule] = []
        self._initialized = False

    def _init_rules(self, ctx: ExecutionContext):
        """Разбиваем все rule.lookup на внешние и self."""
        tbl = ctx.table_cfg.target_table
        for rule in ctx.table_cfg.mappings:
            if not rule.lookup:
                continue
            if rule.lookup.table == tbl:
                # lookup на ту же таблицу — self-lookup
                self._self_rules.append(rule)
            else:
                # lookup на другую таблицу — внешний
                self._external_rules.append(rule)
        self._initialized = True

    def transform(self, ctx: ExecutionContext, row: Dict[str, Any]) -> Dict[str, Any]:
        # инициализируем правила один раз
        if not self._initialized:
            self._init_rules(ctx)

        # 1) внешний lookup
        for rule in self._external_rules:
            src_val = row.get(rule.source)
            if src_val is None:
                continue

            tbl_ident = rule.lookup.table
            key_col   = rule.lookup.key_column
            val_col   = rule.lookup.value_column or key_col

            # SQL с явным кастом обоих столбцов в text
            query = sql.SQL(
                'SELECT CAST({valcol} AS text)'
                '  FROM {tbl}'
                ' WHERE CAST({key}    AS text) = %s'
            ).format(
                valcol=sql.Identifier(val_col),
                tbl   =sql.Identifier(tbl_ident),
                key   =sql.Identifier(key_col),
            )

            try:
                with ctx.pg_conn.conn.cursor() as cur:
                    cur.execute(query, (str(src_val),))
                    res = cur.fetchone()
                if res:
                    row[rule.target] = res[0]
                else:
                    # on_missing
                    om = rule.lookup.on_missing or 'error'
                    if om.lower() == 'null':
                        row[rule.target] = None
                    elif om.lower() == 'skip':
                        row['_skip'] = True
                        return row
                    elif om.lower().startswith('default:'):
                        row[rule.target] = om.split(':',1)[1]
                    else:
                        raise RuntimeError(
                            f"Lookup failed: {tbl_ident}.{key_col}={src_val}"
                        )
            except Exception as e:
                ctx.error(
                    "External lookup error %s.%s=%r: %s",
                    tbl_ident, key_col, src_val, e
                )
                raise

        # 2) self-lookup: создаём tmp-поле сразу
        for rule in self._self_rules:
            src_val = row.get(rule.target)
            tgt = rule.target
            tgt_tmp = f"{tgt}_tmp"
            if src_val is None:
                continue

            # Обнуляем настоящий mo_id, чтобы не было “дублирования”
            row[tgt] = None
            # Заполняем временный столбец
            row[tgt_tmp] = src_val

        return row
