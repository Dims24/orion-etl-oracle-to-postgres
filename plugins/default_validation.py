import json
from core import register_validation, ExecutionContext
from plugin_interfaces import ValidationPlugin
import re
from typing import Dict, Any

class_name = "DefaultValidation"

@register_validation
class DefaultValidation(ValidationPlugin):
    def validate(self, ctx: ExecutionContext, row: Dict[str, Any]) -> Dict[str, Any]:
        for rule in ctx.table_cfg.mappings:              # MappingRule
            if not rule.validation:
                continue
            for vr in rule.validation:               # ValidationRule
                val = row.get(rule.target)
                # пропускаем пустые
                if val is None:
                    continue
                action = vr.on_fail

                # 1) REGEX
                if vr.type == "regex":
                    pattern = vr.pattern or ""
                    if not re.match(pattern, str(val)):
                        ctx.warning("Не прошёл regex для %s=%r (pattern=%r) → %s",
                                     rule.target, val, pattern, action)
                        if action is None:
                            row[rule.target] = None
                        elif action == "skip":
                            row["_skip"] = True
                            return row
                        elif action.startswith("default:"):
                            row[rule.target] = action.split(":", 1)[1]
                        else:  # error
                            raise RuntimeError(
                                f"Валидация regex прошла с ошибкой: {rule.target}={val} !~ {pattern}"
                            )

                # 2) RANGE (pattern вида "min-max")
                elif vr.type == "range":
                    try:
                        low, high = vr.pattern.split("-", 1)
                        num = float(val)
                        if not (float(low) <= num <= float(high)):
                            ctx.warning("Не в диапазоне %s=%r (range=%s) → %s",
                                         rule.target, val, vr.pattern, action)
                            if action is None:
                                row[rule.target] = None
                            elif action == "skip":
                                row["_skip"] = True
                                return row
                            elif action.startswith("default:"):
                                row[rule.target] = action.split(":", 1)[1]
                            else:
                                raise RuntimeError(
                                    f"Валидация range прошла с ошибкой: {rule.target}={val} not in [{low},{high}]"
                                )
                    except Exception as e:
                        ctx.error("Ошибка парсинга range для %s: %s", rule.target, e)

                # 3) LOOKUP (проверка в справочнике)
                elif vr.type == "lookup" and vr.lookup:
                    tbl = vr.lookup.table
                    key = vr.lookup.key_column
                    sql = f"SELECT 1 FROM \"{tbl}\" WHERE \"{key}\" = %s LIMIT 1"
                    exists = False
                    try:
                        with ctx.pg_conn.conn.cursor() as cur:
                            cur.execute(sql, (str(val),))
                            exists = cur.fetchone() is not None
                    except Exception as e:
                        ctx.error("Ошибка выполнения запроса %s.%s=%r: %s",
                                  tbl, key, val, e)
                        exists = False

                    if not exists:
                        ctx.warning("Ошибка выполнения запроса for %s=%r in %s.%s → %s",
                                     rule.target, val, tbl, key, action)

                        if action is None:
                            row[rule.target] = None
                        elif action == "skip":
                            row["_skip"] = True
                            return row
                        elif action.startswith("default:"):
                            row[rule.target] = action.split(":", 1)[1]
                        else:
                            raise RuntimeError(
                                f"Валидация lookup прошла с ошибкой: {tbl}.{key}={val} not found"
                            )
                # другие типы можно добавить здесь
        return row
