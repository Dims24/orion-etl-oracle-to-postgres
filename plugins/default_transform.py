from core import ExecutionContext
from plugin_interfaces.transform_interface import TransformPlugin
import logging

logger = logging.getLogger(__name__)
class_name = "DefaultTransform"

class DefaultTransform(TransformPlugin):
    name = "DefaultTransform"

    def transform(self, ctx: "ExecutionContext", row: dict) -> dict:
        """
        Переносит поля 1:1 согласно mappings, применяя transform-правила из MappingRule.
        """
        out = {}
        for rule in ctx.table_cfg.mappings:
            val = row.get(rule.source)
            # если в mapping_rule.transform задан список операций
            for op in rule.transform or []:
                if op == "strip" and isinstance(val, str):
                    val = val.strip()
                elif op == "upper" and isinstance(val, str):
                    val = val.upper()
                elif op == "lower" and isinstance(val, str):
                    val = val.lower()
                elif "false" in op or "true" in op:
                    if val == "N" or val == 0:
                        val = False
                    elif val == "Y" or val == 1:
                        val = True
                    else:
                        val = val
                elif op.startswith("insert:"):
                    value = op.split(":", 1)[1]
                    if value=='null':
                        value = None
                    val = value
                else:
                    logger.debug("Неизвестная операция '%s' для поля %s", op, rule.source)
            out[rule.target] = val
        return out
