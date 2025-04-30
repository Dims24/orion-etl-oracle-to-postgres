from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from mappings.parser import TableConfig

if TYPE_CHECKING:
    # импортируем ExecutionContext только при проверке типов, не во время выполнения
    from core import ExecutionContext

class AutoMappingPlugin(ABC):
    """
    Интерфейс для плагинов автозаполнения mappings.
    """

    @abstractmethod
    def apply(self, ctx: "ExecutionContext", table_cfg: TableConfig) -> None:
        """
        Заполняет table_cfg.mappings, если они пусты.
        :param ctx: ExecutionContext с таблицей и batch_id
        :param table_cfg: конфиг таблицы, куда надо добавить mappings
        """
        pass
