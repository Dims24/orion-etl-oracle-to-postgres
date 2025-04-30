from abc import ABC, abstractmethod
from typing import Iterator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core import ExecutionContext
    from connectors.base import BaseConnector

class FetcherPlugin(ABC):
    """
    Интерфейс для плагина выполнения запросов к Oracle.
    Плагин должен определить:
      - class attribute `name`: str — уникальное имя плагина;
      - метод `fetch(self, ora_conn: BaseConnector, table_cfg: TableConfig, batch_size: int) -> Iterator[dict]`.
    """

    # Уникальное имя плагина, совпадает с классом
    class_name: str

    @abstractmethod
    def fetch(
        self,
        ctx: "ExecutionContext",
        batch_size: int
    ) -> Iterator[dict]:
        """
        Выполняет выборку данных из Oracle согласно конфигурации таблицы.
        :param ctx: класс контекста
        :param ora_conn: коннектор к Oracle
        :param table_cfg: конфигурация таблицы из mappings.parser.TableConfig
        :param batch_size: размер батча для выборки
        :return: итератор словарей (каждая запись – dict)
        """
        pass
