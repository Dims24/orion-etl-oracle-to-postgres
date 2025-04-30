from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core import ExecutionContext

class TransformPlugin(ABC):
    """
    Интерфейс для плагина преобразования одной записи.
    """
    # уникальное имя плагина
    class_name: str

    @abstractmethod
    def transform(self, ctx: "ExecutionContext", row: dict) -> dict:
        """
        Берёт одну запись (dict из Oracle), возвращает новую (для вставки в Postgres).
        :param ctx: Класс контекста
        :param row: Словарь из Oracle
        :return: преобразованный row
        """
        ...
