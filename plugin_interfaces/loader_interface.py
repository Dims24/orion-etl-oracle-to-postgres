from abc import ABC, abstractmethod
from typing import List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from core import ExecutionContext

class LoaderPlugin(ABC):

    @abstractmethod
    def pre_load(self, ctx: "ExecutionContext", batch_id: int = 0) -> None:
        """
        Вызывается один раз перед загрузкой первой порции данных для таблицы.
        Здесь можно создавать временные колонки.
        """
        ...

    @abstractmethod
    def load_batch(self,
                   ctx: "ExecutionContext",
                   rows: List[Dict[str, Any]]) -> None:
        """
        Фактическая загрузка одной порции (батча) данных в таблицу.
        """
        ...

    @abstractmethod
    def finalize_table(self, ctx: "ExecutionContext") -> None:
        """
        Вызывается после загрузки всех батчей. Тут мы делаем UPDATE … и удаляем tmp-колонки.
        """
        ...
