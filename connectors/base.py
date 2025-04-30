from abc import ABC, abstractmethod
from typing import List, Optional, Any, Iterator, Tuple

class BaseConnector(ABC):
    """
    Абстрактный базовый класс для коннекторов к источникам и приёмникам данных.
    """

    @abstractmethod
    def connect(self) -> None:
        """Установить соединение."""
        pass

    @abstractmethod
    def fetch(self, query: str, batch_size: Optional[int] = None) -> Iterator[dict]:
        """
        Получить данные по произвольному SELECT‑запросу.
        :param query: полный SQL‑запрос SELECT.
        :param batch_size: размер батча; при None отдаём по одной строке.
        :return: итератор словарей (каждая запись – dict).
        """
        ...

    @abstractmethod
    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Any:
        """
        Выполнить произвольный запрос.
        :param query: SQL-запрос.
        :param params: параметры запроса.
        :return: результат выполнения.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Закрыть соединение."""
        pass

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
