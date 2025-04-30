from abc import ABC, abstractmethod
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from core import ExecutionContext

class ValidationPlugin(ABC):
    """
    Плагин валидации: получает дообработанную запись и либо помечает её
    (_skip=True), либо меняет поля по правилам, либо бросает ошибку.
    """
    class_name: str

    @abstractmethod
    def validate(self, ctx: "ExecutionContext", row: Dict) -> Dict:
        """
        :param ctx: контекст (таблица, batch_id, коннекторы и т.д.)
        :param row: словарь с полями target → значение
        :return: либо тот же row, либо дополненный флагом row['_skip']=True,
                 либо изменённый (например row[field]=None), либо бросает ошибку.
        """
        pass
