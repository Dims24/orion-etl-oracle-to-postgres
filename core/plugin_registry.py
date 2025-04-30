# core/plugin_registry.py

import importlib
from typing import Dict, Type
from plugin_interfaces.auto_mapping_interface import AutoMappingPlugin
from plugin_interfaces.fetcher_interface import FetcherPlugin
from plugin_interfaces.transform_interface import TransformPlugin
from plugin_interfaces.validation_interface import ValidationPlugin
from plugin_interfaces.loader_interface import LoaderPlugin

# Словари: ключ — идентификатор плагина (class_name или module), значение — класс
_AUTO_MAPPING_PLUGINS: Dict[str, Type[AutoMappingPlugin]] = {}
_FETCHER_PLUGINS:     Dict[str, Type[FetcherPlugin]]     = {}
_TRANSFORM_PLUGINS:   Dict[str, Type[TransformPlugin]]   = {}
_VALIDATION_PLUGINS:  Dict[str, Type[ValidationPlugin]]  = {}
_LOADER_PLUGINS:      Dict[str, Type[LoaderPlugin]]      = {}

# Соответствие категорий интерфейсам
_CATEGORY_TO_INTERFACE = {
    'auto_mapping': AutoMappingPlugin,
    'fetcher':      FetcherPlugin,
    'transform':    TransformPlugin,
    'validation':   ValidationPlugin,
    'loader':       LoaderPlugin,
}


def register_auto_mapping(cls: Type[AutoMappingPlugin]):
    # Регистрируем плагин под его собственным именем класса
    _AUTO_MAPPING_PLUGINS[cls.__name__] = cls
    return cls

def register_validation(cls: Type[ValidationPlugin]):
    _VALIDATION_PLUGINS[cls.__name__] = cls
    return cls

def register_fetcher(cls: Type[FetcherPlugin]):
    _FETCHER_PLUGINS[cls.__name__] = cls
    return cls


def register_transform(cls: Type[TransformPlugin]):
    _TRANSFORM_PLUGINS[cls.__name__] = cls
    return cls

#
def register_loader(cls: Type[LoaderPlugin]):
    _LOADER_PLUGINS[cls.__name__] = cls
    return cls


def get_plugin(plugin_name: str, category: str):
    """
    Возвращает класс плагина по его имени (class name) или названию файла-модуля.
    Сначала ищем по зарегистрированным плагинам, затем пробуем импорт модуля plugins.<plugin_name>
    и берём первый класс, реализующий нужный интерфейс.

    :param plugin_name: имя плагина (имя класса или имя модуля-файла без .py)
    :param category: одна из ['auto_mapping','fetcher','transform','loader']
    :return: класс плагина
    """
    # Словарь зарегистрированных плагинов для категории
    mapping = {
        'auto_mapping': _AUTO_MAPPING_PLUGINS,
        'fetcher':      _FETCHER_PLUGINS,
        'transform':    _TRANSFORM_PLUGINS,
        'validation': _VALIDATION_PLUGINS,
        'loader':       _LOADER_PLUGINS,
    }.get(category)
    if mapping is None:
        raise ImportError(f"Неизвестная категория: {category}")

    # 1) Попробуем найти по имени класса
    cls = mapping.get(plugin_name)
    if cls:
        return cls

    # 2) Попробуем импортировать модуль plugins.<plugin_name>
    try:
        module = importlib.import_module(f"plugins.{plugin_name}")
    except ImportError as e:
        raise ImportError(
            f"Плагин '{plugin_name}' не найден в {category!r} и не удалось импортировать: {e}"
        )

    # 3) Ищем внутри модуля класс, реализующий нужный интерфейс
    interface = _CATEGORY_TO_INTERFACE[category]
    for attr in dir(module):
        obj = getattr(module, attr)
        if isinstance(obj, type) and issubclass(obj, interface) and obj is not interface:
            return obj

    # 4) Если ничего не нашли — ошибка
    raise ImportError(
        f"Плагин '{plugin_name}'»', загруженный из модуля plugins.{plugin_name}, "
        f"не содержит класс, реализующий интерфейс {interface.__name__}"
    )
