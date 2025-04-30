import os
import yaml
from typing import List, Optional, Union
from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
from pathlib import Path



# Конфигурация логирования
class LoggingConfig(BaseModel):
    log_file: str = 'error/etl_error.log'
    console_level: str = 'INFO'
    file_level: str = 'ERROR'

# Коннекторы
class OracleConnectorConfig(BaseModel):
    client_lib_dir: Optional[str]
    user: str
    password: str
    host: str
    port: Union[int, str]
    service_name: str

class PostgresConnectorConfig(BaseModel):
    user: str
    password: str
    host: str
    port: Union[int, str]
    database: str

class ConnectorsConfig(BaseModel):
    oracle: OracleConnectorConfig
    postgres: PostgresConnectorConfig

class LookupConfig(BaseModel):
    table: str
    key_column: str
    value_column: Optional[str] = None
    on_missing: Optional[str] = None

# Правила валидации
class ValidationRule(BaseModel):
    type: str  = Field(..., description="Тип валидации: 'regex', 'range', 'lookup'")
    pattern: Optional[str] = Field(
        None,
        description="Если type='regex', сюда вставляем регулярку"
    )
    lookup: Optional[LookupConfig] = Field(
        None,
        description="Если type='lookup', настраиваем, в какой таблице и по какому столбцу проверять"
    )
    on_fail: Optional[str] = Field(
        None,
        description="Что делать, если валидация не прошла: 'null', 'error', 'default:XYZ' и т.п."
    )

# Конфиг таблицы


class MappingRule(BaseModel):
    source: Optional[str] = None
    target: Optional[str] = None
    transform: Optional[Union[str, List[str]]] = None
    plugin: Optional[str] = None
    lookup: Optional[LookupConfig] = None
    validation: Optional[List[ValidationRule]] = Field(
        None,
        description="Локальные правила валидации полей (ValidationRule)"
    )

    @field_validator('transform', mode='before')
    def ensure_transform_list(cls, v):
        if isinstance(v, str):
            return [piece.strip() for piece in v.split(',') if piece.strip()]
        return v

class TableConfig(BaseModel):
    source_table: str = Field(
        ...,
        description="Имя таблицы-источника в Oracle (без схемы)"
    )
    source_schema: str = Field(
        ...,
        description="Схема в Oracle, где лежит source_table"
    )
    target_table: str = Field(
        ...,
        description="Имя таблицы-приёмника в Postgres"
    )
    target_schema: Optional[str] = Field(
        "public",
        description="Схема в Postgres; по умолчанию 'public'"
    )
    fetcher_plugin: Optional[str] = Field(
        None,
        description=(
            "Имя плагина для выборки (Fetcher). "
            "Если не задано — берётся global.fetcher_plugin."
        )
    )
    mappings: Optional[List[MappingRule]] = Field(
        None,
        description=(
            "Список правил маппинга колонок (MappingRule). "
            "Если пусто — применяется auto_mapping_plugin."
        )
    )
    where: Optional[str] = Field(
        None,
        description="Дополнительное условие WHERE для SELECT-запроса"
    )
    transform_override: bool = Field(
        False,
        description=(
            "Если true — глобальные transform_plugins игнорируются, "
            "используются только локальные из transform_plugins."
        )
    )
    transform_plugins: Optional[List[str]] = Field(
        None,
        description=(
            "Список плагинов для преобразования (Transform). "
            "Применяется после DefaultTransform, если transform_override=false, "
            "или вместо глобальных, если transform_override=true."
        )
    )
    loader_plugin: Optional[str] = Field(
        None,
        description=(
            "Имя плагина для загрузки (Loader). "
            "Если не задано — берётся global.loader_plugin."
        )
    )

class GlobalConfig(BaseModel):
    logging: Optional[LoggingConfig] = None

    tables_folder: str = Field(
        "tables",
        description="Папка (относительно config_path.parent), где лежат файлы по таблицам"
    )

    batch_size: int = Field(default=5000, ge=1)

    auto_mapping_plugin: str = Field(default="default_auto_mapping")
    fetcher_plugin:     str = Field(default="default_fetcher")
    transform_plugins:  List[str] = Field(
        default=["default_transform"],
        description="Глобальные трансформеры"
    )
    validation_plugins: List[str] = Field(
        default=["default_validation"],
        description="Глобальные валидаторы"
    )
    loader_plugin:      str = Field(default="default_loader")

    connectors: ConnectorsConfig

    table_files: List[str] = Field(
        ...,
        description="Список файлов (имена *.yaml) из tables_folder в порядке обработки"
    )

class Config(BaseModel):
    global_config: GlobalConfig = Field(...,alias="global", description="Конфиг главного файла")
    tables: List[TableConfig]
    model_config = ConfigDict(populate_by_name=True)


def load_config(path: Optional[str] = None) -> Config:
    """
    Загружает конфиг:
     1) главный файл config.yaml → GlobalConfig
     2) из global.tables_folder и global.table_files читаем каждый файл в TableConfig
    """
    # 1) главный конфиг
    config_path = Path(path or os.environ.get('ETL_CONFIG_PATH', 'config/config.yaml'))
    raw = yaml.safe_load(config_path.read_text(encoding='utf-8'))

    try:
        global_cfg = GlobalConfig.model_validate(raw.get('global', {}))
    except ValidationError as e:
        raise RuntimeError(f"Ошибка в секции [global] конфига: {e}")

    # 2) находим папку с таблицами
    tables_dir = config_path.parent / global_cfg.tables_folder
    if not tables_dir.is_dir():
        raise FileNotFoundError(f"Папка с таблицами не найдена: {tables_dir}")

    # 3) читаем каждый файл из списка
    tables: List[TableConfig] = []
    for file_name in global_cfg.table_files:
        table_path = tables_dir / file_name
        if not table_path.is_file():
            raise FileNotFoundError(f"Файл описания таблицы не найден: {table_path}")
        raw_tbl = yaml.safe_load(table_path.read_text(encoding='utf-8'))
        try:
            tbl_cfg = TableConfig.model_validate(raw_tbl)
        except ValidationError as e:
            raise RuntimeError(f"Ошибка в файле {file_name}: {e}")
        tables.append(tbl_cfg)

    return Config(global_config=global_cfg, tables=tables)
