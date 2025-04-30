#!/usr/bin/env python3
import os
import sys
import logging
import argparse

from logger import setup_logging
from mappings.parser import load_config, Config
from connectors.oracle_connector import OracleConnector
from connectors.postgres_connector import PostgresConnector
from core import get_plugin
from core import ExecutionContext
from plugin_interfaces.auto_mapping_interface import AutoMappingPlugin
from plugin_interfaces.fetcher_interface import FetcherPlugin
from plugin_interfaces.transform_interface import TransformPlugin
from datetime import datetime

def run_pipeline(cfg: Config):

    setup_logging()
    logger = logging.getLogger(__name__)

    logger.debug("Запущен пайплайн с конфигом: %s", cfg)

    with OracleConnector() as ora_conn, PostgresConnector() as pg_conn:
        # 1) Auto-mapper
        AutoMapCls = get_plugin(cfg.global_config.auto_mapping_plugin, 'auto_mapping')
        auto_mapper = AutoMapCls(pg_conn)

        default_fetcher_name = cfg.global_config.fetcher_plugin

        global_transformers = [
            get_plugin(name, 'transform')() for name in cfg.global_config.transform_plugins
        ]
        global_validators = [
            get_plugin(name, 'validation')() for name in cfg.global_config.validation_plugins
        ]

        for table_cfg in cfg.tables:
            table_start = datetime.now()

            batch_size = cfg.global_config.batch_size
            batch_id = 0
            buffer = []

            # Новый контекст для таблицы и первого батча
            ctx = ExecutionContext(table_cfg, batch_id, ora_conn, pg_conn)
            ctx.header(table_cfg.target_table, table_cfg.source_table)
            logger.info("Начало обработки %s", table_start)
            # 1.1) Auto-mapping
            auto_mapper.apply(ctx, table_cfg)

            # 2) Fetcher для таблицы
            fetcher_name = table_cfg.fetcher_plugin or default_fetcher_name
            fetcher = get_plugin(fetcher_name, 'fetcher')()

            # 3) Трансформеры и валидаторы
            if table_cfg.transform_override:
                transformers = [ get_plugin(n,'transform')() for n in (table_cfg.transform_plugins or []) ]
            else:
                transformers = global_transformers + [ get_plugin(n,'transform')() for n in (table_cfg.transform_plugins or []) ]
            validators = global_validators

            # 4) Loader для таблицы
            loader_name = cfg.global_config.loader_plugin
            loader = get_plugin(loader_name, 'loader')()

            # 4.1) Создаём tmp-поля и т.п.
            loader.pre_load(ctx, batch_id)

            # 5) Основной цикл – fetch → transform → validate → buffer → load_batch
            for raw in fetcher.fetch(ctx, batch_size):
                rec = raw
                for tr in transformers:
                    rec = tr.transform(ctx, rec)
                ctx.debug(f"Строка преобразована {rec}")
                skip = False
                for v in validators:
                    rec = v.validate(ctx, rec)
                    if rec.get('_skip'):
                        ctx.info("Строка пропущена по валидации")
                        skip = True
                        break
                if skip:
                    continue

                buffer.append(rec)

                # как только накопили batch_size → грузим
                if len(buffer) >= batch_size:
                    # вызываем finalize_batch у трансформеров
                    for tr in transformers:
                        fin = getattr(tr, "finalize_batch", None)
                        if callable(fin):
                            tr.finalize_batch(ctx)

                    # собственно загрузка
                    loader.load_batch(ctx, buffer)

                    ctx.info("Батч #%d загружен (%d строк)", batch_id, len(buffer))
                    buffer.clear()

                    # следующий батч
                    batch_id += 1
                    ctx = ExecutionContext(table_cfg, batch_id, ora_conn, pg_conn)

            # 6) Остаток после всех fetch’ей
            if buffer:
                for tr in transformers:
                    fin = getattr(tr, "finalize_batch", None)
                    if callable(fin):
                        tr.finalize_batch(ctx)

                loader.load_batch(ctx, buffer)
                if batch_id == 0:
                    ctx.info("Данные загружены (%d строк)",  len(buffer))
                else:
                    ctx.info("Остаток батча #%d загружен (%d строк)", batch_id, len(buffer))
                buffer.clear()

            # 7) Финальная донастройка таблицы (UPDATE … и удаление tmp-полей)
            loader.finalize_table(ctx)
            table_end = datetime.now()
            duration = table_end - table_start
            ctx.info("Таблица %s обработана", table_cfg.source_table)
            logger.info("Обработка таблицы %s закончена в %s, за %s",
                        table_cfg.source_table, table_end, duration)

    ctx.info("Pipeline успешно завершён")





def main():
    parser = argparse.ArgumentParser(description="ETL Framework")
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Путь до главного конфигурационного файла"
    )
    args = parser.parse_args()

    os.environ["ETL_CONFIG_PATH"] = args.config
    cfg = load_config(args.config)
    try:
        run_pipeline(cfg)
    except Exception as e:
        logging.error("Фатальная ошибка пайплайна: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
