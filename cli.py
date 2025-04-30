#!/usr/bin/env python3
import os
import sys
import argparse
import logging

from logger import setup_logging
from connectors.oracle_connector import OracleConnector
from connectors.postgres_connector import PostgresConnector
from mappings.parser import load_config
from pipeline import run_pipeline

def check_oracle():
    try:
        with OracleConnector() as ora:
            pass
        logging.info("Соединение с Oracle установлено")
        return True
    except Exception as e:
        logging.error("Ошибка соединения с Oracle: %s", e)
        return False

def check_postgres():
    try:
        with PostgresConnector() as pg:
            pass
        logging.info("Соединение с Postgres установлено")
        return True
    except Exception as e:
        logging.error("Ошибка соединения с Postgres: %s", e)
        return False

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(
        description="ETL Framework: connectivity checker"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to ETL config file (default: config/config.yaml)"
    )
    args = parser.parse_args()

    # Устанавливаем путь к конфигу для всех модулей
    os.environ["ETL_CONFIG_PATH"] = args.config

    # Сначала загружаем конфиг, чтобы setup_logging прочитал правильные настройки
    try:
        cfg = load_config(args.config)
    except Exception as e:
        logger.error(f"Не удалось загрузить конфигурацию: {e}")
        sys.exit(1)

    # Настраиваем логирование единожды на основании секции global.logging
    logger.info(
        "Загружена конфигурация:\n%s",
        cfg.model_dump_json(indent=2, exclude_unset=True)
    )

    # Проверяем соединения
    ok_oracle = check_oracle()
    ok_postgres = check_postgres()

    if not (ok_oracle and ok_postgres):
        logger.error("Ошибка соединения с Oracle или Postgres")
        sys.exit(1)

    run_pipeline(cfg)
    logger.info("Пайплайн завершён успешно")
    sys.exit(0)



if __name__ == "__main__":
    main()
