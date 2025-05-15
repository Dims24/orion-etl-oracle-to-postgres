
# Orion ETL: Миграция данных из Oracle в PostgreSQL

**Orion ETL** — это модульная и расширяемая ETL-система, предназначенная для эффективной миграции данных из Oracle в PostgreSQL. Проект реализован на Python и поддерживает как пакетную, так и потоковую обработку данных, обеспечивая гибкость и надежность при переносе данных между базами данных.

## Возможности

- **Поддержка Oracle и PostgreSQL**
- **Модульная архитектура**
- **Конфигурация через YAML**
- **Docker-окружение**
- **Логирование и мониторинг**

## Структура проекта

```
orion-etl-oracle-to-postgres/
├── cli.py
├── pipeline.py
├── logger.py
├── config/
├── connectors/
├── core/
├── plugins/
├── plugin_interfaces/
├── mappings/
├── generate/
├── docker/
├── docker-compose.yaml
└── requirements.txt
```

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/Dims24/orion-etl-oracle-to-postgres.git
cd orion-etl-oracle-to-postgres
```

### 2. Установка зависимостей

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Настройка конфигурации

```yaml
source:
  type: oracle
  dsn: your_oracle_dsn
  user: your_username
  password: your_password

destination:
  type: postgres
  host: your_postgres_host
  port: 5432
  database: your_database
  user: your_username
  password: your_password

mappings:
  - source_table: employees
    destination_table: employees_pg
    columns:
      - source: emp_id
        destination: id
      - source: emp_name
        destination: name
```

### 4. Запуск ETL-процесса

```bash
python cli.py --config config/your_config.yaml
```

## Использование Docker

```bash
docker-compose up --build
```

## Расширение функциональности

1. Реализуйте интерфейс из `plugin_interfaces/`
2. Поместите в `plugins/`
3. Укажите в конфигурации

## Зависимости

- Python 3.8+
- cx_Oracle
- psycopg2
- PyYAML
- Docker (опционально)

##  Лицензия

MIT

## Вклад

Приветствуются предложения и PR

## Полезные ресурсы

- [Документация Oracle](https://docs.oracle.com/)
- [Документация PostgreSQL](https://www.postgresql.org/docs/)
- [cx_Oracle](https://cx-oracle.readthedocs.io/)
- [psycopg2](https://www.psycopg.org/docs/)
