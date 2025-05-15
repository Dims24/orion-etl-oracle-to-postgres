# YAML ⇄ Excel Конвертер конфигураций

Утилита для генерации конфигурационных файлов YAML из Excel-файла и обратной генерации Excel-файла из YAML.

Поддерживает описание ETL-процессов: таблицы, маппинги, трансформации, проверки данных, lookup'ы.

---

## 📦 Структура

- `config/`
  - `global.yaml` — глобальный конфиг с параметрами загрузки и списком таблиц
  - `tables/` — директория с YAML-файлами таблиц (по одному на каждую таблицу)
- `data/main.xlsx` — исходный или результирующий Excel-файл

---

## 🚀 Установка

Требуется Python 3.7+ и зависимости:

```bash
pip install pandas pyyaml openpyxl
```

---

## ⚙️ Использование

### ▶️ Генерация YAML из Excel

```bash
python3 script.py --xlsx_file data/main.xlsx --tables_folder tables
```

- Excel-файл `data/main.xlsx` должен содержать:
  - Первый лист: описание таблиц
  - Остальные листы: правила маппинга (если заданы)

### ◀️ Генерация Excel из YAML

```bash
python3 script.py --reverse --xlsx_file data/generated.xlsx --tables_folder tables
```

- Скрипт прочитает конфиги в `config/global.yaml` и `config/tables/*.yaml`
- Сформирует `data/generated.xlsx` с гиперссылками на листы маппинга

---

## 🧩 Поддерживаемые поля

### В таблице

| Поле               | Описание                           |
|--------------------|------------------------------------|
| `source_table`     | Исходная таблица                   |
| `target_table`     | Целевая таблица                    |
| `source_schema`    | Схема источника                    |
| `target_schema`    | Схема назначения (`public` по умолчанию) |
| `fetcher_plugin`   | Плагин для извлечения              |
| `transform_override` | Отключить глобальные трансформации |
| `transform_plugins` | Список трансформеров через `,`     |
| `loader_plugin`    | Плагин загрузки                    |
| `mappings`         | Имя листа с правилами маппинга     |

### В маппинге

| Поле        | Описание                                      |
|-------------|-----------------------------------------------|
| `source`    | Колонка-источник                              |
| `target`    | Целевая колонка                               |
| `transform` | Описание трансформаций (`func1,func2`)        |
| `plugin`    | Плагин маппинга                               |
| `lookup`    | Lookup-описание в формате `on_missing:table.key=value_col` |
| `validation`| Проверки (`regex:...`, `range:...`, `lookup:...`) |

---

## 📝 Пример вызова

```bash
python3 script.py --xlsx_file data/my_config.xlsx
```

```bash
python3 script.py --reverse --xlsx_file data/exported.xlsx
```

---

## 📂 Пример структуры

```
project/
├── config/
│   ├── global.yaml
│   └── tables/
│       ├── customer.yaml
│       └── orders.yaml
├── data/
│   └── main.xlsx
├── script.py
└── README.md
```

---

## 📑 Логирование

Для вывода используется встроенный логгер (`logger.py`), который настраивается через `setup_logging()`.

---

## 📬 Обратная связь

Для добавления новых типов правил, улучшений или вопросов — обращайся напрямую.
