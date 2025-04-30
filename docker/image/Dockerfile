# Используем официальный Python-образ
FROM python:3.10-slim

# Настройка часового пояса
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Устанавливаем системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libaio1 libaio-dev \
        libpq-dev build-essential \
        unzip nano \
    && rm -rf /var/lib/apt/lists/*

# Переводим pip в небуферизованный режим (чтобы логи сразу шли в консоль)
ENV PYTHONUNBUFFERED=1

# Рабочая директория
RUN mkdir /app
RUN chmod 777 /app

# Копируем и устанавливаем Python-зависимости
COPY requirements.txt .
RUN pip install -r requirements.txt

# Устанавливаем Oracle Instant Client
# Предполагается, что файлы instantclient-basic и instantclient-sdk лежат в корне проекта
COPY  --chmod=775 instantclient/instantclient-basic-linux.x64-21.17.0.0.0dbru.zip /app/
COPY  --chmod=775 instantclient/instantclient-sdk-linux.x64-21.17.0.0.0dbru.zip /app/

# Создаём папку под Instant Client
ENV ORACLE_HOME=/opt/oracle/instantclient
RUN mkdir -p $ORACLE_HOME

# Распаковываем оба архива и удаляем их
RUN unzip /app/instantclient-basic-*.zip -d $ORACLE_HOME
RUN unzip -o /app/instantclient-sdk-*.zip   -d $ORACLE_HOME
RUN rm -f /app/instantclient-*.zip

# Добавляем Oracle в путь библиотек
ENV ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient/instantclient_21_17
ENV LD_LIBRARY_PATH=$ORACLE_CLIENT_LIB_DIR:$LD_LIBRARY_PATH

# Копируем код проекта
COPY . /app

WORKDIR /app
