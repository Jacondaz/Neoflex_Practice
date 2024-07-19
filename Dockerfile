# Используем базовый образ Apache Airflow
FROM apache/airflow:2.9.2

# Переключаемся на пользователя root для установки зависимостей
USER root

# Обновление списка пакетов и установка OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Переключаемся на пользователя airflow для установки PySpark
USER airflow

# Установка PySpark
RUN pip install pyspark

COPY postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
