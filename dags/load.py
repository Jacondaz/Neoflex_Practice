from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, FloatType, StringType

hook = PostgresHook(postgres_conn_id='postgres_default')
postgres_uri = 'jdbc:postgresql://postgres:5432/postgres'

properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

connection_params = {
    "url": postgres_uri,
    "dbtable": "dm.dm_f101_round_f_v2",
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

schema = StructType([
    StructField("FROM_DATE", StringType()),
    StructField("TO_DATE", StringType()),
    StructField("CHAPTER", StringType()),
    StructField("LEDGER_ACCOUNT", StringType()),
    StructField("CHARACTERISTIC", StringType()),
    StructField("BALANCE_IN_RUB", FloatType()),
    StructField("BALANCE_IN_VAL", FloatType()),
    StructField("BALANCE_IN_TOTAL", FloatType()),
    StructField("TURN_DEB_RUB", FloatType()),
    StructField("TURN_DEB_VAL", FloatType()),
    StructField("TURN_DEB_TOTAL", FloatType()),
    StructField("TURN_CRE_RUB", FloatType()),
    StructField("TURN_CRE_VAL", FloatType()),
    StructField("TURN_CRE_TOTAL", FloatType()),
    StructField("BALANCE_OUT_RUB", FloatType()),
    StructField("BALANCE_OUT_VAL", FloatType()),
    StructField("BALANCE_OUT_TOTAL", FloatType()),
])
def start_time(text):
    connection = hook.get_conn()
    cursor = connection.cursor()
    time = datetime.now()
    sql = """
        insert into logs.logs(start_time, status)
        values (%s, %s)
        returning log_id;
    """
    cursor.execute(sql, (time, text))
    log_id = cursor.fetchone()[0]
    connection.commit()
    cursor.close()
    connection.close()
    return log_id

def end_time(id, text):
    connection = hook.get_conn()
    cursor = connection.cursor()
    time = datetime.now()
    sql = """
        update logs.logs
        set end_time = %s, status = %s
        where log_id = %s
    """
    cursor.execute(sql, (time, text, id))
    connection.commit()
    cursor.close()
    connection.close()

def spark_session():
    spark = SparkSession.builder.appName("AirflowETL").config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar").getOrCreate()
    return spark

def extract_from_csv_to_pg():
    log_id = start_time("Start_load_to_pg")
    spark = spark_session()
    df = spark.read.csv(f'/opt/airflow/files/part-00000-6bcea2db-2fbb-4dc8-9cf6-9caecbeddc18-c000.csv', sep=',', header=True, schema=schema)
    df = df.withColumn("from_date", col("from_date").cast("date"))
    df = df.withColumn("to_date", col("to_date").cast("date"))
    df.write.jdbc(url=postgres_uri, table=f'dm.dm_f101_round_f_v2', mode="append", properties=properties)
    end_time(log_id, "Finished_load_to_pg")

default_args={
    'owner':'airflow',
    'start_date':datetime(2024, 7, 19),
    'retries': 1
}

dag = DAG(
    "load",
    default_args=default_args,
    schedule_interval='@daily'
)

save_to_pg = PythonOperator(
    task_id='save_to_pg',
    python_callable=extract_from_csv_to_pg,
    dag=dag
)

save_to_pg