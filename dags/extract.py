from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

postgres_uri = 'jdbc:postgresql://postgres:5432/postgres'
hook = PostgresHook(postgres_conn_id='postgres_default')

properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

connection_params = {
    "url": postgres_uri,
    "dbtable": "dm.dm_f101_round_f",
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}
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

def extract_from_pg_and_to_csv():
    log_id = start_time("Start_extract_to_csv")
    spark = spark_session()
    df = spark.read.format("jdbc").option("url", connection_params["url"]).option("dbtable", connection_params["dbtable"]) \
    .option("user", connection_params["user"]) \
    .option("password", connection_params["password"]) \
    .option("driver", connection_params["driver"]) \
    .load()

    output_path = "/opt/airflow/files/dm_f101_round_f.csv"
    df.write.option("header", "true").csv(output_path)
    end_time(log_id, "Finished_extract_to_csv")

def extract_from_csv_to_pg():
    spark = spark_session()
    df = spark.read.csv(f'/opt/airflow/files/dm_f101_round_f.csv/part-00000-5b91166e-b91f-41db-a5b3-d162613dacc2-c000.csv', sep=',', header=True)
    df.write.jdbc(url=postgres_uri, table=f'dm.dm_f101_round_f', mode="append", properties=properties)
    spark.close()

default_args={
    'owner':'airflow',
    'start_date':datetime(2024, 7, 19),
    'retries': 1
}

dag = DAG(
    "extract",
    default_args=default_args,
    schedule_interval='@daily'
)

save_to_csv = PythonOperator(
    task_id='save_to_csv',
    python_callable=extract_from_pg_and_to_csv,
    dag=dag
)

save_to_csv