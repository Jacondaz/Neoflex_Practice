from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from datetime import datetime
import os
import time

hook = PostgresHook(postgres_conn_id='postgres_default')
postgres_uri = 'jdbc:postgresql://postgres:5432/postgres'
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

schema_ft_balance_f = StructType([
    StructField("ON_DATE", StringType(), nullable=False),
    StructField("ACCOUNT_RK", IntegerType(), nullable=False),
    StructField("CURRENCY_RK", IntegerType()),
    StructField("BALANCE_OUT", FloatType())
])

schema_ft_posting_f = StructType([
    StructField("OPER_DATE", StringType(), nullable=False),
    StructField("CREDIT_ACCOUNT_RK", IntegerType(), nullable=False),
    StructField("DEBET_ACCOUNT_RK", IntegerType(), nullable=False),
    StructField("CREDIT_AMOUNT", FloatType()),
    StructField("DEBET_AMOUNT", FloatType())
])

schema_md_account_d = StructType([
    StructField("DATA_ACTUAL_DATE", StringType(), nullable=False),
    StructField("DATA_ACTUAL_END_DATE", StringType(), nullable=False),
    StructField("ACCOUNT_RK", IntegerType(), nullable=False),
    StructField("ACCOUNT_NUMBER", StringType(), nullable=False),
    StructField("CHAR_TYPE", StringType(), nullable=False),
    StructField("CURRENCY_RK", IntegerType(), nullable=False),
    StructField("CURRENCY_CODE", StringType(), nullable=False)
])

schema_md_currency_d = StructType([
    StructField("CURRENCY_RK", IntegerType(), nullable=False),
    StructField("DATA_ACTUAL_DATE", StringType(), nullable=False),
    StructField("DATA_ACTUAL_END_DATE", StringType()),
    StructField("CURRENCY_CODE", StringType()),
    StructField("CODE_ISO_CHAR", StringType())
])

schema_md_exchange_rate_d = StructType([
    StructField("DATA_ACTUAL_DATE", StringType(), nullable=False),
    StructField("DATA_ACTUAL_END_DATE", StringType()),
    StructField("CURRENCY_RK", IntegerType(), nullable=False),
    StructField("REDUCED_COURCE", FloatType()),
    StructField("CODE_ISO_NUM", StringType())
])

schema_md_ledger_account_s = StructType([
    StructField("CHAPTER", StringType()),
    StructField("CHAPTER_NAME", StringType()),
    StructField("SECTION_NUMBER", IntegerType()),
    StructField("SECTION_NAME", StringType()),
    StructField("SUBSECTION_NAME", StringType()),
    StructField("LEDGER1_ACCOUNT", IntegerType()),
    StructField("LEDGER1_ACCOUNT_NAME", StringType()),
    StructField("LEDGER_ACCOUNT", IntegerType(), nullable=False),
    StructField("LEDGER_ACCOUNT_NAME", StringType()),
    StructField("CHARACTERISTIC", StringType()),
    StructField("START_DATE", StringType(), nullable=False),
    StructField("END_DATE", StringType())
])

schemas = {
    'ft_balance_f': schema_ft_balance_f,
    'ft_posting_f': schema_ft_posting_f,
    'md_account_d': schema_md_account_d,
    'md_currency_d': schema_md_currency_d,
    'md_exchange_rate_d': schema_md_exchange_rate_d,
    'md_ledger_account_s': schema_md_ledger_account_s
}
yy_mm_dd = ['md_account_d', 'md_currency_d', 'md_exchange_rate_d']

def spark_session():
    spark = SparkSession.builder.appName("AirflowETL").config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar").getOrCreate()
    return spark

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

def truncate_table(file_name):
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE ds.{file_name};")
    connection.commit()
    connection.close()
    cursor.close()

def extract_and_load_task():
    spark = spark_session()
    for file in os.listdir('/opt/airflow/files/'):
        file_name = file.split('.')[0]
        schema = schemas[file_name]
        df = spark.read.csv(f'/opt/airflow/files/{file}', sep=';', header=True, schema=schema)
        if file_name in yy_mm_dd:
            df = df.withColumn('DATA_ACTUAL_DATE',to_date(col('DATA_ACTUAL_DATE'), "yyyy-MM-dd"))
            df = df.withColumn('DATA_ACTUAL_END_DATE',to_date(col('DATA_ACTUAL_END_DATE'), "yyyy-MM-dd"))
            if file_name == 'md_exchange_rate_d':
                df = df.dropDuplicates(["data_actual_date", "currency_rk"])
        elif file_name == 'md_ledger_account_s':
            df = df.withColumn('START_DATE',to_date(col('START_DATE'), "yyyy-MM-dd"))
            df = df.withColumn('END_DATE',to_date(col('END_DATE'), "yyyy-MM-dd"))
        if file_name == 'ft_balance_f':
            df = df.withColumn('ON_DATE', to_date(col('ON_DATE'), 'dd.MM.yyyy'))
        elif file_name =='ft_posting_f':
            df = df.withColumn('OPER_DATE', to_date(col('OPER_DATE'), "dd-MM-yyyy"))
        truncate_table(file_name)
        df.write.jdbc(url=postgres_uri, table=f'ds.{file_name}', mode="append", properties=properties)
    spark.stop()

def fill_dm_account_turnover():
    connection = hook.get_conn()
    cursor = connection.cursor()
    log_id = start_time("Start_fill_dm_account_turnover")
    sql = """
    do $$
        declare
            d date := '2018-01-01';
            log_id_temp int;
        begin
            while d <= '2018-01-31' loop
                call dm.fill_account_turnover_f(d);
                d := d + interval '1 day';
            end loop;
    end $$;
    """
    end_time(log_id, "Finished_fill_dm_account_turnover")
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()

def fill_dm_account_balance_f():
    connection = hook.get_conn()
    cursor = connection.cursor()
    log_id = start_time("Start_fill_account_balance")
    sql = """
    do $$
        declare
            d date := '2018-01-01';
            log_id_temp int;
        begin
            while d <= '2018-01-31' loop
            call dm.fill_account_balance_f(d);
            d := d + interval '1 day';
            end loop;
    end $$;
    """
    end_time(log_id, "Finished_fill_account_balance")
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()

def start_process(**kwargs):
    start_time = datetime.now()
    sql = """
    INSERT INTO logs.logs (start_time, status)
    VALUES (%s, %s) returning log_id
    """
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql, (start_time, 'Started_etl_process'))
    log_id = cursor.fetchone()[0]
    ti = kwargs['ti']
    ti.xcom_push(key='log_id', value=log_id)
    connection.commit()
    connection.close()
    cursor.close()
    time.sleep(5)

def end_process(**kwargs):
    end_time = datetime.now()
    sql = """
    UPDATE logs.logs
    SET end_time = %s, status = %s
    WHERE log_id = %s
    """
    connection= hook.get_conn()
    cursor = connection.cursor()
    ti = kwargs['ti']
    log_id = ti.xcom_pull(key='log_id')
    cursor.execute(sql, (end_time, 'Finished_etl_process', log_id))
    connection.commit()
    cursor.close()
    connection.close()


default_args={
    'owner':'airflow',
    'start_date':datetime(2024, 7, 15),
    'retries': 1
}

dag = DAG(
    "run_fill_procedure",
    default_args=default_args,
    schedule_interval='@daily'
)

start = PythonOperator(
    task_id='start_process',
    python_callable=start_process,
    provide_context=True,
    dag=dag
)

end = PythonOperator(
    task_id='end_process',
    python_callable=end_process,
    provide_context=True,
    dag=dag
)

extract_and_load = PythonOperator(
    task_id='extract_and_load_process',
    python_callable=extract_and_load_task,
    provide_context=True,
    dag=dag
)

fill_dm_account_turnover_task = PythonOperator(
    task_id = 'fill_dm_account_turnover',
    python_callable=fill_dm_account_turnover,
    provide_context=True,
    dag=dag
)

fill_dm_account_balance_f_task = PythonOperator(
    task_id = 'fill_dm_account_balance_f',
    python_callable=fill_dm_account_balance_f,
    provide_context=True,
    dag=dag
)

fill_dm_account_balance_31_12_2017 = PostgresOperator(
    task_id = 'fill_31_12_2017',
    postgres_conn_id = 'postgres_default',
    sql = """
    truncate table dm.dm_account_balance_f;
    insert into dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    select 
        '2017-12-31' as on_date,
        account_rk,
        balance_out,
        balance_out * coalesce(er.reduced_cource, 1) as balance_out_rub
    from ds.ft_balance_f bf
    left join ds.md_exchange_rate_d er on er.currency_rk = bf.currency_rk and er.data_actual_date = '2017-12-31';
    """,
    dag=dag
)

start >> extract_and_load >> fill_dm_account_turnover_task >> fill_dm_account_balance_31_12_2017 >> fill_dm_account_balance_f_task >> end