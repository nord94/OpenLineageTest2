from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count, current_timestamp
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def count_words():
    # Initialize Spark session with proper configuration
    spark = SparkSession.builder \
        .appName("WordCount") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "airflow-worker") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Read the text file using the path mounted in Airflow
        text_df = spark.read.text('/opt/airflow/data/test.txt')
        
        # Split into words and count
        word_counts = text_df \
            .select(explode(split('value', ' ')).alias('word')) \
            .select('word') \
            .groupBy('word') \
            .agg(count('*').alias('count'))
        
        # Convert to pandas for PostgreSQL insertion
        pdf = word_counts.toPandas()
        
        # Save to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS word_counts (
            word VARCHAR(255) PRIMARY KEY,
            count INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)
        
        # Insert data
        pdf.to_sql('word_counts', engine, if_exists='replace', index=False)
    
    finally:
        # Always stop Spark session
        spark.stop()

with DAG(
    'word_count_dag',
    default_args=default_args,
    description='A DAG to count words in a text file and store in DWH',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['word_count', 'dwh'],
) as dag:

    count_words_task = PythonOperator(
        task_id='count_words',
        python_callable=count_words,
    )

    count_words_task 