from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from collections import Counter
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
    # Read the text file
    with open('/opt/airflow/data/test.txt', 'r') as file:
        text = file.read()
    
    # Count words
    words = text.lower().split()
    word_counts = Counter(words)
    
    # Convert to DataFrame
    df = pd.DataFrame(list(word_counts.items()), columns=['word', 'count'])
    
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
    df.to_sql('word_counts', engine, if_exists='replace', index=False)

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