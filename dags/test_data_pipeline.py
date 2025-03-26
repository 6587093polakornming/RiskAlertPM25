import json
from datetime import datetime, date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import requests


def get_posts_report_today():
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    data = response.json()
    with open('data/data.json', 'w') as f:
        json.dump(data, f)

    return data


def save_data_into_db():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    today = datetime.today()

    with open('data.json') as f:
        data = json.load(f)

    for x in data:
        insert = """
            INSERT INTO daily_posts (
                userId,
                postId,
                title,
                body,
                insertedDate)
            VALUES (%s, %s, %s, %s, %s);
        """

        postgres_hook.run(insert, parameters=(x['userId'], x['id'], x['title'], x['body'], today))

def remove_before_into_db():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    today = datetime.today().strftime('%Y-%m-%d')

    delete = f"""DELETE FROM daily_posts WHERE insertedDate='{today}';"""
    postgres_hook.run(delete)

default_args = {
    'owner': 'Polakorn Anantapakorn Ming',
    'start_date': datetime(2025, 3, 26),
}
with DAG('test_data_pipeline_v4',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for testing',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_posts_report_today',
        python_callable=get_posts_report_today
    )

    # t2 = PythonOperator(
    #     task_id='save_data_into_db',
    #     python_callable=save_data_into_db
    # )

    # t3 = PythonOperator(
    #     task_id='remove_before_into_db',
    #     python_callable=remove_before_into_db
    # )


    t1