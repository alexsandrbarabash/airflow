from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def print_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    print("Отримані дані:")
    print(data)
    # for row in data:
    #     print(row)

with DAG(
    'example_postgres_python_v3',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    schedule_interval='@once',
) as dag:
    fetch_data = PostgresOperator(
        task_id='fetch_data_v2',
        postgres_conn_id='demo-db',
        sql='SELECT * FROM solardata;',
        database='demo-db'

    )

    display_data = PythonOperator(
        task_id='display_data',
        python_callable=print_data,
        provide_context=True,
    )

    fetch_data >> display_data
