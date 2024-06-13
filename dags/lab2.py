from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

def fetch_data_handler(**kwargs):
    hook = PostgresHook(postgres_conn_id='demo-db')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM solardata;')
    data = cursor.fetchall()
    cursor.close()
    conn.close()

    kwargs['ti'].xcom_push(key='data', value=data)

def display_data_handler(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    print("Отримані дані:")
    print(data)

def filter_and_aggregate_handler(**kwargs):
    data = kwargs['ti'].xcom_pull(key="data")
    filtered_data = [row for row in data if 2018 <= row[2] <= 2020]

    sum_first_column = sum(row[0] for row in filtered_data)
    print(f"Від фільтровані і агриговані дані {sum_first_column}")


with DAG(
    'example_postgres_python_v10',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    schedule_interval='@once',
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_handler,
        provide_context=True,
    )

    display_data = PythonOperator(
        task_id='display_data',
        python_callable=display_data_handler,
        provide_context=True,
    )
    
    filter_and_aggregate_data = PythonOperator(
        task_id='filter_and_aggregate',
        python_callable=filter_and_aggregate_handler,
        provide_context=True,
    )

    fetch_data >> display_data
    fetch_data >> filter_and_aggregate_data
