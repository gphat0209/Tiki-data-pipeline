from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define a simple function to run as a task
def print_hello():
    print("Hello, Airflow!")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'my_first_dag',  # DAG ID
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),  # Start date for DAG
    catchup=False,
) as dag:

    # Define tasks
    start = DummyOperator(task_id='start')
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )
    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> hello_task >> end
