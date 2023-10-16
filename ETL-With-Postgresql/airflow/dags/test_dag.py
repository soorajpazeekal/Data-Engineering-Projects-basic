import __init__

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def test1_function():
    print("Task-1 Complete")

def test2_function():
    print("Task-2 Complete")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

# Create the DAG instance
dag = DAG(
    'testing-dag',
    default_args=default_args,
    schedule_interval='@daily',
    description='An example DAG with two Python tasks',
)

task_1 = PythonOperator(
    task_id="compress_to_gz",
    python_callable=test1_function,
    dag=dag,
)

# Define the second Python task using PythonOperator
task_2 = PythonOperator(
    task_id="extract_and_validate",
    python_callable=test2_function,
    dag=dag,
)

# Set up the dependencies between the tasks
task_1 >> task_2 
