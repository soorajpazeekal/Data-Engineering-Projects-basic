import __init__

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from main import PysparkManager, FileExtractPhase

# Define the function for the first Python task
def task_1_function():
    # Your code for task 1 goes here
    print("Executing Task 1")
    spark = PysparkManager().CreateSparkSession()
    df = FileExtractPhase(spark=spark)
    print(df.printSchema())
    PysparkManager().StopSparkSession(spark)
    return "OK"

# Define the function for the second Python task
def task_2_function():
    # Your code for task 2 goes here
    print("Executing Task 2")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG instance
dag = DAG(
    'example_dag_with_two_tasks',
    default_args=default_args,
    description='An example DAG with two Python tasks',
    schedule_interval=timedelta(days=1),  # Set the schedule interval as per your requirement
)

# Define the first Python task using PythonOperator
task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1_function,
    dag=dag,
)

# Define the second Python task using PythonOperator
task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2_function,
    dag=dag,
)

# Set up the dependencies between the tasks
task_1 >> task_2
