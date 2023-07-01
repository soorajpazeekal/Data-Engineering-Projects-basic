import __init__

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from main import FileExtractPhase


def my_function():
    df = FileExtractPhase()
    df.printSchema()
    print(df.count())
    return "Works"



dag = DAG(
    dag_id="simple_dag_v1",
    description="A simple DAG with 2 tasks",
    start_date=datetime.now(),
    schedule_interval="@daily",
    catchup=False
)



task1 = PythonOperator(
    task_id="task1",
    python_callable=my_function,
    dag=dag,
)

task2 = PythonOperator(
    task_id="task2",
    python_callable=my_function,
    dag=dag,
)

task1 >> task2
