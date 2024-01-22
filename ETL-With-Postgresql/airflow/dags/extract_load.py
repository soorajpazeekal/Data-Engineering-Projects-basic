
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from include.main import PysparkManager, FileExtractPhase, write_database, database_conn_properties, read_database

# Define the function for the first Python task

def task_1_function():
    # Task 1 from file conversion
    print("Executing Task 1")
    from include.gzip_transform import main 
    result = main()
    print("success")



# Define the function for the second Python task
def task_2_function():
    # Your code for task 2 goes here
    print("Executing Task 2")
    spark = PysparkManager().CreateSparkSession()
    df = FileExtractPhase(spark=spark)
    print(df.printSchema())
    PysparkManager().StopSparkSession(spark)


# Define the function for the third Python task
def task_3_function():
    print("Executing Task 3")
    spark = PysparkManager().CreateSparkSession()
    df = FileExtractPhase(spark)
    data_schema = df.schema

    for field in data_schema.fields:
        null_count = df.filter(df[field.name].isNull()).count(); break
    if null_count == 0 and len(df.columns) == 13: 
        print("Data quality check passed")
        PysparkManager().StopSparkSession(spark)
    else:
        print("Data quality check failed")
        PysparkManager().StopSparkSession(spark)
        raise Exception("This task failed")


# Define the function for the fourth Python task
def task_4_function():
    print("Executing Task 4")
    spark = PysparkManager().CreateSparkSession()
    df = FileExtractPhase(spark=spark)
    database_url, properties, config = database_conn_properties()
    try:
        write_database(data_frame=df, table_name="test", database_url=database_url, properties=properties)
        PysparkManager().StopSparkSession(spark)
    except Exception as e:
        print("An error occurred:", str(e))


# Define the function for the fifth Python task
def task_5_function():
    print("Executing Task 5")
    spark = PysparkManager().CreateSparkSession()
    database_url, properties, config = database_conn_properties()
    try:
        df = read_database(spark, table_name="test", database_url=database_url, properties=properties)
        print(df.printSchema())
        PysparkManager().StopSparkSession(spark)
    except Exception as e:
        print("An error occurred:", str(e))



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
    'example_dag_with_four_tasks',
    default_args=default_args,
    description='An example DAG with two Python tasks',
    schedule_interval='@daily',  # Set the schedule interval as per your requirement
)

# Define the first Python task using PythonOperator
task_1 = PythonOperator(
    task_id="compress_to_gz",
    python_callable=task_1_function,
    dag=dag,
)

# Define the second Python task using PythonOperator
task_2 = PythonOperator(
    task_id="extract_and_validate",
    python_callable=task_2_function,
    dag=dag,
)

# Define the third Python task using PythonOperator
task_3 = PythonOperator(
    task_id="data_quaility_checks",
    python_callable=task_3_function,
    dag=dag,
)

# Define the fourth Python task using PythonOperator
task_4 = PythonOperator(
    task_id="write_to_database",
    python_callable=task_4_function,
    dag=dag,
)

# Define the fifth Python task using PythonOperator
task_5 = PythonOperator(
    task_id="read_from_database_final",
    python_callable=task_5_function,
    dag=dag,
)

# Set up the dependencies between the tasks
task_1 >> task_2 >> task_3 >> task_4 >> task_5
