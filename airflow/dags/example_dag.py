from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments applied to all tasks
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
    dag_id='hello_airflow',
    default_args=default_args,
    description='A simple example DAG',
    schedule=timedelta(days=1),  # Run once a day
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=['example', 'tutorial'],
) as dag:

    # Task 1: Bash command
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Task 2: Another Bash command
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow!"',
    )

    # Task 3: Python function
    def greet(name):
        print(f"Hello {name}! Welcome to Airflow!")
        return f"Greeted {name}"

    python_task = PythonOperator(
        task_id='greet_user',
        python_callable=greet,
        op_kwargs={'name': 'Data Engineer'},
    )

    # Task 4: Bash command with multiple commands
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='echo "Pipeline completed!" && date',
    )

    # Define task dependencies
    # This means: print_date runs first, then print_hello and python_task run in parallel,
    # then cleanup runs after both are done
    print_date >> [print_hello, python_task] >> cleanup