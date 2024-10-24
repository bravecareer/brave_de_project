#this must be stored in ~/airflow/dags
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 13),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'dim_product_dag_example2',
    default_args=default_args,
    description='Just one model - dim_product_arsen_c',
    schedule_interval=timedelta(days=1),
) as dag:

    dbt_run = BashOperator(
        task_id='dim_product_dag_example2',
        bash_command='source bravede/bin/activate && cd /Users/arsenchuzhykov/Desktop/brave_de_project/brave_de_project && dbt run --select +dim_product_arsen_c',
        env={'DBT_PROFILES_DIR': '/Users/arsenchuzhykov/Desktop/brave_de_project/brave_de_project'},
        dag=dag,
    )
"""