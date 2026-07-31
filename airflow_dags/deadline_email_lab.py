from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.email import send_email

# Function to handle email notification
def notify_on_timeout(context):
    """
    This function sends an email when the task exceeds its deadline/SLA.
    In Airflow 3.X, this is linked via on_deadline_miss_callback.
    In Airflow 2.X, we use on_sla_miss_callback for the same logic.
    """
    task_id = context['task_instance'].task_id
    subject = f"Alert: Task {task_id} missed its deadline"
    body = f"The task {task_id} took longer than the defined 5 seconds to complete."
    
    # Send the email
    send_email(
        to="kris.shuyi.l@gmail.com", 
        subject=subject, 
        html_content=body
    )

# DAG Definition
with DAG(
    dag_id="deadline_email_lab",
    start_date=datetime(2026, 3, 1),
    schedule=None,  # Manual trigger
    catchup=False,
) as dag:

    # This task is designed to fail the deadline (sleep 20s > deadline 5s)
    test_task = BashOperator(
        task_id="timeout_test_task",
        bash_command="sleep 20",
        # Use 'sla' to avoid TypeError in current stable version
        # Represents the 'deadline' concept in Airflow 3.x
        sla=timedelta(seconds=5),
        on_sla_miss_callback=notify_on_timeout,
    )
