import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator



with DAG(
    dag_id="aicns_test_dag",
    start_date=pendulum.datetime(2022,8,30, tz="Asia/Seoul"),
    schedule_interval="@daily"
) as dag:
    """
    Test
    """

    @task
    def compose_email():
        return {
            'subject': 'Server',
            'body': 'Your server executing Airflow is connected from the external IP<br>'
        }

    email_send = EmailOperator(
        task_id="send_email",
        to="youngmin.develop@gmail.com",
        subject="sdd",
        html_content="werwer"
    )

    compose_email() >> email_send
