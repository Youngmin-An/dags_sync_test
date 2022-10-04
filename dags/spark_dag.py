import pendulum
import pathlib

from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from jinja2 import Environment, FileSystemLoader
import os

with DAG(
    dag_id="spark_tag",
    start_date=pendulum.datetime(2022, 8, 30, tz="Asia/Seoul"),
    schedule_interval="@daily",
    catchup=False
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

    templates_dir = os.path.join(os.path.realpath(os.path.dirname(__file__)), '../templates')
    file_loader = FileSystemLoader(templates_dir)
    env = Environment(loader=file_loader)
    template = env.get_template('spark-test.yaml')
    spark_app = {"name": "template-test", "start": "{{ data_interval_start }}", "end": "{{ data_interval_end }}",
                 "feature_id": "2"}
    sa = template.render(spark_app=spark_app)

    clean_data = SparkKubernetesOperator(
        task_id="clean_data",
        application_file=sa,
        namespace="default"
    )  # todo: application file from open storage?

    clean_data_sensor = SparkKubernetesSensor(
        task_id='clean_data_monitor',
        namespace="default",
        application_name="{{ task_instance.xcom_pull(task_ids='clean_data')['metadata']['name'] }}"
    )

    compose_email() >> clean_data >> clean_data_sensor
