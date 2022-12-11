from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# In this example, we will run few bash scripts
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
import pendulum

from s3_python_access import get_s3_data

args = {
    'owner': 'Airflow',
}

with DAG(
        dag_id='example_spark_operator',
        default_args=args,
        # schedule=None,
        schedule_interval=None,
        # start_date=pendulum.today('UTC').add(days=-2),
        start_date=days_ago(2),
        tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]

    get_data_from_s3 = PythonOperator(
        task_id="get_data_from_s3",
        python_callable=get_s3_data,
    )

    pyspark_submit_job = SparkSubmitOperator(
        task_id="pyspark_job",
        application="pyspark_transform_data.py",
    )

    # as the Spark (PySpark) script is located in a different directory, it is executed using a BashOperator

    # # Example of Jinja Templating
    # templated_command = dedent(
    #     """
    #     {% for i in range(5) %}
    #         echo "{{ ds }}"
    #         echo "{{ macros.ds_add(ds, 7) }}"
    #     {% endfor %}
    #     """
    # )
    #
    # bash_templated_job = BashOperator(
    #     task_id="templated",
    #     depends_on_past=False,
    #     bash_command=templated_command
    # )

    get_data_from_s3 >> pyspark_submit_job

    # [END howto_operator_spark_submit]
