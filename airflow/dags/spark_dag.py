from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import pendulum

from s3_python_access import get_s3_data

args = {
    'owner': 'Airflow',
}

with DAG(
        dag_id='example_spark_operator',
        default_args=args,
        schedule=None,
        start_date=pendulum.today('UTC').add(days=-2),
        tags=['example'],
) as dag:

    get_data_from_s3 = PythonOperator(
        task_id="get_data_from_s3",
        python_callable=get_s3_data,
    )

    pyspark_submit_job = SparkSubmitOperator(
        task_id="pyspark_job",
        application="pyspark_transform_data.py",
    )

    get_data_from_s3 >> pyspark_submit_job
