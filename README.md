# ETL-spark-airflow-s3

This project is for educational purpose to learn the basics of Spark, Airflow, S3 technologies and ETL process in general. The goal is to connect all those technologies into simple and functinal project.

The architecture of project is represented like following:

Also, the repository consist of:
* Airflow DAG file that orchestrates the tasks
* S3 python file for accessing S3 data lake using credentials
* PySpark file used for data processing

## Set-up

#### Apache Airflow

First, install [Apache Airflow](https://airflow.apache.org/)

Start the scheduler:  ```airflow scheduler```

Start the web server: ```airflow webserver --port 8080```

Then enable and run specific task!

### Spark

First, install [Spark](https://spark.apache.org/docs/latest/index.html)

Start the Spark Master with : ```spark bin/start-master.sh```

Start the Spark Worker with : ```spark bin/start-worker.sh <MASTER_URL>```

### S3

Create AWS account and connect python script using Boto3 AWS Python Module (or Airflow hook system)




