import boto3
import pandas as pd
from s3_credentials import aws_configuration

def get_s3_data():

    # Connect to the S3
    s3 = boto3.resource(
        service_name=aws_configuration["service_name"],
        region_name=aws_configuration["region_name"],
        aws_access_key_id=aws_configuration["aws_access_key_id"],
        aws_secret_access_key=aws_configuration["aws_secret_access_key"]
    )

    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)

    # Download file and read from disc
    s3.Bucket('project-s3-spark-airflow-bucket').download_file(Filename='dataset/household_consumption_s3.csv', Key='household_power_consumption.txt')
    df = pd.read_csv('dataset/household_consumption_s3.csv', index_col=0)

    # print(df)
