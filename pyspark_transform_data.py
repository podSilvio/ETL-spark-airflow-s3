from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth, hour, mean, date_format, dayofyear
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, FloatType, StructType, StructField

from pyspark_schema import csv_schema

# Create Spark Session
spark_session = SparkSession.builder.getOrCreate()

# Read CSV file defined with specific schema
spark_titanic_df = spark_session.read.option("dateFormat", "d/M/y").schema(csv_schema).csv("dataset/household_consumption_s3.csv", header=True, sep=";").cache()

# Remove nan values
spark_titanic_df = spark_titanic_df.dropna()

# Group by day_of_year
spark_mean_voltage_df = spark_titanic_df.groupBy(dayofyear("Date").alias("Date")).agg(mean("Voltage").alias("mean"))

# Print values
spark_mean_voltage_df.orderBy("Date").show()

# Write in csv
spark_mean_voltage_df.orderBy("Date").write.option("header", True).csv("dataset/household.csv")



