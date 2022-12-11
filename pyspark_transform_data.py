from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofmonth, hour, mean, date_format, dayofyear
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, FloatType, StructType, StructField

spark_session = SparkSession.builder.getOrCreate()

csv_schema = StructType([
    StructField("Date", DateType(), nullable=True),
    StructField("Time", TimestampType(), nullable=True),
    StructField("Global_active_power", FloatType(), nullable=True),
    StructField("Global_reactive_power", FloatType(), nullable=True),
    StructField("Voltage", FloatType(), nullable=True),
    StructField("Global_intensity", FloatType(), nullable=True),
    StructField("Sub_metering_1", FloatType(), nullable=True),
    StructField("Sub_metering_2", FloatType(), nullable=True),
    StructField("Sub_metering_3", FloatType(), nullable=True),
])

# print(csv_schema)

spark_titanic_df = spark_session.read.option("dateFormat", "d/M/y").schema(csv_schema).csv("dataset/household_consumption_s3.csv", header=True, sep=";").cache()

# print(spark_titanic_df.dtypes)

# Remove nan values
spark_titanic_df = spark_titanic_df.dropna()

# Group by day_of_year
spark_mean_voltage_df = spark_titanic_df.groupBy(dayofyear("Date").alias("Date")).agg(mean("Voltage").alias("mean"))

# # Return proportion of active/reactive
# def power_proportion(row):
#     global_active_power = row.Global_active_power
#     global_reactive_power = row.Global_reactive_power
#     return global_active_power / global_reactive_power if global_reactive_power else 0
#
# # Add global active / reactive column
# spark_titanic_rdd = spark_titanic_df.select('Global_active_power', 'Global_reactive_power').rdd
# spark_power_rdd = spark_titanic_rdd.map(lambda row : power_proportion(row))
# spark_df = spark.createDataFrame([spark_power_rdd], FloatType())


# spark_power_df.show()

# Print values
# for element in spark_power_rdd.collect():
#     print(element)


spark_mean_voltage_df.orderBy("Date").show()
spark_titanic_df.show()

# Write in csv
spark_mean_voltage_df.orderBy("Date").write.option("header", True).csv("dataset/household.csv")



