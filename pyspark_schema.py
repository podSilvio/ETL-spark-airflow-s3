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