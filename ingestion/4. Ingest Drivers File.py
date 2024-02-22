# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DateType

# since JSON has inner nested JSON. We define the Struct for Inner JSON first
name_schema = StructType(fields=[
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True)
])

# map the Inner Struct to the name field
driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), nullable=False),
    StructField("driverRef", StringType(), nullable=True),
    StructField("number", IntegerType(), nullable=True),
    StructField("code", StringType(), nullable=True),
    StructField("name", name_schema, nullable=True),
    StructField("dob", DateType(), nullable=True),
    StructField("nationality", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(schema=driver_schema) \
    .json("/mnt/azstoragedatabricksgen2/raw/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

drivers_with_columns_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(drivers_df["name.forename"], lit(" "), drivers_df["name.surname"] ))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(drivers_with_columns_df["url"])

# COMMAND ----------

drivers_final_df.write \
    .mode("overwrite") \
    .parquet("/mnt/azstoragedatabricksgen2/processed/drivers")
