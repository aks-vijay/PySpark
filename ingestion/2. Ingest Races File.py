# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("circuitId", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("date", DateType(), nullable=True),
    StructField("time", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

# reader API has option and schema (optional ones)
races_df = spark.read \
            .option("header", True) \
            .schema(schema=races_schema) \
            .csv("/mnt/azstoragedatabricksgen2/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit, to_timestamp
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn(
                                            "race_timestamp", 
                                            to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')
                                            )

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), 
                                                    col("year").alias("race_year"), 
                                                    col("round"), 
                                                    col("circuitId").alias("circuit_id"), 
                                                    col("name"), 
                                                    col("ingestion_date"), 
                                                    col("race_timestamp"))

# COMMAND ----------

# partition by => Can be accessed from Writer API 
# creates separate folders based on column

races_selected_df.write \
    .mode("overwrite") \
    .partitionBy("race_year") \
    .parquet("/mnt/azstoragedatabricksgen2/processed/races")

# COMMAND ----------


