# Databricks notebook source
# DDL Schema (DataTypes come from Hive) instead of StructType

constructors_schema = """
                        constructorId INT,
                        constructorRef STRING,
                        name STRING,
                        nationality STRING,
                        url STRING 
                      """

constructors_df = spark.read \
                    .schema(schema=constructors_schema) \
                    .json("/mnt/azstoragedatabricksgen2/raw/constructors.json")

# COMMAND ----------

# dropping unwanted columns
from pyspark.sql.functions import col
constructor_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_dropped_df \
                            .withColumnRenamed("constructorId", "constructor_id") \
                            .withColumnRenamed("constructorRef", "constructor_ref") \
                            .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write \
    .mode("overwrite") \
    .parquet("/mnt/azstoragedatabricksgen2/processed/constructors")
