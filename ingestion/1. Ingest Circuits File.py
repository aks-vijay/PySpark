# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read the CSV file using Spark Dataframe Reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField

# specify the fields. Fields should be in array
circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), nullable=False),
    StructField("circuitRef", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lng", DoubleType(), nullable=True),
    StructField("alt", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

# inferSchema - Creates 2 jobs - Not efficient because Spark reads through the data - SLOWS DOWN THE READS
circuits_df = spark.read \
    .option("header", True) \
    .schema(schema=circuits_schema) \
    .csv("/mnt/azstoragedatabricksgen2/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col
circuits_df_selected = circuits_df \
                            .select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_df_renamed = circuits_df_selected \
                        .withColumnRenamed("circuitId", "circuit_id") \
                        .withColumnRenamed("circuitRef", "circuit_ref") \
                        .withColumnRenamed("lat", "latitude") \
                        .withColumnRenamed("lng", "longitude") \
                        .withColumnRenamed("alt", "altitude")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# any column object can be added as a new value
circuits_final_df = circuits_df_renamed.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write \
    .mode("overwrite") \
    .parquet("/mnt/azstoragedatabricksgen2/processed/circuits/")
