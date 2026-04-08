# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver (PySpark)
# MAGIC
# MAGIC Read raw odds JSON from S3 (Bronze), normalize into tabular Silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC Configure paths and run in a Databricks job or interactively.

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
#
# spark = SparkSession.builder.getOrCreate()
#
# bronze_path = "s3a://YOUR_BUCKET/bronze/odds/*/*.json"
# silver_path = "s3a://YOUR_BUCKET/silver/odds/"
#
# df = spark.read.option("multiline", "true").json(bronze_path)
# # Example: select, explode bookmakers, write parquet to silver_path
# # df.write.mode("overwrite").parquet(silver_path)

print("Define bronze_path, silver_path, and Spark transforms for your schema.")
