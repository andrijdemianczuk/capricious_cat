# Databricks notebook source
inputPath = "/FileStore/tmp/"

# COMMAND ----------

df = (spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .options(header='true', inferSchema='true')
  .load("dbfs:/FileStore/tmp/Humidity_data_000687ee_a8d9_4ff2_8259_261a7fc1a062.csv")
)

schema = df.schema

# COMMAND ----------

# from pyspark.sql.functions import *

# inputPath = "/FileStore/tmp/*.csv"


# # Similar to definition of staticInputDF above, just using `readStream` instead of `read`
# streamingInputDF = (
#   spark
#     .readStream
#     .format("csv")
#     .schema(schema)
#     .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
#     .csv(inputPath)
# )

# COMMAND ----------

df = (spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "csv")
  .option("maxFilesPerTrigger", 1)
  .load(inputPath))

# COMMAND ----------

df.isStreaming

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tmp

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tmp/*.csv", True)

# COMMAND ----------

 
