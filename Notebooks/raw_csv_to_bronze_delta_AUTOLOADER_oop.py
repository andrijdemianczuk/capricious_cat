# Databricks notebook source
# DBTITLE 1,Remove all widgets if necessary (uncomment)
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create the widgets
dbutils.widgets.text("lib_loc", "/Workspace/Repos/", "DEW Module Location")

dbutils.widgets.text("source", "caseware", "Source")
dbutils.widgets.text("sourceFolderPath", "caseware/*/am", "Source Path")
dbutils.widgets.text("sourceDataset", "*am*.csv", "Source Dataset")
dbutils.widgets.text("sourceOriginalFormat", "csv", "Source Original Format")
dbutils.widgets.text("targetDatabase", "caseware_bronze", "Target Database")
dbutils.widgets.text("targetFolderPath", "/caseware", "Target Path")
dbutils.widgets.text("targetDataset", "trial_balance", "Target Dataset")
dbutils.widgets.text("piiColumns", "null_string", "PII Columns")

# COMMAND ----------

# DBTITLE 1,Get widget vals
lib_loc = dbutils.widgets.get("lib_loc")

source = dbutils.widgets.get("source")
sourcePath = dbutils.widgets.get("sourceFolderPath")
sourceDataset = dbutils.widgets.get("sourceDataset")
sourceOriginalFormat = dbutils.widgets.get("sourceOriginalFormat")
targetDb = dbutils.widgets.get("targetDatabase")
targetPath = dbutils.widgets.get("targetFolderPath")
targetDataset = dbutils.widgets.get("targetDataset")
piiColumns = dbutils.widgets.get("piiColumns")

# COMMAND ----------

# DBTITLE 1,System imports
import sys
import os
import datetime as dt
import hashlib

from delta.tables import *
from  pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

#Add the repo to the sys path and import the module
sys.path.append(os.path.abspath(lib_loc))
import dew

# COMMAND ----------

# DBTITLE 1,Instance the derived class
#Create a new instance of the derived class which inherits the base class functions too
dew_func = dew.DewFn()

# COMMAND ----------

dew_func.read_stream_raw_autoloader()

# COMMAND ----------

# DBTITLE 1,Set the notebook parameters
basePath = "dbfs:/mnt/lake"
rawPath                = basePath + f"/raw/{sourcePath}/{sourceDataset}"
bronzePath             = basePath + f"/bronze/{targetPath}/{targetDataset}"
bronzeCheckpoint       = basePath + f"/bronze/{targetPath}/{targetDataset}/_checkpoint"

# COMMAND ----------

# DBTITLE 1,Set once the autoloader configuration as a dictionary
autoloader_config = {
  "cloudFiles.format": sourceOriginalFormat,
#   "cloudFiles.connectionString": dbutils.secrets.get(scope="AzureKeyVault",key="DatabricksAutoloaderSasKeyConnectionString"),
#   "cloudFiles.resourceGroup": dbutils.secrets.get(scope="AzureKeyVault",key="AnalyticsPlatformResourceGroup"),
#   "cloudFiles.subscriptionId": dbutils.secrets.get(scope="AzureKeyVault",key="SubscriptionId"),
#   "cloudFiles.tenantId": dbutils.secrets.get(scope="AzureKeyVaultAAD",key="DatabricksAdlsTenantId"),
#   "cloudFiles.clientId": dbutils.secrets.get(scope="AzureKeyVaultAAD",key="DatabricksAdlsAppId"),
#   "cloudFiles.clientSecret": dbutils.secrets.get(scope="AzureKeyVaultAAD",key="DatabricksAdlsSecret"), 
  "cloudFiles.includeExistingFiles": "true",
#   "cloudFiles.schemaLocation": bronzePath,
  "cloudFiles.inferColumnTypes": "false",
  "cloudFiles.schemaEvolutionMode": "rescue"
#   "cloudFiles.useNotifications": "true"
  }

# COMMAND ----------

# MAGIC %md
# MAGIC Question for MNP: Do the encrypted cols need to be decrypted or can they simply be redacted?
# MAGIC 
# MAGIC We have 3 options:
# MAGIC 1. Replace with nulls
# MAGIC 2. Do it with sha1 using an inline UDF (since the library is out of scope this won't work via api call)
# MAGIC 3. Do it the same way I did for the HLS demo (needs to be revisited)

# COMMAND ----------

# DBTITLE 1,DEBUG
rawPath = "/FileStore/tmp/"
bronzeCheckpoint = "/FileStore/tmp/_checkpoint"

schema = (spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .options(header='true', inferSchema='true')
  .load("dbfs:/FileStore/tmp/Humidity_data_000687ee_a8d9_4ff2_8259_261a7fc1a062.csv")
).schema

# COMMAND ----------

# DBTITLE 1,DEBUG
# df = (spark.readStream
#   .format("cloudFiles")
#   .schema(schema)
#   .option("cloudFiles.format", "csv")
#   .option("maxFilesPerTrigger", 1)
#   .load(inputPath))

# df.isStreaming

# COMMAND ----------

# DBTITLE 1,DEBUG
# display(df)

# COMMAND ----------

# DBTITLE 1,#Option1 Load, redact and write the feed - using nullout in lieu of sha1 per row
# #Read stream

#Used by Andrij for debugging (remove schema=schema if passing the schema loc in options[])
rawDf = dew_func.read_stream_raw_autoloader(spark=spark, autoloader_config=autoloader_config, rawPath=rawPath, schema=schema) 
# rawDf = dew_func.read_stream_raw_autoloader(spark=spark, autoloader_config=autoloader_config, rawPath=rawPath)

# #PII col nullout
if piiColumns != "null_string": rawDf = dew_func.nullout_cols(piiColumns=piiColumns, df=rawDf)

# #Add metadata cols
bronzeReadyDf = dew_func.add_bronze_metadata_cols(spark=spark, df=rawDf)

# #Bronze write-once
# dew_func.write_stream_bronze_delta_trigger_once(spark=spark, df=bronzeReadyDf, bronzePath=bronzePath, bronzeCheckpoint=bronzeCheckpoint)

# COMMAND ----------

display(bronzeReadyDf)

# COMMAND ----------

# DBTITLE 1,#Option 2 if we *need* the UDF for encryption
#NOTE: Only *if* the encrypted cols need to be decrypted otherwise we'd simply redact the fields (which can't be undone.)

# @udf("String")
# def encrypt_col(pii_col):
  
#   sha_value = hashlib.sha1(pii_col.encode()).hexdigest()
  
#   return sha_value

#read the initial dataframe - Finished
#rawDf = dew_func.read_stream_raw_autoloader(spark=spark, autoloader_config=autoloader_config, rawPath=rawPath)

#PII Columns are coming from the widgets -->
# if piiColumns != "null_string":

#   pii_cols = piiColumns.replace(' ', '').split(',')

#   for c in pii_cols:
#     bronzeReadyDf = bronzeReadyDf.withColumn(c, coalesce(c, lit('null'))).withColumn(c, encrypt_col(c))

#Add metadata cols - Finished
#bronzeReadyDf = dew_func.add_bronze_metadata_cols(spark=spark, df=rawDf)

#Execute exactly once the modified dataframe to delta
#dew_func.write_stream_bronze_delta_trigger_once(spark=spark, df=bronzeReadyDf, bronzePath=bronzePath, bronzeCheckpoint=bronzeCheckpoint)
