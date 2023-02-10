# Databricks notebook source
# DBTITLE 1,Remove all widgets if necessary (uncomment)
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create the widgets
dbutils.widgets.text("lib_loc", "/Workspace/Repos/", "DEW Module Location")

dbutils.widgets.text("source", "caseware", "Source")
dbutils.widgets.text("sourceFolderPath", "/caseware/*/am", "Source Path")
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

from delta.tables import *
from  pyspark.sql.functions import *
import datetime as dt

# COMMAND ----------

# DBTITLE 1,Add repo location
sys.path.append(os.path.abspath(lib_loc))

# COMMAND ----------

# DBTITLE 1,Import our custom library
import dew

# COMMAND ----------

# DBTITLE 1,Create an instance for the dew function based off the derived class
dew_func = dew.DewFn()

# COMMAND ----------

# DBTITLE 1,DEBUG - reload the module during development
import importlib
importlib.reload(dew)

dew_func = dew.DewFn()

# COMMAND ----------

# DBTITLE 1,DEBUG - test dataframe
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

dfFromData2 = spark.createDataFrame(data).toDF(*columns)

# COMMAND ----------

# DBTITLE 1,DEBUG - test to see if the dataframe reference is being passed
bronzeReadyDf = dew_func.add_bronze_metadata_cols(spark, dfFromData2)

display(bronzeReadyDf)

# COMMAND ----------

# DBTITLE 1,Set the notebook parameters
basePath = "dbfs:/mnt/lake"
rawPath                = basePath + f"/raw/{sourcePath}/{sourceDataset}"
bronzePath             = basePath + f"/bronze/{targetPath}/{targetDataset}"
bronzeCheckpoint       = basePath + f"/bronze/{targetPath}/{targetDataset}/_checkpoint"

# COMMAND ----------

# DBTITLE 1,Load, redact and write the feed
#read the initial dataframe
rawDf = dew_func.read_stream_raw_autoloader()

#PII Columns are coming from the widgets -->
if piiColumns != "null_string":
  rawDf = dew_func.encrypt_cols()

#Add metadata cols - Finished
bronzeReadyDf = dew_func.add_bronze_metadata_cols(spark=spark, df=rawDf)

#Execute exactly once the modified dataframe to delta
dew_func.write_stream_bronze_delta_trigger_once()

# COMMAND ----------

# read raw csv's into autoloader stream
# rawDf = read_stream_raw_autoloader(spark = spark
#                                    , fileFormat = "csv"
#                                    , rawPath = rawPath
#                                    , bronzePath = bronzePath
#                                    , includeExisting = "true"
#                                    , inferTypes = "false"
#                                    , schemaMode = "rescue")

# COMMAND ----------

## encrypt values in any pii columns
# if piiColumns != "null_string":
  # rawDf = encrypt_cols(spark, piiColumns, rawDf)

# COMMAND ----------

# add metadata columns
# bronzeReadyDf = add_bronze_metadata_cols(spark = spark
#                                          , df = rawDf)

# COMMAND ----------

# write to bronze delta table
# write_stream_bronze_delta_trigger_once(spark = spark
#                                        , df = bronzeReadyDf
#                                        , bronzePath = bronzePath
#                                        , bronzeCheckpoint = bronzeCheckpoint
#                                        , mergeSchema = "true"
#                                        , mode = "append")
