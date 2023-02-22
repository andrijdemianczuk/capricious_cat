from abc import abstractmethod, abstractproperty, ABC
from dew_base import DewStream
from pyspark.sql.functions import input_file_name, current_timestamp, lit, coalesce, col

import datetime as dt

class DewFn(DewStream):

  def __init__(self):
    pass
  

  ########### USING THE ABSTRACT BASE CLASS INHERITED METHODS AS-IS
  def read_stream_raw_autoloader(self, 
    spark="{spark_context}", 
    autoloader_config:dict={}, 
    rawPath:str="",
    schema:str=""):
    
    df =  (super().read_stream_raw_autoloader(spark=spark, 
      autoloader_config=autoloader_config, 
      rawPath=rawPath, schema=schema))

    return df


  def  write_stream_bronze_delta_trigger_once(self, 
    spark="{spark_context}", 
    df="{dataframe_struct}", 
    key:str="key", 
    bronzePath="/",
    bronzeCheckpoint:str="/", 
    mergeSchema:str="true", 
    mode:str="append"):

    df = (super().write_stream_bronze_delta_trigger_once(spark=spark, 
        df=df, 
        key=key, 
        bronzePath=bronzePath,
        bronzeCheckpoint=bronzeCheckpoint, 
        mergeSchema=mergeSchema, 
        mode=mode))

    return df 


  ########### EXTENDING THE BASE CLASS WITH NEW METHODS
  def nullout_cols(self, 
    piiColumns:str="", 
    df="{dataframe_struct}"):
    
    pii_cols = piiColumns.replace(' ', '').split(',')

    for c in pii_cols:
      df = (df
        .withColumn(c, coalesce(c, lit('null')))
        .withColumn(c, lit('null')))

    return df


  def add_bronze_metadata_cols(self, 
    spark="{spark_context}", 
    df="{dataframe_struct}"):
    
    df = (df
      .withColumn("source_file_name", input_file_name())
      .withColumn("bronze_loaded_timestamp_utc", current_timestamp())
      .withColumn("date_key", lit(f"{dt.datetime.now():%Y%m%d}")))
    
    print("transform_raw functions loaded")
    return(df)
