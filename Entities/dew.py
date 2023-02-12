from abc import abstractmethod, abstractproperty, ABC
from dew_base import DewStream
from pyspark.sql.functions import input_file_name, current_timestamp, lit, coalesce, col, udf

import datetime as dt #Might want to be specific around the functions we need
import hashlib

class DewFn(DewStream):

  def __init__(self):
    pass
  
  def read_stream_raw_autoloader(self, spark="{spark_context}", autoloader_config:dict={}, rawPath:str=""):

    df = (spark.readStream.format("cloudFiles") 
    .options(**autoloader_config)
    .load(rawPath))
      
    return(df)


  def  write_stream_bronze_delta_trigger_once(self):
    
    return super().write_stream_bronze_delta_trigger_once()


  def nullout_cols(self, piiColumns:str="", df="{dataframe_struct}"):
    
    pii_cols = piiColumns.replace(' ', '').split(',')
    
    for c in pii_cols:
      df = df.withColumn(c, coalesce(c, lit('null'))).withColumn(c, lit('null'))

    return df


  def add_bronze_metadata_cols(self, spark="{spark_context}", df="{dataframe_struct}"):
    
    df = df.withColumn("source_file_name", input_file_name()) \
      .withColumn("bronze_loaded_timestamp_utc", current_timestamp()) \
      .withColumn("date_key", lit(f"{dt.datetime.now():%Y%m%d}"))
    
    print("transform_raw functions loaded")
    return(df)
