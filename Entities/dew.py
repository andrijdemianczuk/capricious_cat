from abc import abstractmethod, abstractproperty, ABC
from dew_base import DewStream
from pyspark.sql.functions import input_file_name, current_timestamp, lit

import datetime as dt #Might want to be specific around the functions we need

class DewFn(DewStream):

  def __init__(self):
    pass
  
  def read_stream_raw_autoloader(self):
    return super().read_stream_raw_autoloader()

  def  write_stream_bronze_delta_trigger_once(self):
    return super().write_stream_bronze_delta_trigger_once()

  def encrypt_cols(self):
    return ("Encrypting cols")

  def add_bronze_metadata_cols(self, spark="{spark_context}", df="{dataframe_struct}"):
    
    df = df.withColumn("source_file_name", input_file_name()) \
      .withColumn("bronze_loaded_timestamp_utc", current_timestamp()) \
      .withColumn("date_key", lit(f"{dt.datetime.now():%Y%m%d}"))
    
    print("transform_raw functions loaded")
    return(df)
