from abc import abstractmethod, abstractproperty, abstractclassmethod, ABC
import datetime as dt #Might want to be specific around the functions we need

class Dew(ABC):
  
  def __init__(self):
    pass

  @abstractmethod
  def load(self):
    pass
  
  def add_bronze_metadata_cols(self, spark, df):
    df = (df.withColumn("source_file_name", input_file_name())
        .withColumn("bronze_loaded_timestamp_utc", current_timestamp())
        .withColumn("date_key", lit(f"{dt.datetime.now():%Y%m%d}")))
    print("transform_raw functions loaded")
    return (df)