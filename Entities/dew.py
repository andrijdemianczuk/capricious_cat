from abc import abstractmethod, abstractproperty, ABC
import datetime as dt #Might want to be specific around the functions we need

class DewStream(ABC):

  def __init__(self):
    self.global_flag = 0
  
  @abstractmethod
  def read_stream_raw_autoloader(self):
    return ("reading stream")

  @abstractmethod
  def  write_stream_bronze_delta_trigger_once(self):
    return ("writing stream")

class DewFn(DewStream):

  def __init__(self):
    pass
  
  def read_stream_raw_autoloader(self):
    #Directly composed from the base class - since we don't need to override for any specific reason we can just use whatever's in the base class.
    return super().read_stream_raw_autoloader()

  def  write_stream_bronze_delta_trigger_once(self):
    pass

  def encrypt_cols(self):
    return ("Encrypting cols")

  def add_bronze_metadata_cols(self, spark, df):
    return ("Adding metadata cols")
    # df = (df.withColumn("source_file_name", input_file_name())
    #     .withColumn("bronze_loaded_timestamp_utc", current_timestamp())
    #     .withColumn("date_key", lit(f"{dt.datetime.now():%Y%m%d}")))
    # print("transform_raw functions loaded")
    # return (df)

#sample calls for testing
k = DewFn()
print(k.read_stream_raw_autoloader())
print(k.add_bronze_metadata_cols("blah", "blah"))
print(k.write_stream_bronze_delta_trigger_once())