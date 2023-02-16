from abc import abstractmethod, abstractproperty, ABC

class DewStream(ABC):

  def __init__(self):
    self.global_flag = 0
  
  @abstractmethod
  def read_stream_raw_autoloader(self, 
    spark="{spark_context}", 
    autoloader_config:dict={}, 
    rawPath:str="",
    schema:str=""):
    
    # return (spark
    #   .readStream
    #   .format("cloudFiles")
    #   .options(**autoloader_config)
    #   .load(rawPath))

    if (schema!=""):
        df = (spark
            .readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilesPerTrigger", 1)
            .options(**autoloader_config)
            .load(rawPath))
    else:
        df = (spark
            .readStream
            .format("cloudFiles")
            .options(**autoloader_config)
            .load(rawPath))

    return df


  @abstractmethod
  def  write_stream_bronze_delta_trigger_once(self, 
    spark="{spark_context}", 
    df="{dataframe_struct}", 
    key:str="date_key",
    bronzePath:str="/", 
    bronzeCheckpoint:str="/",
    mergeSchema:str="true",
    mode:str="append"):
    
    df = (df.writeStream.partitionby(key)
        .format("delta")
        .option("checkpointlocation", bronzeCheckpoint)
        .option("mergeSchema", mergeSchema)
        .outputmMode(mode)
        .trigger("once")
        .start(bronzePath))
    
    return df