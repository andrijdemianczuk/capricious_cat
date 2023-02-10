from abc import abstractmethod, abstractproperty, ABC

class DewStream(ABC):

  def __init__(self):
    self.global_flag = 0
  
  @abstractmethod
  def read_stream_raw_autoloader(self):
    return ("reading stream")

  @abstractmethod
  def  write_stream_bronze_delta_trigger_once(self):
    return ("writing stream")