#This is a class because it stores its model parameters and has a 'prediction' function which returns predictions for input data
import numpy as np
from baseModel import baseModel, ModellingError as me
from datetime import datetime
import pandas as pd

class ModellingError(me): pass

class ConstantMonthlyModel(baseModel):
  """
  A constant consumption model: consumption is estimated as the average of all input data
  Input_data must respond to the method call 'consumption' 
  """
  
  n_parameters = 1
  
  def __init__(self, data):
    if len(data) <= 11:#(self.n_parameters + 2):
        self.mean = np.nan
        self.std = np.nan
        #raise ModellingError, "Not enough input data"
    if 'temperature' in data.dtype.names:
        x = data['temperature']
        self.xrange = [min(x), max(x)]
    data_pd = pd.DataFrame.from_records(data)
    data_pd['ts'] = data_pd['timestamp'].apply(datetime.fromtimestamp)
    data_pd = data_pd.set_index(pd.DatetimeIndex(data_pd['ts']))
    data_pd.sort_index(inplace=True)
    
    last_month = data_pd[-1:].index.month+1 if data_pd[-1:].index.month != 12 else 1
    
    self.mean = data_pd[data_pd.index.month==last_month]['consumption'].mean()
    self.std = data_pd[data_pd.index.month==last_month]['consumption'].std()

  def prediction(self, independent_data):       
    return np.array([self.mean] * len(independent_data))

  def simulation(self, independent_data):
      return self.std * np.random.randn(independent_data.size) + self.mean

  def parameters(self):
    return {'mean': self.mean, 'std': self.std}