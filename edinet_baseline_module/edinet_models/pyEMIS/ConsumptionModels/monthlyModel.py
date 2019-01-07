import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from baseModel import baseModel, ModellingError as me
from nanModel import NanModel
from ..DataCleaning import utils
import logging

class ModellingError(me): pass

class MonthlyModelFactory(object):
    def __init__(self, factory, resolution):
        self.resolution = resolution
        self.factory = factory

    def __call__(self, data):
        model = MonthlyModel(self.factory, self.resolution)
        model(data)
        return model

class MonthlyModel(baseModel):
    def __init__(self, factory, resolution):
        """Configure the Monthly elements of the model """
        self.logger = logging.getLogger('pyEMIS:Models:Monthly')
        self.resolution = resolution
        self._factory = factory

    def __call__(self, data):
        """
        Mimicking the baseModel __init__ function, i.e. fit the model
        The data are split into subsets (the number of subsets is dependent on the resolution)
        """
        datetimes = utils.datetime_from_timestamp(data['timestamp'])                                            #datetimes for each record
        _keys = np.array(['1'])                                                                                 #A list of unique keys for the model
        formats = np.tile('1', len(datetimes))                                                                  #Keys mapped to the datetimes
        self._models = dict([(key, self._factory(data[formats==key])) for key in _keys])                        #dictionary of models with formats as keys
        self.n_parameters = sum([self._models[key].n_parameters for key in self._models.keys()])                #add up all the parameters

    def parameters(self):
        return [self._models[key].parameters() for key in self._models.keys()]

    def prediction(self, independent_data):
        self.logger.debug('Calculating prediction')
        datetimes = utils.datetime_from_timestamp(independent_data['timestamp'])
        formats = np.tile('1', len(datetimes))
        result = np.zeros(independent_data.shape)
        for key in self._models.keys():
            indices = formats==key
            result[indices] = self._models[key].prediction(independent_data[indices])
        return result

    def percentiles(self, independent_data, percentiles, expand=True):
        self.logger.debug('Calculating percentiles')
        result = np.zeros(independent_data.shape, dtype=np.dtype([(str(p), np.float64) for p in percentiles]))
        datetimes = utils.datetime_from_timestamp(independent_data['timestamp'])
        formats = np.tile('1', len(datetimes))
        for key in self._models.keys():
            indices = formats==key
            chunk = self._models[key].percentiles(independent_data[indices], percentiles)
            for p in chunk.keys():
                result[p][indices] = chunk[p]
        return result