from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# # mongo clients libs
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

# # Generic imports
import glob
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from json import load
from datetime import datetime, timedelta
from time import mktime
from dateutil.relativedelta import relativedelta
import ast
import re
import bee_data_cleaning as dc
from bee_dataframes import create_dataframes
from edinet_models.edinet_models import baseline_calc_pyemis_old, baseline_calc_pyemis_new, monthly_calc

class MRJob_align(MRJob):
     
    INTERNAL_PROTOCOL = PickleProtocol
    
    def mapper_init(self):
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        self.mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo[self.config['mongodb']['db']].authenticate(
            self.config['mongodb']['username'],
            self.config['mongodb']['password']
        )
        self.devices = self.config['devices']
        self.task_id = self.config['task_id']

    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))

        self.mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
                )

        self.company = self.config['company']
        self.devices = self.config['devices']
        self.stations = self.config['stations']
        self.task_id = self.config['task_id']

        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read   

        # emits modelling_units as key
        # emits deviceId, consumption, ts
        try:
            ret = doc.split('\t')
            modelling_units = self.devices[str(ret[0])]
            d = {
                'deviceid': ret[0],
                'date': datetime.fromtimestamp(float(ret[1])),
                'energyType': ret[3],
                'source': ret[4]
                }

            try:
                d['value'] = float(ret[2])
            except:
                d['value'] = None
            try:
                d['temperature'] = float(ret[5])
            except:
                d['temperature'] = None

            for modelling_unit in modelling_units:
                yield modelling_unit, d
        except Exception as e:
            pass
    
    def reducer(self, key, values):
        # obtain the needed info from the key
        modelling_unit, multipliers = key.split('~')
        multipliers = ast.literal_eval(multipliers) #string to dict
        multiplier = {}
        for i in multipliers:
            multiplier[i['deviceId']] = i['multiplier']

        # create dataframe from values list
        df = pd.DataFrame.from_records(values, index='date', columns=['value','temperature','date','deviceid','energyType','source'])

        df = df.sort_index()
        #test only Inergy
        df = df[df.source=="3230658933"]
        grouped = df.groupby('deviceid')
        # has to multiply each modelling unit values by multiplier and add them all:
        df_new_daily = None
        df_weather = None
        for device, data in grouped:
            if data.empty:
                continue
            data = data[~data.index.duplicated(keep='last')]
            if device not in multiplier.keys():
                continue
            if df_new_daily is None:
                df_new_daily = data[['value']] * multiplier[device]
            else:
                df_new_daily += data[['value']] * multiplier[device]
            df_weather = data[['temperature']]

        if df_new_daily.empty or df_new_daily is None or df_weather.empty or df_weather is None:
            return
        df_new_daily = df_new_daily.dropna()

        monthly_baseline = monthly_calc(modelling_unit, df_weather, self.company, multipliers, df_new_daily)


        baseline = {
            'companyId': int(self.company),
            'devices': str(multipliers),
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

        #baseline.update(monthly_baseline)
        baseline.update(monthly_baseline)
        mongo = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['collection']]

        mongo.update(
            {'modellingUnitId': modelling_unit, 'companyId': int(self.company)},
            {"$set": baseline},
            upsert=True
        )


if __name__ == '__main__':
    MRJob_align.run()    