from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# mongo clients libs
from pymongo import MongoClient

# Generic imports
import glob
import pandas as pd
import numpy as np
from json import load
from datetime import datetime





class MRJob_align(MRJob):
     
    INTERNAL_PROTOCOL = PickleProtocol
    
    def reducer_init(self):
        
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        
        self.mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
                )
              
                
        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read
        # emits stationId as key
        # emits value and ts as values
        ret = doc.split('\t')
        d = {'date': datetime.fromtimestamp(float(ret[1])) }
        d['lat'] = ret[3]
        d['long'] = ret[4]
        d['altitude'] = ret[5]
        try:
            d['value'] = float(ret[2])
        except ValueError, e:
            d['value'] = np.nan
        
        yield ret[0], d
    
    
    def reducer(self, key, values):
        # create  dataframe from values list
        v = []
        for i in values:
            v.append(i)
        df = pd.DataFrame.from_records(v, index='date', columns=['value','date','lat','long','altitude'])
        
        # extract location info and remove it from df
        lat = df.lat[0]
        long = df.long[0]
        altitude = df.altitude[0]
        df = df.drop('lat', axis=1)
        df = df.drop('long', axis=1)
        df = df.drop('altitude', axis=1)
        
        # sort
        df = df.sort_index()
        # resampling every 30 minutes 
        df = df.resample('30T').mean().interpolate()
        #yield 5, '%s' % df
        #yield 51, '%s' % df.value

            
        # final lists
        ts_list = []
        value_list = []
        for k, value in df.iterrows():
            ts_list.append(k)
            value_list.append(value.value)


         
        self.mongo[self.config['mongodb']['db']][self.config['mongodb']['weather_collection']].update(
            {'stationId': key},
            {
                '_created': datetime.now(),
                'stationId': key,
                'timestamps': ts_list,
                'values': value_list,
                'altitude': altitude,
                'GPS': {
                        'lat': lat,
                        'long': long
                        } 
            },                                                                                                    
            upsert = True
        )
            
        
            
if __name__ == '__main__':
    MRJob_align.run()    