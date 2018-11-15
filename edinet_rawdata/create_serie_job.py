from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# mongo clients libs
from pymongo import MongoClient, ASCENDING, DESCENDING

# Generic imports
import glob
import pandas as pd
from json import load
from datetime import datetime

class MRJob_create_serie(MRJob):
     
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
        self.company = self.config['company']
              
                
        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read
        # emits deviceId as key
        # emits consumption, ts and stationid from customer
        ret = doc.split('\t')
        d = {
             'date': datetime.fromtimestamp(float(ret[1])),
             'energyType': ret[4]
             }
        try:
            d['value'] = float(ret[2]) 
        except:
            d['value'] = None
        try:
            d['accumulated'] = float(ret[3]) 
        except:
            d['accumulated'] = None
        
        #if ret[0] == '045fcd4e-33d7-5e0d-83ed-83e9c6933747':
        yield ret[0], d
    
    
    def reducer(self, key, values):
        # create  dataframe from values list
        v = []
        for i in values:
            v.append(i)
        df = pd.DataFrame.from_records(v, index='date', columns=['value','accumulated','date','energyType'])
        # sort by timestamp (index of df)
        df = df.sort_index()
#        yield 1, key
        # final lists for energyType
        grouped = df.groupby('energyType')
        for typeEnergy, group in grouped:
            ts_list = []
            value_list = []
            type = None
            for k, value in group.iterrows():
                ts_list.append(k)
                if value.accumulated:
                    type = 'CUMULATIVE' if (type != 'INSTANT' and type != 'ERROR') else 'ERROR'
                    consumption = value.accumulated
                else:
                    type = 'INSTANT' if (type != 'CUMULATIVE' and type != 'ERROR') else 'ERROR'
                    consumption = value.value 
                value_list.append(consumption)
    #         yield 2, "%s" %value_list
    #         yield 3, type
    #         yield 4, "%s" %ts_list
            
            self.mongo[self.config['mongodb']['db']][self.config['mongodb']['raw_collection']].update(
                {'deviceId': key, 'companyId': self.company, 'type': typeEnergy },
                {    
                    'companyId': self.company,
                    'deviceId': key,
                    'type': typeEnergy,
                    'timestamps': ts_list,
                    'values': value_list,
                    'period': type,
                    '_created': datetime.now(),
                    '_updated': datetime.now()
                },                                                                                                    
                upsert = True
            )

    
if __name__ == '__main__':
    MRJob_create_serie.run()    