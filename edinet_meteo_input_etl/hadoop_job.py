import calendar

import pytz
from mrjob.job import MRJob
from mrjob.protocol import PickleValueProtocol

# hbase and mongo clients libs
import happybase
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

# Generic imports
import glob
from json import load
from datetime import datetime
import numpy as np

class Hadoop_ETL(MRJob):
    
    INPUT_PROTOCOL = PickleValueProtocol

    def mapper_init(self):
        
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))

        # open connections
        self.hbase = happybase.Connection(self.config['hbase']['host'], self.config['hbase']['port'])
        self.hbase.open()
        
        self.tables_list = self.hbase.tables()
        
        self.mongo = MongoClient(self.config['mongodb']['host'], int(self.config['mongodb']['port']))
        self.mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
                )

    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))

        # open connections
        self.hbase = happybase.Connection(self.config['hbase']['host'], self.config['hbase']['port'])
        self.hbase.open()

        self.tables_list = self.hbase.tables()

    def build_row_key(self, doc):
        row_key = []
        for element in self.config['hbase_table']['key']:
            row_key.append(str(doc[element]))
        return "~".join(row_key)


    def datetime_to_timestamp(self, doc, field):
        # Input data is always in UTC and the timestamp stored in HBase must be in UTC timezone.
        doc[field] = calendar.timegm(doc[field].utctimetuple())
        return doc

    def is_float(self, x):
        try:
            x = float(x)
            return np.isfinite(x)
        except:
            return False

        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read
        
        """
        doc = {
            "time": "2013-11-30 18:00:00",
            "temperature" : 6.55,
            "precipAccumulation" : 0.0,
            "stationId" : "C6",
            "longitude" : 0.95172,
            "humidity" : 80.0,
            "pressure" : 1022.2,
            "windSpeed" : 20.9,
            "time" : ISODate("2018-12-16T23:00:00.000Z"),
            "latitude" : 41.6566,
            "GHI" : 0.0,
            "windBearing" : 263.5
            }
        """
        
        # create a dictionary from python string
        # use config file uploaded with script
        #doc = self.list_to_doc(line)
        
        doc = self.datetime_to_timestamp(doc,'time')
        # ROW KEY DEFINITION
        row_key = self.build_row_key(doc)
        
        # Key - Value lists from the values dictionary
        cf = self.config['hbase_table']['cf'][0]
        doc_key = ["{}:{}".format(cf['name'],f) for f in cf['fields']]
        doc_val = []
        for f in cf['fields']:
            try:
                doc_val.append(doc[f])
            except:
                doc_val.append(np.NaN)

        # ROW VALUE DEFINITION
        row = {}
        for i in range(len(doc_key)):
            row[doc_key[i]] = str(doc_val[i])

        table_name = self.config['hbase_table']['name']
        
        try:
            if not table_name in self.tables_list:
                self.hbase.create_table(table_name, {cf['name']: dict()})
                self.tables_list.append(table_name)
        except:
            pass
            
        #hbase_table = self.hbase.table(table_name)
            
        #hbase_table.put(row_key, row)
        
        yield doc['stationId'], {"key": row_key, "row": row}
    
    def reducer(self, key, values):
        table_name = self.config['hbase_table']['name']
        hbase_table = self.hbase.table(table_name)
        batch = hbase_table.batch()
        for v in values:
            batch.put(v['key'], v['row'])
        batch.send()
    
if __name__ == '__main__':
    Hadoop_ETL.run()    