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
        
        self.readings_cache = {}
        self.devices_cache = {} #careful: consider 1 Million contracts with 5 devices each it will take 1GB on memory [[ (48*2+100)*5 * 1000000 / 1024 / 1024 = 934 MB  ]]
        
        # open connections
        self.hbase = happybase.Connection(self.config['hbase']['host'], self.config['hbase']['port'])
        self.hbase.open()
        
        self.tables_list = self.hbase.tables()
        
        self.mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
                )


    def add_reading_information(self, doc):
        r = self.readings_cache.get(doc['reading'])
        if not r:
            #r = self.mongo[self.config['app']['mongodb']['db']]['readings'].find_one({'_id': doc['reading']})
            r = self.mongo[self.config['mongodb']['db']]['readings'].find_one({'_id': ObjectId(doc['reading'])})
            self.readings_cache[doc['reading']] = r
        
        if r is None:
            doc['error'] = "No reading information related"
            return doc
        
        doc['reading'] = r
        
        return doc
    
    def build_row_key(self, doc):
        row_key = []
        for element in self.config['hbase_table']['key']:
            try:
                if isinstance(doc[element], unicode):
                    row_key.append(doc[element].encode("utf-8"))
                else:
                    row_key.append(str(doc[element]))
            except Exception as e:
                print(e)
                print(type(doc[element]))
                print(doc[element].encode("utf-8"))
                raise e
            #row_key.append(element)
            
        return "~".join(row_key)


    def datetime_to_timestamp(self, doc, field):
        # Input data is always in UTC and the timestamp stored in HBase must be in UTC timezone.
        doc[field] = calendar.timegm(doc[field].utctimetuple())
        return doc

    def convert_units_to_kilo(self, unit, value):
        conversions = ['w', 'wh', 'varh', 'va', 'var', 'whth']
        conversions_M = ['mw', 'mwh', 'mvarh', 'mva', 'mvar', 'mwhth']
        try:
            # Watts, WattsHour, VoltAmpHour, VoltAmps, VoltAmpsReactive,WattHoursofHeath
            i = conversions.index(unit.lower())
            value /= 1000.0
        except ValueError:
            pass
        try:
            # Watts, WattsHour, VoltAmpHour, VoltAmps, VoltAmpsReactive,WattHoursofHeath
            i = conversions_M.index(unit.lower())
            value *= 1000.0
        except ValueError:
            pass
        return value

    def is_float(self, x):
        try:
            x = float(x)
            return np.isfinite(x)
        except:
            return False

        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read
        
        """
        doc = {
            "timestamp": "2013-11-30 18:00:00",
            "reading": "52a9845fdfeb570207c02319",
            "deviceId": "912062bb-21ec-5787-805d-cf3858c67405",
            "value": 120,
            "companyId": "1234509876"
            }
        """
        
        # create a dictionary from python string
        # use config file uploaded with script
        #doc = self.list_to_doc(line)
        
        # Transform functions
        doc = self.add_reading_information(doc)
        #doc = self.translate_deviceId_contractId(doc)
        
        if 'error' in doc:
            #If there are errors with the doc, save the measure in the amon_measure_measurements_with_errors collection in REST
            doc['reading'] = doc['reading']['_id']
            doc['error_detected_at'] = datetime.now()
            #self.mongo[self.config['app']['mongodb']['db']]['amon_measures_measurements_with_errors'].insert(doc)
            yield 1, str(doc) # yielding records will tell us if something went wrong (output map records should be 0)
            #yield 2, str(doc['query'])
            # customer not found
            #raise Exception("The deviceId %s dont correspond to any contractId" % doc['deviceId'])
            return
        doc = self.datetime_to_timestamp(doc,'timestamp')
        # ROW KEY DEFINITION
        row_key = self.build_row_key(doc)
        
        # Key - Value lists from the values dictionary
        doc_key = ['m:v' if doc['reading']['period']=='INSTANT' else 'm:va']
        doc_val = [self.convert_units_to_kilo(doc['reading']['unit'], doc['value'])]

        # ROW VALUE DEFINITION
        row = {}
        for i in range(len(doc_key)):
            row[doc_key[i]] = str(doc_val[i])

            table_name = "_".join([self.config['hbase_table']['name'], doc['reading']['type'], str(doc['companyId'])])
        
        try:
            if not table_name in self.tables_list:
                self.hbase.create_table(table_name, { 'm': dict() })
                self.tables_list.append(table_name)
        except:
            pass
            
        hbase_table = self.hbase.table(table_name)
            
        hbase_table.put(row_key, row)
        
        #yield row_key, str(row, table_name)  
    
    
    
if __name__ == '__main__':
    Hadoop_ETL.run()    