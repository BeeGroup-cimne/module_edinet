from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# mongo clients libs
from pymongo import MongoClient, ASCENDING, DESCENDING

# Generic imports
import glob
import pandas as pd
from json import load
from datetime import datetime
import ast
from mrjob.protocol import RawValueProtocol 
from calendar import monthrange
import json
from bee_dataframes import create_dataframes

class MRJob_align(MRJob):
     
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    
    
    def mapper_init(self):  
                
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))      
        self.devices = self.config['devices']

    
    
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
        self.fields = self.config['fields'] 
                
        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read   
         
        # emits modelling_units as key
        # emits deviceId, consumption, ts as values
        ret = doc.split('\t')
        d = {
             'deviceid': ret[0],
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
        modelling_units = self.devices[str(ret[0])]
        for modelling_unit in modelling_units:
            yield modelling_unit, d

    
    def reducer(self, key, values):
        # obtain the needed info from the key 
        modelling_unit, multipliers, model = key.split('~')
        # find the building inside the mongo
        buildingID = None
        mongo_building = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['buildings_collection']]
        mongo_reporting = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['reporting_collection']]
        try: 
            #primer mirem si el podem aconseguir directament( Edificis BenchAPP)
            building_item=mongo_building.find_one({"modellingUnits":modelling_unit, "companyId":self.company})
            if building_item and "buildingId" in building_item:
                    buildingID = building_item['buildingId']
            else:
                #Sino, ho mirem al reporting unit (Edificis Dashboard)
                reporting_item = mongo_reporting.find_one({"modelling_Units":modelling_unit, "companyId":self.company})
                if reporting_item and "buildingId" in reporting_item:
                    buildingID = reporting_item['buildingId']
        except Exception as e:
            print(e)
        
        
        multipliers = ast.literal_eval(multipliers)            # string to dict
        multiplier = {}
        for i in multipliers:
            multiplier[i['deviceId']] = i['multiplier']
        
        
        # create dataframe from values list
        v = []
        for i in values:
            v.append(i)
        df = pd.DataFrame.from_records(v, index='date', columns=['value','accumulated','date','deviceid','energyType'])
        df = df.sort_index()

        # apply the multiplier over each deviceId value and sum all the values 
        grouped = df.groupby('deviceid')

        df_new = create_dataframes.create_daily_dataframe(grouped, multiplier)

        #final dataframe with monthly data and number of days within each month
        df_new.dropna(inplace=True)
        if df_new.empty != True:
            number_of_days = df_new.groupby(pd.TimeGrouper(freq='M')).count()
            number_of_days.rename(columns={'value': 'numberOfDays'},inplace=True)
            df_new = df_new.groupby(pd.TimeGrouper(freq='M')).sum()
            final_df = df_new.join(number_of_days)
            final_df.dropna(inplace=True)
        else:
            final_df = pd.DataFrame()

        # get building info to obtain the criterias
        building_doc = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['buildings_collection']].find_one(
           {'buildingId': buildingID, 'companyId': int(self.company)},
           {'address': True, 'data':True, 'entityId':True }
        )
#         # haig the buscar a reporting_units i modelling_units 
#         if not building_doc:
#             doc = self.mongo[self.config['app']['mongodb']['db']][self.config['app']['mongodb']['reporting_units_collection']].find_one(
#                             {'modelling_Units': {'$elemMatch': {'$eq':modelling_unit}}, 'companyId': int(self.company)})
#             buildingId = doc['buildingId'] if doc and 'buildingId' in doc else None
#             if buildingId:
#                 building_doc = self.mongo[self.config['app']['mongodb']['db']][self.config['app']['mongodb']['buildings_collection']].find_one(
#                         {'buildingId': buildingId, 'companyId': int(self.company)},
#                         {'address': True, 'data':True, 'entityId':True } )
#             
#                 doc = self.mongo[self.config['app']['mongodb']['db']][self.config['app']['mongodb']['modelling_units_collection']].find_one(
#                             {'modellingUnitId': modelling_unit, 'companyId': int(self.company)})
#                 location = doc['location'] if doc and 'location' in doc else None
#                 building_doc['location'] = location
        
        #resultats finals
        if not building_doc:
            pass
        else:         
            # find the different criteria fields
            criterias = []
            for field in self.fields: 
                find = False
                if field in building_doc.keys():
                    criterias.append(building_doc[field])
                    find = True
                else:
                    for key in building_doc.keys():
                        if isinstance(building_doc[key],dict) and field in building_doc[key].keys():
                            criterias.append(building_doc[key][field])
                            find = True
                if find == False:  # if the field is not within the document
                     criterias.append('null')
            
            # find the surface
            if 'data' in building_doc and 'areaBuild' in building_doc['data']:
                surf = building_doc['data']['areaBuild']
            else:
                surf = None    

            # final lists
            res = []
            for k, value in final_df.iterrows():
                res_parcial = {}
                res_parcial["v_surf"] = (value.value / surf) if surf else "null"
                res_parcial["v"] = value.value
                res_parcial["m"] = int(k.strftime('%Y%m'))
                res_parcial["rd"] = int(value.numberOfDays)
                res_parcial["td"] = monthrange(k.year,k.month)[1]
                res.append(res_parcial)        

            yield modelling_unit, modelling_unit + '\t' + json.dumps(res) + '\t' + '\t'.join(criterias)
                
    
    
if __name__ == '__main__':
    MRJob_align.run()    
