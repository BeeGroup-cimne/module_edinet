#from myMRJob import myMRJob
from mrjob.job import MRJob
import datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import happybase
import glob
from json import load,loads,dumps
import random
import types
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
from scipy import arange, array, exp
from list_functions import set_dict_from_list

# Function to transform an string variable to boolean
def toBoolean(x):
      return x[0].upper()=='T'


# Function to transform the original list of dictionaries [{'m':201501, 'd':1,'v':3.8},{'m':201501, 'd':2,'v':5.71},...] to
# [{'m':201501, 'w':[24.2,10.4,17.3,13.4,22.5,22.3,19.5]},{'m':201502,'v':[14.2,11.7,12.3,13.6,13.5,12.3,15.5]},...]
def correct_format_rawWeeks(val):
    df = pd.DataFrame(val)
    val_new = []
    for m, idf in df[['d','v']].groupby(df['m']):
        if idf['d'].count()==7:
            val_new.append({
                'm': m,
                'v': [round(i,3) for i in list(idf.sort_values('d')['v'])]
            })
    return val_new


# Function to transform the original list of dictionaries [{'m':201501, 'd':'Mon','h':0,'v':0.23},{'m':201501, 'd':'Mon','h':1,'v':0.251},...] to
# [{'m':201501, 'Mon':[0.23,0.255,...<24 values>], 'Tue':[0.212,0.196,...<24 values>],...},...]
def correct_format_rawDays(val):
    df = pd.DataFrame(val)
    val_new = []
    for m, idf in df[['d','h','v']].groupby(df['m']):
        dict_new={'m': m}
        for d, iidf in idf[['h','v']].groupby(df['d']):
            if iidf['h'].count()==24:
                dict_new.update({d: [round(i,3) for i in list(iidf.sort_values('h')['v'])]})
        val_new.append(dict_new)
    return val_new

# Function to transform the original list of dictionaries [{'m':201501, 't':'High', 'd':31, 'v':3.42},...] to
# [{'m':201501, 'Mon':[0.23,0.255,...<24 values>], 'Tue':[0.212,0.196,...<24 values>],...},...]
def correct_format_rawTimeslots(val):
    df = pd.DataFrame(val)
    val_new = []
    for m, idf in df[['d','t','v']].groupby(df['m']):
        dict_new={'m': m}
        maxd = (datetime.datetime(int(str(m)[:4]),int(str(m)[4:6]),1,0,0,0)+relativedelta(months=1)-relativedelta(seconds=1)).day
        for t, iidf in idf[['d','v']].groupby(df['t']):
            if iidf['d'].count()>10: #At least more than 10 days per month
                aux = iidf.set_index('d')
                aux = aux.reindex(list(xrange(1,maxd+1)))
                dict_new.update({t: [round(float(i),3) if not np.isnan(i) else None for i in list(aux['v'])]})
        val_new.append(dict_new)
        return val_new

# Function to interpolate and extrapolate faulty months in a yearly profile
# example val=[{'m':1,'v':0.8},{'m':2,'v':0.71},...]
def complete_relative_months(val):
    if len(val)<12:
        def extrap1d(interpolator):
            xs = interpolator.x
            ys = interpolator.y
            def pointwise(x):
                if x < xs[0]:
                    return ys[0]+(x-xs[0])*(ys[1]-ys[0])/(xs[1]-xs[0])
                elif x > xs[-1]:
                    return ys[-1]+(x-xs[-1])*(ys[-1]-ys[-2])/(xs[-1]-xs[-2])
                else:
                    return interpolator(x)
            def ufunclike(xs):
                return array(map(pointwise,xs))
            return ufunclike
        df = pd.DataFrame(val)
        f = extrap1d(interp1d(df['m'],df['v'],kind="cubic"))
        months = [item['m'] for item in val]
        for m in range(12): 
            if not m+1 in months:
                val.append({'m':m+1, 'v':float(f([m+1]))})
    return val


class LoadResultsToRESTandHBase(MRJob):
    
    def mapper_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        config = load(open(fn[0]))
        
        # Other configs 
        self.input_fields = config['input_fields']
        self.output_hbase_key = config['output_hbase_key']
        self.output_hbase_fields = config['output_hbase_fields']
        self.output_mongo_key = config['output_mongo_key']
        self.output_mongo_fields = config['output_mongo_fields']
        self.sep = config['sep']
        self.n_reducers = config['n_reducers']
        self.companyId = config['companyId']
        self.type = config['type']
        self.case = config['case']
        self.operation = config['operation']
        self.special_functions_for_input_fields = config['special_functions_for_input_fields']
    
    
    
    def mapper(self, _, line):   #we don't have value -> input protocol pickleValue which means no key is read
        
        # Row definition
        line = line.decode('utf-8').split(self.sep)
        
        try:
            item = {}
            
            # HBase document preparation
            ##################################
            
            # Generate the key of the row
            key_hbase = "~".join( [ str(line[self.input_fields.index(key_var)]) if key_var in self.input_fields else str(eval('self.%s' % key_var)) for key_var in self.output_hbase_key ] )
            
            # Generate the values of the row
            value_hbase = {}
            
            for fields_hbase in self.output_hbase_fields:
                
                #Name of the column
                # If some input variable has to be specified inside the name of the column, introduce a substring specifying the input field between <>. For example, "profile<season>".
                # This will search the input variable 'season' of the row in treatment, and will generate an output name of the HBase column like 'profileSummer' if the 'season' variable of that row is 'summer'.
                if ">" in fields_hbase[0] and "<" in fields_hbase[0]:
                    input_col_in_name = fields_hbase[0][fields_hbase[0].find("<")+1:fields_hbase[0].find(">")]
                    var_in_name = line[self.input_fields.index(input_col_in_name)]
                    name_column_hbase = fields_hbase[0].replace("<%s>" % input_col_in_name, var_in_name[0].upper()+var_in_name[1:])
                else:
                    name_column_hbase = fields_hbase[0]
                
                #Value of the column in the row
                if line[self.input_fields.index(fields_hbase[1])] not in ['NA','na','null','None','\\N','\N','NULL','Inf','-Inf','inf','-inf','NaN','nan','Nan','NAN']:
                    
                    if fields_hbase[2] in ['string','str','int','float']:
                        value_hbase[name_column_hbase] = str(line[self.input_fields.index(fields_hbase[1])]).encode('utf-8')
                    
                    elif fields_hbase[2] in ['json','dict']:
                        val = loads(line[self.input_fields.index(fields_hbase[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_hbase[1]:
                                    val = eval(sp_fun_item[1])(val)
                        elif len(fields_hbase)>3:
                            for array_set_item in fields_hbase[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'orderedby' in array_set:
                                val = sorted(val,key= lambda x:x[array_set['orderedby']])
                        
                        value_hbase[name_column_hbase] = dumps(val).encode('utf-8')
                            
                    elif fields_hbase[2] == 'array':
                        val = loads(line[self.input_fields.index(fields_hbase[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_hbase[1]:
                                    val = eval(sp_fun_item[1])(val)
                        if isinstance(val,types.ListType):
                            array_set={}
                            for array_set_item in fields_hbase[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'value' in array_set and 'orderedby' in array_set:
                                val = [it[array_set['value']] for it in sorted(val, key= lambda x:x[array_set['orderedby']])]
                            elif 'value' in array_set:
                                val = [it[array_set['value']] for it in val]
                    
                        value_hbase[name_column_hbase] = dumps(val).encode('utf-8')
                    
                    elif fields_hbase[2] in ['get_smaller_item_in_dict','get_bigger_item_in_dict']:
                        position_item = 0 if 'smaller' in fields_hbase[2] else -1
                        val = loads(line[self.input_fields.index(fields_hbase[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_hbase[1]:
                                    val = eval(sp_fun_item[1])(val)
                        if isinstance(val,types.ListType):
                            array_set={}
                            for array_set_item in fields_hbase[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'value' in array_set and 'orderedby' in array_set:
                                val = [it[array_set['value']] for it in sorted(val, key= lambda x:x[array_set['orderedby']])]
                            elif 'value' in array_set:
                                val = [it[array_set['value']] for it in val]
                        
                        value_hbase[name_column_hbase] = str(val[position_item]).encode('utf-8')
                    
            item["hbase"] = (key_hbase,value_hbase)
            
            
            # MongoDB document preparation
            ##################################
            
            # Generate the key fields of the row (This fields will be used to find the documents to update in the MongoDB contracts plus collection)) 
            key_mongo = {}
            
            for key_var in self.output_mongo_key:
                
                if key_var[1] in self.input_fields:
                    if key_var[2] in ['string','str']:
                        key_mongo[key_var[0]] = line[self.input_fields.index(key_var[1])]
                    elif key_var[2] == 'int':
                        try:
                            key_mongo[key_var[0]] = int(line[self.input_fields.index(key_var[1])])
                        except:
                            key_mongo[key_var[0]] = None
                else:
                    if key_var[2] == 'string':
                        key_mongo[key_var[0]] = eval('self.%s' % key_var[1])
                    elif key_var[2] == 'int':
                        key_mongo[key_var[0]] = int(eval('self.%s' % key_var[1]))

            
            # Generate the fields of the row
            value_mongo = key_mongo.copy()
            
            for fields_mongo in self.output_mongo_fields:
                
                #Name of the field
                # If some input variable has to be specified inside the name of the column, introduce a substring specifying the input field between <>. For example, "profile<season>".
                # This will search the input variable 'season' of the row in treatment, and will generate an output name of the HBase column like 'profileSummer' if the 'season' variable of that row is 'summer'.
                if ">" in fields_mongo[0] and "<" in fields_mongo[0]:
                    input_col_in_name = fields_mongo[0][fields_mongo[0].find("<")+1:fields_mongo[0].find(">")]
                    var_in_name = line[self.input_fields.index(input_col_in_name)]
                    name_field_mongo = fields_mongo[0].replace("<%s>" % input_col_in_name, var_in_name.lower())
                else:
                    name_field_mongo = fields_mongo[0]
                
                #Value of the field in the row
                if line[self.input_fields.index(fields_mongo[1])] not in ['NA','na','null','None','\\N','\N','NULL','Inf','-Inf','inf','-inf','NaN','nan','Nan','NAN']:
                    
                    if fields_mongo[2] == 'string':
                        value_mongo[name_field_mongo] = line[self.input_fields.index(fields_mongo[1])]
                    
                    elif fields_mongo[2] in ['json','dict']:
                        val = loads(line[self.input_fields.index(fields_mongo[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_mongo[1]:
                                    val = eval(sp_fun_item[1])(val)
                        elif len(fields_mongo)>3:
                            for array_set_item in fields_mongo[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'orderedby' in array_set:
                                val = sorted(val,key= lambda x:x[array_set['orderedby']])
                        value_mongo[name_field_mongo] = val
                    
                    elif fields_mongo[2] == 'boolean':
                        try:
                            value_mongo[name_field_mongo] = toBoolean(line[self.input_fields.index(fields_mongo[1])])
                        except:
                            value_mongo[name_field_mongo] = None
                        
                    elif fields_mongo[2] == 'int':
                        try:
                            value_mongo[name_field_mongo] = int(line[self.input_fields.index(fields_mongo[1])])
                        except:
                            value_mongo[name_field_mongo] = None
                            
                    elif fields_mongo[2] == 'float':
                        try:
                            value_mongo[name_field_mongo] = float(line[self.input_fields.index(fields_mongo[1])])
                        except:
                            value_mongo[name_field_mongo] = None
                            
                    elif fields_mongo[2] == 'array':
                        val = loads(line[self.input_fields.index(fields_mongo[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_mongo[1]:
                                    val = eval(sp_fun_item[1])(val)
                        if isinstance(val,types.ListType):
                            array_set={}
                            for array_set_item in fields_mongo[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'value' in array_set and 'orderedby' in array_set: 
                                val = [it[array_set['value']] for it in sorted(val, key= lambda x:x[array_set['orderedby']])]
                            elif 'value' in array_set:
                                val = [it[array_set['value']] for it in val]
                        value_mongo[name_field_mongo] = val
                    
                    elif fields_mongo[2] in ['get_smaller_item_in_dict','get_bigger_item_in_dict']:
                        position_item = 0 if 'smaller' in fields_mongo[2] else -1
                        val = loads(line[self.input_fields.index(fields_mongo[1])])
                        if self.special_functions_for_input_fields:
                            for sp_fun_item in self.special_functions_for_input_fields:
                                if sp_fun_item[0]==fields_mongo[1]:
                                    val = eval(sp_fun_item[1])(val)
                        if isinstance(val,types.ListType):
                            array_set={}
                            for array_set_item in fields_mongo[3].split(" "):
                                array_set_item = array_set_item.split(":")
                                array_set[array_set_item[0]] = array_set_item[1]
                            if 'value' in array_set and 'orderedby' in array_set:
                                val = [it[array_set['value']] for it in sorted(val, key= lambda x:x[array_set['orderedby']])]
                            elif 'value' in array_set:
                                val = [it[array_set['value']] for it in val]
                        value_mongo[name_field_mongo] = val[position_item]
                    
                    elif fields_mongo[2]== 'datetime':
                        try:
                            value_mongo[name_field_mongo] = datetime.datetime.strptime(line[self.input_fields.index(fields_mongo[1])],fields_mongo[3])
                        except:
                            value_mongo[name_field_mongo] = None
                
                else:
                    value_mongo[name_field_mongo] = None
            
            value_mongo['_updated'] = datetime.datetime.now()
            
            item["mongodb"] = (str(key_mongo),str(value_mongo))
            
        except Exception,e:
            print "Error with result line: %s" %e
        
        yield int(random.random()*self.n_reducers), item
        #yield str(line),item
    
    
    
    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        config = load(open(fn[0]))
        
        # Hbase connection
        self.hbase = happybase.Connection(config['hbase']['host'], config['hbase']['port'])
        self.hbase.open()
        
        # Hbase table destination
        self.tables_list = self.hbase.tables()
        try:
            if not config['output_hbase_table'] in self.tables_list:
                if config['create_hbase_column_family_if_not_exists']:
                    cfs=config['create_hbase_column_family_if_not_exists']
                else:
                    cfs={'p':dict(), 'i':dict(), 'l':dict(),'w':dict(),'c':dict()}
                self.hbase.create_table(config['output_hbase_table'], cfs)
                self.tables_list.append(config['output_hbase_table'])
        except:
            pass
        self.hbase_table = self.hbase.table(config['output_hbase_table'])
        
        # Mongo connection
        conn = MongoClient(config['mongodb']['host'], config['mongodb']['port'])
        conn[config['mongodb']['db']].authenticate(config['mongodb']['username'],config['mongodb']['password'])
        self.conn = conn[config['mongodb']['db']][config['output_mongo_collection']]
        self.mongo_operation = config['mongo_operation']
    
    
    def reducer(self, key, values):
        
        kv_mongo = []
        kv_hbase = []
        for value in values:
            kv_hbase.append(value['hbase'])
            kv_mongo.append(value['mongodb'])
        
        # MongoDB load
        bulk = self.conn.initialize_unordered_bulk_op()
        if self.mongo_operation == "update":
            for value_mongo in kv_mongo:
                bulk.find(eval(value_mongo[0])).upsert().update({"$set": eval(value_mongo[1])})
        elif self.mongo_operation == "insert":
            for value_mongo in kv_mongo:
                old = eval(value_mongo[1])
                new = {}
                for i in old.items():
                    set_dict_from_list(new,i[0].split("."),i[1])
                bulk.insert(new)
        result = bulk.execute()
        
        
        # HBase load
        with self.hbase_table.batch(batch_size=50000) as b:
            for value_hbase in kv_hbase:
                b.put(value_hbase[0],value_hbase[1])
    
       
if __name__ == '__main__':
    LoadResultsToRESTandHBase.run()    