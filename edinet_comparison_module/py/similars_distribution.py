import numpy as np
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, RawValueProtocol
import glob
import json
import pandas as pd
from list_functions import find_keys, pretty_numeric, getFromDict, setInDict

class SimilarsDistribution(MRJob):
    
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    
    def mapper_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        config = json.load(open(fn[0]))
        
        self.columns = config['columns']
        self.variables= config['variables']
        self.criteria= config['criteria']
        self.sep= config['sep']
        self.key_yearmonths= config['key_yearmonths']
    
    
    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        config = json.load(open(fn[0]))
        
        self.keys_for_filtering_and_best_criteria = config['keys_for_filtering_and_best_criteria']
        self.min_consumption_percentile= config['min_consumption_percentile']
        self.max_consumption_percentile= config['max_consumption_percentile']
        self.group_volume_limits= config['group_volume_limits']
        self.group_volume_penalties= config['group_volume_penalties']
        
    
    def mapper(self, _, line):
        
        # Row definition
        line = line.decode('utf-8').split(self.sep)
        
        dl = {}
        for i in range(len(line)):
            dl.update({self.columns[i]: line[i]})
        
        value_months = {}
        for item_v in self.variables:
            for item in json.loads(dl[item_v]):
                if item_v=='rawMonths':              ## Days normalization in rawMonths case
                    item['v']=(item['v']*item.get('td')/item.get('rd')) if 'rd' in item and item['rd'] > 0 else 0
                    if item['v_surf'] != 'null':
                        item['v_surf']=(item['v_surf']*item.pop('td')/item.pop('rd')) if 'rd' in item and item['rd'] > 0 else 0
                    else:
                        item['v_surf']=None
                        item.pop('td')
                        item.pop('rd')
                ym = str(item.pop(self.key_yearmonths))
                if not ym in value_months:
                    value_months.update({ym: {item_v:item}})
                    value_months[ym].update({'modellingUnitId':dl['modellingUnitId']})
                else:
                    value_months[ym].update({item_v:item})
            
        criteria_groups = [(" + ".join(item_k), " + ".join([dl[item] for item in item_k])) for item_k in self.criteria]
        
        for ym in value_months.keys():
            for criteria_group in criteria_groups:
                yield [criteria_group,ym], value_months[ym]
        
            
    
    def reducer(self, key, values):
        
        criteria = key[0][0]
        group_criteria = key[0][1]
        month = key[1]
        
        values_red = []
        unique_columns = {}

        for item in values:
            unique_columns.update(find_keys(item))
            values_red.append(item)
        if 'modellingUnitId' in unique_columns.keys():
            del unique_columns['modellingUnitId']

        #filter of contracts if customers are bigger than 10.
        contracts_to_avoid = []
        if '~'.join(self.keys_for_filtering_and_best_criteria) in unique_columns.keys() and len(values_red)>10:
            filter = []
            for item in values_red:
                try:
                    filter.append({
                        'v': getFromDict(item, self.keys_for_filtering_and_best_criteria),
                        'c': item['modellingUnitId']
                        })
                except:
                    pass
            if len(filter)>0:
                filter_df = pd.DataFrame(filter).set_index('c')
                contracts_to_avoid.extend( list(filter_df[filter_df < filter_df.quantile(float(self.min_consumption_percentile)/100.0)].dropna(how='all').index) )
                contracts_to_avoid.extend( list(filter_df[filter_df > filter_df.quantile(float(self.max_consumption_percentile)/100.0)].dropna(how='all').index) )
        ncust = len(values_red)-len(contracts_to_avoid)
        
        if ncust>1: #at least two contracts to compare... if not, any result will be yield
            # Generate the list of lists to compute. This is needed in order to generate the proper pandas series or DataFrame in the following steps
            to_compute = {}
            for item in values_red:
                for namecol,cols in unique_columns.iteritems():
                    if item['modellingUnitId'] not in contracts_to_avoid:
                        if namecol in to_compute.keys():
                            try:
                                to_compute[namecol].append(getFromDict(item, cols))
                            except:
                                pass
                        else:
                            try:
                                to_compute.update({namecol: [getFromDict(item, cols)]})
                            except:
                                pass
            
            # Compute the summary results
            results={}
            for key_variable in to_compute.keys():
                to_compute.update({key_variable: pd.DataFrame(to_compute[key_variable])})
                
                keys = key_variable.split('~')
                itk = []
                for key in keys:
                    if key not in getFromDict(results, itk):
                        getFromDict(results, itk).update({key: {}})
                    itk.append(key)

                setInDict(results, keys,
                          {
                              'p5': pretty_numeric(to_compute[key_variable].dropna().quantile(0.05)) if not to_compute[key_variable].dropna().empty else None,
                              'p25': pretty_numeric(to_compute[key_variable].dropna().quantile(0.25)) if not to_compute[key_variable].dropna().empty else None,
                              'p50': pretty_numeric(to_compute[key_variable].dropna().quantile(0.50)) if not to_compute[key_variable].dropna().empty else None,
                              'p75': pretty_numeric(to_compute[key_variable].dropna().quantile(0.75)) if not to_compute[key_variable].dropna().empty else None,
                              'p95': pretty_numeric(to_compute[key_variable].dropna().quantile(0.95)) if not to_compute[key_variable].dropna().empty else None,
                              'mean': pretty_numeric(to_compute[key_variable].mean()),
                              'sd': pretty_numeric(to_compute[key_variable].std()),
                          })
            
            #Penalty (pcust) calculation depending the number of customers (ncust)
            if ncust==1:
                pcust = float(self.group_volume_penalties[0])
            elif ncust < self.group_volume_limits[0]:
                pcust = pretty_numeric(float(self.group_volume_penalties[0])+((1.0-float(self.group_volume_penalties[0]))/(float(self.group_volume_limits[0]-1.0)))*float(ncust))
            elif ncust > self.group_volume_limits[1]:
                pcust = pretty_numeric(1+((float(self.group_volume_penalties[1])-1.0)/(float(self.group_volume_limits[2])-float(self.group_volume_limits[1])))*(float(ncust)-float(self.group_volume_limits[1])))
            else:
                pcust = 1.0
                
            #Calculation of the variation (cvar) and dispersion (cdisp) coefficients
            cdisp = pcust
            cvar = pcust
            avg_value = None
            if self.keys_for_filtering_and_best_criteria[0] in results:
                    res = getFromDict(results,self.keys_for_filtering_and_best_criteria)
                    avg_value = res['mean']
                    try:
                        if isinstance(avg_value,float) or isinstance(avg_value,int) and ncust > 10:
                            cdisp = pretty_numeric((float(res['p75']-res['p25'])/float(res['p75']+res['p25']))*float(pcust))
                            cvar = pretty_numeric((float(res['sd'])/float(abs(avg_value)))*float(pcust))
                        elif isinstance(avg_value,list) and ncust > 10:
                            cdisp = [(float(res['p75'][i]-res['p25'][i])/float(res['p75'][i]+res['p25'][i]))*float(pcust) for i in range(len(avg_value))]
                            cdisp = pretty_numeric(np.nanmean(cdisp))
                            cvar = [(float(res['sd'][i])/float(abs(avg_value[i])))*float(pcust) for i in range(len(avg_value))]
                            cvar = pretty_numeric(np.nanmean(cvar))
                    except:
                        pass
                    
            yield None,"\t".join([criteria, group_criteria, month, str(pcust), str(ncust), str(cdisp),
                                  str(cvar), json.dumps(avg_value), json.dumps(results)])
    
if __name__ == '__main__':
    SimilarsDistribution.run()    