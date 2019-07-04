from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, RawValueProtocol
import glob
import json
from calendar import monthrange
import pandas as pd
import operator
import numpy as np
from list_functions import unique, getFromDict, pretty_numeric, ensure_list
import sys
class SimilarsBestCriteria(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        config = json.load(open(fn[0]))
        
        self.columns = config['columns']
        self.criteria = config['criteria']
        self.key_yearmonths = config['key_yearmonths']
        self.min_yearmonths = config['min_yearmonths']
        self.n_months = config['n_months']
        self.fixed_part = config['fixed_part']
        self.sep = config['sep']
        self.sep_similars = config['sep_similars']
        self.keys_for_filtering_and_best_criteria = config['keys_for_filtering_and_best_criteria']
        
        # recover similars uploaded with script
        fn = glob.glob('*.similars')
        self.df_similars = pd.read_csv(fn[0],header=None,names=config['columns_similars'],sep=self.sep_similars)
        self.df_similars = self.df_similars.set_index('month').sort_index()
        if '[' in ",".join([str(item) for item in list(self.df_similars.average)]):
            # average must be arrays
            self.df_similars.loc[self.df_similars.average=="\N",'average'] = np.nan
            self.df_similars.average = [json.loads(json.dumps(i)) for i in self.df_similars.average]
        else:
            # average must be floats
            self.df_similars.average = pd.to_numeric(self.df_similars.average,errors='coerce')
        #self.df_similars.loc[self.df_similars.coefficientVariation=="\N",'coefficientVariation'] = np.nan
        #self.df_similars.loc[self.df_similars.coefficientDispersion=="\N",'coefficientDispersion'] = np.nan

        
    def mapper(self, _, line):
        
        # Row definition
        line = line.decode('utf-8').split(self.sep)
        
        dl = {}
        for i in range(len(line)):
            dl.update({self.columns[i]: line[i]})
        
        value_months = []
        item_v = self.keys_for_filtering_and_best_criteria[0]
        if dl[item_v] != "\N" or dl[item_v] != "\\N":
            for item in json.loads(dl[item_v]):
                if item_v=='rawMonths':              ## Days normalization in rawMonths case
                    item['v']=item['v']*item.get('td')/item.get('rd') if 'rd' in item and item['rd'] > 0 else None ##
                    item['v_surf']=item['v_surf']*item.get('td')/item.get('rd') if item['v_surf'] != 'null' and 'rd' in item and item['rd'] > 0 else None
                ym = int(item.pop(self.key_yearmonths))
                value = getFromDict(item,self.keys_for_filtering_and_best_criteria[1:]) if len(self.keys_for_filtering_and_best_criteria)>1 else item
                if not ym in value_months:
                    value_months.append({
                        self.key_yearmonths: ym,
                        item_v: json.loads(json.dumps(value))
                    })
            
            criteria_groups = [(" + ".join(item_k), " + ".join([dl[item] for item in item_k])) for item_k in self.criteria]
            
            df = pd.DataFrame(value_months)
            
            if not df.empty:
                df = df.set_index(self.key_yearmonths).sort_index()
    
                # COST CALCULATION FOR EACH CUSTOMER, n_months and fixed_part combination 
                cost = {}
                
                # N_months iterator
                for i in range(len(self.n_months)):
                    df_i = df[df.index >= self.min_yearmonths[i]]
                    unique_ym = unique(list(df_i.index))
                    val = list(df_i[item_v])
                    df_s_i = self.df_similars[[item in unique_ym for item in list(self.df_similars.index)]]               
                    if df_s_i.empty:
                        continue
                    for cg in criteria_groups:
                        # Get the similar neighbors within this criteria group
                        similar = df_s_i[list(df_s_i.criteria==cg[0])]
                        
                        if len(similar)>0:
                            similar = similar[list(similar.groupCriteria==cg[1])][['average','coefficientVariation','coefficientDispersion']]
                            similar = similar.reindex(unique_ym)
                            
                            if len(similar)>0:
                                #try:
                                avg = list(similar.average)
                                #except:
                                #    yield _, str(list(similar.average))
                                
                                diff=[]
                                # Calculate the difference between customer month value and the related average within each group criteria
                                #### Euclidean distance is computed because the monthly values could be either a float or a list.
                                #### The result for each month is always a float or np.nan.
                                for j in range(len(val)):
                                    if (isinstance(val[j],list) or isinstance(val[j],float)) and\
                                       (isinstance(avg[j],list) or isinstance(avg[j],float)) and\
                                       len(ensure_list(val[j]))==len(ensure_list(avg[j])):
                                        diff.append( np.linalg.norm( ((np.array(val[j])-np.array(avg[j]))/np.array(avg[j]))*100 ) )
                                    else:
                                        diff.append(np.nan)
                                
                                # Fixed_part iterator
                                for f in self.fixed_part:
                                    
                                    # COST FUNCTION
                                    cst = np.array(similar.coefficientDispersion)*(f/100) + np.array(similar.coefficientDispersion)*(1-(f/100))*np.array(diff)
                                    
                                    if "%s~%s" % (str(self.n_months[i]),str(f)) not in cost.keys():
                                        cost["%s~%s" % (str(self.n_months[i]),str(f))] = []
                                    cst_v = pretty_numeric(np.nanmean(cst),3)
                                    if cst_v and cg[1] != 'null':
                                        cost["%s~%s" % (str(self.n_months[i]),str(f))].append({
                                            'criteria': cg[0],
                                            'groupCriteria': cg[1],
                                            'ldiff': pretty_numeric(diff,3),
                                            'lcost': pretty_numeric(cst,3),
                                            'diff': pretty_numeric(np.nanmean(diff),3),
                                            'cost': cst_v
                                        })
                    
                # Yield the results
                for n_month in self.n_months:
                    for f in self.fixed_part:
                        key="%s~%s" % (str(n_month),str(f))
                        if key in cost.keys():
                            l = sorted(cost[key], key= lambda x:x['cost'])
                            #replacing the 'null' by None
                            values = json.loads(dl['rawMonths'])
                            if isinstance(values, list):
                                for i in range(len(values)):
                                    if isinstance(values[i], dict):
                                        for key, value in values[i].items():
                                            if value == 'null':
                                                values[i][key] = None
                            if (isinstance(l, list) and l):
                                yield _,"\t".join([dl['modellingUnitId'],
                                                   str(n_month),
                                                   str(f),
                                                   l[0]['criteria'],
                                                   l[0]['groupCriteria'],
                                                   json.dumps(l),
                                                   str(l[0]['diff']),
                                                   str(l[0]['cost']),
                                                   json.dumps(values)
                                                ])
                            else:
                                yield _,"\t".join([dl['modellingUnitId'],
                                                   str(n_month),
                                                   str(f),
                                                   'error',
                                                   'error',
                                                   json.dumps(l),
                                                   'error',
                                                   'error',
                                                   json.dumps(values)
                                                ])
                                
                        else:
                            yield _,"\t".join([dl['modellingUnitId'],
                                               str(n_month),
                                               str(f),
                                               'null',
                                               'null',
                                               'null',
                                               'null',
                                               'null',
                                               'null'
                                            ])
            
                
        
if __name__ == '__main__':
    SimilarsBestCriteria.run()    