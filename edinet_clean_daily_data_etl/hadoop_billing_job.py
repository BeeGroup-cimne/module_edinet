from datetime import datetime

from mrjob.job import MRJob

# hbase and mongo clients libs
from mrjob.protocol import PickleProtocol
# Generic imports
import glob
from json import load
import pandas as pd
from pymongo import MongoClient
import bee_data_cleaning as dc


class TSVProtocol(object):

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return k_str, v_str

    def write(self, key, value):
        return '%s' % value


class MRJob_clean_billing_data(MRJob):


    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = TSVProtocol

    def mapper_init(self):
        
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))



    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        self.companyId = self.config['companyId']
        self.mongo_client = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo_client[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
                )
        self.mongo = self.mongo_client[self.config['mongodb']['db']]

    def mapper(self, _, doc):
        """
        emits deviceId as keys
        emits ts_ini, ts_end and consumption as values
        """
        ret = doc.split('\t')
        key = ret[2]
        values = {}
        try:
            values["ts_ini"] = datetime.utcfromtimestamp(float(ret[0]))
        except:
            values["ts_ini"] = None
        try:
            values["ts_end"] = datetime.utcfromtimestamp(float(ret[1]))
        except:
            values["ts_end"] = None
        try:
            values["value"] = ret[3]
        except:
            values["value"] = None
        try:
            values["energytype"] = ret[4]
        except:
            values["energytype"] = None
        try:
            values["source"] = ret[5]
        except:
            values["source"] = None

        yield key, values

    def reducer(self, key, values):
        """
        Cleans the billing data:
            -checks gaps
            -checks overlappings
            -generates daily dataframe
            -checks outliers
            -saves results to RAW DATA and HBASE
        :param key: the device
        :param values: the information
        :return:
        """
        #create dataframe with the values:
        df = pd.DataFrame.from_records(values, columns=["ts_ini", "ts_end", "value", "energytype", "source"])
        # group it by source and energyType
        source_group = df.groupby('source')

        for source, df_source_group in source_group:
            etype_group = df_source_group.groupby('energytype')
            for etype, df_etype_group in etype_group:
                df_etype_group.dropna(subset=["ts_ini"])
                df_etype_group = df_etype_group.set_index('ts_ini')
                df_etype_group = df_etype_group.sort_index()
                df_etype_group['ts_ini'] = df_etype_group.index
                # save billing information in raw_data
                self.mongo['raw_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "billing"}, {'$set': {
                        "device": key, "source": source, "energy_type": etype, "companyId": self.companyId,
                        "raw_data":df_etype_group[["ts_ini","ts_end","value"]].to_dict('records')
                    }
                }, upsert=True)
                # generate daily dataframe dividing by days:
                dfs = []
                for row in df_etype_group.iterrows():
                    index = pd.date_range(row[1]['ts_ini'], row[1]['ts_end'])
                    df_temp = pd.DataFrame(
                        data={"index": index, "value": [(float(row[1]['value']) / float(len(index)))] * len(index)}, index=index)
                    dfs.append(df_temp)

                #join daily df and detect overlapings and gaps
                global_df = dfs[0]
                overlappings = []
                for df_temp in dfs[1:]:
                    overlappings.extend(global_df.index.intersection(df_temp.index).tolist())
                    global_df = global_df.append(df_temp)
                    global_df.drop_duplicates(keep='last', inplace=True)

                gaps = []
                gap_last_index = global_df[global_df.index.to_series().diff() > pd.Timedelta('1 days')].index.tolist()
                for gf in gap_last_index:
                    index = list(global_df.index).index(gf)
                    gi = list(global_df.index)[index-1]
                    gaps.append([gi,gf])

                max_threshold = self.config['max_threshold'][etype] * 24 if 'etype' in self.config['max_threshold'] else self.config['max_threshold']['default'] * 24
                max_outliers_bool = dc.detect_max_threshold_outliers(global_df['value'], max_threshold)
                global_df['value'] = dc.clean_series(global_df['value'], max_outliers_bool)
                negative_values_bool = dc.detect_min_threshold_outliers(global_df['value'], 0)
                global_df['value'] = dc.clean_series(global_df['value'], negative_values_bool)
                znorm_bool = dc.detect_znorm_outliers(global_df['value'], 30, mode="global")
                global_df['value'] = dc.clean_series(global_df['value'], znorm_bool)

                max_outliers = list(global_df[max_outliers_bool].index)
                negative_outliers = list(global_df[negative_values_bool].index)
                znorm_outliers = list(global_df[znorm_bool].index)

                self.mongo['raw_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "billing"},
                                                {"$set":
                                                   {
                                                    "overlapings_billing" : overlappings,
                                                    "clean_data": global_df.to_dict('records'),
                                                    "gaps": gaps,
                                                    "negative_values": negative_outliers,
                                                    "znorm_outliers": znorm_outliers,
                                                    "max_outliers": max_outliers}
                                                }, upsert=True)

                for row in global_df.iterrows():
                    yield None, "\t".join([str(row[1]['index'].timestamp()), key, str(row[1]['value']), etype, source])


if __name__ == '__main__':
    MRJob_clean_billing_data.run()