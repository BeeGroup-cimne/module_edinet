import sys
from datetime import datetime, timedelta

from mrjob.job import MRJob

# hbase and mongo clients libs
from mrjob.protocol import PickleProtocol
# Generic imports
import glob
from json import load
import pandas as pd
import numpy as np
from pymongo import MongoClient, InsertOne, DeleteMany
import bee_data_cleaning as dc


class TSVProtocol(object):

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return k_str, v_str

    def write(self, key, value):
        return "{}".format(value)


def calculate_frequency(dataset):
    if len(dataset.index) > 1:
        return (pd.Series(dataset.index[1:]) - pd.Series(dataset.index[:-1])).value_counts().index[0]
    else:
        return None

class MRJob_clean_metering_data(MRJob):


    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = TSVProtocol

    def mapper_init(self):
        
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))



    # def reducer_init(self):
    #     # recover json configuration uploaded with script
    #     fn = glob.glob('*.json')
    #     self.config = load(open(fn[0]))
    #     self.companyId = self.config['companyId']
    #     self.mongo_client = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
    #     self.mongo_client[self.config['mongodb']['db']].authenticate(
    #             self.config['mongodb']['username'],
    #             self.config['mongodb']['password']
    #             )
    #     self.mongo = self.mongo_client[self.config['mongodb']['db']]

    def mapper(self, _, doc):
        """
        emits deviceId as keys
        emits ts, and consumption as values
        """
        ret = doc.split('\t')
        key = ret[1]
        value = {}
        try:
            value['ts'] = datetime.utcfromtimestamp(float(ret[0]))
            value['energytype'] = ret[4]
            value["source"] = ret[5]
        except:
            return
        try:
            value['value'] = float(ret[2])
        except:
            value['value'] = np.NaN
        try:
            value['accumulated'] = float(ret[3])
        except:
            value['accumulated'] = np.NaN
        if key[0] == "E":
            yield key, value

    # def reducer(self, key, values):
    #     """
    #     Cleans the metering data:
    #         - gets "acumulated or instant" values
    #         - removes negative and outliers
    #         - detects gaps
    #         - generates daily dataframe
    #     :param key: the device
    #     :param values: the information
    #     :return:
    #     """
        #create dataframe with the values:
        #df = pd.DataFrame.from_records(values, columns=["ts", "value", "accumulated", "energytype", "source"])
        #yield "A", 10
        # key, str(df.value.sum())
        # # group it by source and energyType
        # source_group = df.groupby('source')
        #
        # for source, df_source_group in source_group:
        #     etype_group = df_source_group.groupby('energytype')
        #     for etype, df_etype_group in etype_group:
        #         df_etype_group = df_etype_group.set_index('ts')
        #         df_etype_group = df_etype_group.sort_index()
        #         df_etype_group['ts'] = df_etype_group.index
        #         # save billing information in raw_data
        #         # save billing information in raw_data
        #         raw_data = df_etype_group[["ts","value","accumulated"]].to_dict('records')
        #         for r in raw_data:
        #             r.update(
        #                 {"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"})
        #
        #         ops = [InsertOne(x) for x in raw_data]
        #         result = self.mongo['raw_data'].bulk_write(
        #             [
        #                 DeleteMany({"device": key, "source": source, "energy_type": etype, "data_type": "metering",
        #                             "freq": "H"}),
        #             ] + ops
        #         )
        #
        #
        #         # self.mongo['raw_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "metering"}, { "$set" : {
        #         #         "device": key, "source": source, "energy_type": etype, "companyId": self.companyId,
        #         #         "raw_data":df_etype_group[["ts","value","accumulated"]].to_dict('records')
        #         #     }
        #         # }, upsert=True)
        #         #
        #         # self.mongo['raw_data'].update(
        #         #     {"device": key, "source": source, "energy_type": etype, "data_type": "metering"},
        #         #     {"$unset": {"errors": 1}},
        #         #     upsert=True)
        #         # # check if metering is acumulated or instant:
        #         self.increment_counter("Job", "Raw_Data", amount=1)
        #         duplicated_index = df_etype_group.index.duplicated(keep='last')
        #         duplicated_values = df_etype_group[duplicated_index].index.values.tolist()
        #         df_etype_group = df_etype_group[~duplicated_index]
        #
        #         freq = calculate_frequency(df_etype_group)
        #         if not freq:
        #             self.mongo['clean_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"}, {"$set": {
        #                 "errors": "can't infere frequency"
        #             }
        #             }, upsert=True)
        #             continue
        #
        #         hour_delta = timedelta(hours=1)
        #
        #         if df_etype_group.value.isnull().all():  # accumulated
        #             df_etype_group = df_etype_group[['accumulated']]
        #             if freq <= hour_delta: # sub-hourly frequency
        #                 if freq <= timedelta(minutes=30):
        #                     df_etype_group = df_etype_group.resample("30T").max().interpolate().diff(1, 0)
        #                     df_etype_group = df_etype_group.rename(columns={"accumulated": "value"})
        #                 else:
        #                     df_etype_group = df_etype_group.resample("H").max().interpolate().diff(1, 0)
        #                     df_etype_group = df_etype_group.rename(columns={"accumulated": "value"})
        #             else:
        #                 continue
        #         elif df_etype_group.accumulated.isnull().all(): #instant
        #             df_etype_group = df_etype_group[['value']]
        #             if freq <= hour_delta:  # sub-hourly frequency
        #                 if freq <= timedelta(minutes=30):
        #                     df_etype_group = df_etype_group.resample("30T").sum()
        #                 else:
        #                     df_etype_group = df_etype_group.resample("H").sum()
        #             else:
        #                 continue
        #         else:
        #             self.mongo['clean_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"}, {"$set": {
        #                 "errors" : "device with accumulated and instant values at the same metering"
        #                 }
        #             }, upsert=True)
        #             continue
        #         df_etype_group['ts'] = df_etype_group.index
        #         #max_threshold = self.config['max_threshold'][etype] if etype in self.config['max_threshold'] else self.config['max_threshold']['default']
        #         #max_outlier_bool = dc.detect_max_threshold_outliers(df_etype_group['value'], max_threshold)
        #         #df_etype_group['value'] = dc.clean_series(df_etype_group['value'], max_outlier_bool)
        #         negative_values_bool = dc.detect_min_threshold_outliers(df_etype_group['value'], 0)
        #         df_etype_group['value'] = dc.clean_series(df_etype_group['value'], negative_values_bool)
        #         znorm_bool = dc.detect_znorm_outliers(df_etype_group['value'], 30, mode="global")
        #         df_etype_group['value'] = dc.clean_series(df_etype_group['value'], znorm_bool)
        #
        #         #max_outliers = list(df_etype_group[max_outlier_bool].index)
        #         negative_outliers = list(df_etype_group[negative_values_bool].index)
        #         znorm_outliers = list(df_etype_group[znorm_bool].index)
        #         missing_values = list(df_etype_group[df_etype_group.value.isnull()].index)
        #         self.increment_counter("Job", "CleanData", amount=1)
        #         clean_data = df_etype_group[['ts','value']].to_dict('records')
        #         for r in clean_data:
        #             r.update(
        #                 {"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"})
        #
        #         ops = [InsertOne(x) for x in clean_data]
        #         result = self.mongo['clean_data'].bulk_write(
        #             [
        #                 DeleteMany({"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"}),
        #             ] + ops
        #         )
        #
        #         self.mongo['data_quality'].update(
        #             {"device": key, "source": source, "energy_type": etype, "data_type": "metering", "freq": "H"},
        #             {"$set":
        #                 {
        #                     "duplicated_values": duplicated_values,
        #                     "frequency": freq.resolution,
        #                     "gaps": missing_values,
        #                     "negative_values": negative_outliers,
        #                     "znorm_outliers": znorm_outliers
        #                 }
        #             }, upsert=True)
        #
        #         # self.mongo['raw_data'].update({"device": key, "source": source, "energy_type": etype, "data_type": "metering"},
        #         #                                 {"$set":
        #         #                                    {
        #         #                                     "clean_data_hourly": df_etype_group[['ts','value']].to_dict('records'),
        #         #                                     "negative_values": negative_outliers,
        #         #                                     "znorm_outliers_hourly": znorm_outliers,
        #         #                                     "max_outliers_hourly": max_outliers,
        #         #                                     "gaps": missing_values,
        #         #                                     "frequency": freq.isoformat(),
        #         #                                     "duplicated_values": duplicated_values
        #         #                                    }
        #         #                                 }, upsert=True)
        #
        #         for row in df_etype_group.iterrows():
        #             yield None, "\t".join([str(row[1]['ts'].timestamp()), key, str(row[1]['value']), etype, source])


if __name__ == '__main__':
    MRJob_clean_metering_data.run()