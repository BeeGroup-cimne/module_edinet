from datetime import datetime, timedelta

from mrjob.job import MRJob

# hbase and mongo clients libs
from mrjob.protocol import PickleProtocol
# Generic imports
import glob
from json import load
import pandas as pd
import numpy as np
from pymongo import MongoClient
import bee_data_cleaning as dc


class TSVProtocol(object):

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return k_str, v_str

    def write(self, key, value):
        return '%s' % value




class MRJob_clean_meteo_data(MRJob):


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
        emits ts, and consumption as values
        """
        ret = doc.split('\t')
        key = ret[1]
        value = {}
        try:
            value['ts'] = datetime.utcfromtimestamp(float(ret[0]))
        except:
            return
        try:
            value['temperature'] = float(ret[2])
        except:
            value['temperature'] = np.NaN

        yield key, value

    def reducer(self, key, values):
        """
        Cleans the metering data:
            - gets "acumulated or instant" values
            - removes negative and outliers
            - detects gaps
            - generates daily dataframe
        :param key: the device
        :param values: the information
        :return:
        """
        #create dataframe with the values:
        df = pd.DataFrame.from_records(values, columns=["ts", "temperature"])
        # group it by source and energyType

        df = df.set_index('ts')
        df = df.sort_index()
        df['ts'] = df.index
        # save meteo information in raw_data
        self.mongo['meteo_raw_data'].update({"stationId": key}, { "$set" : {
            "stationId": key, "companyId": self.companyId,
            "raw_data":df[["ts","temperature"]].to_dict('records')
            }
        }, upsert=True)

        self.mongo['meteo_raw_data'].update(
            {"stationId": key},
            {"$unset": {"errors": 1}},
            upsert=True)
        # check if duplicated meteo data
        duplicated_index = df.index.duplicated(keep='last')
        duplicated_values = df[duplicated_index].index.values.tolist()
        df = df[~duplicated_index]

        max_threshold = self.config['threshold']['max']
        max_outlier_bool = dc.detect_max_threshold_outliers(df['temperature'], max_threshold)
        df['temperature'] = dc.clean_series(df['temperature'], max_outlier_bool)

        min_threshold = self.config['threshold']['min']
        min_threshold_bool = dc.detect_min_threshold_outliers(df['temperature'], min_threshold)
        df['temperature'] = dc.clean_series(df['temperature'], min_threshold_bool)

        znorm_bool = dc.detect_znorm_outliers(df['temperature'], 30, mode="rolling", window=168)
        df['temperature'] = dc.clean_series(df['temperature'], znorm_bool)

        max_outliers = list(df[max_outlier_bool].index)
        negative_outliers = list(df[min_threshold_bool].index)
        znorm_outliers = list(df[znorm_bool].index)
        missing_values = list(df[df.temperature.isnull()].index)

        self.mongo['meteo_raw_data'].update({"stationId": key},
                                        {"$set":
                                           {
                                            "clean_data_meteo": df[['ts','temperature']].to_dict('records'),
                                            "negative_values": negative_outliers,
                                            "znorm_outliers_hourly": znorm_outliers,
                                            "max_outliers_hourly": max_outliers,
                                            "gaps": missing_values,
                                            "duplicated_values": duplicated_values
                                           }
                                        }, upsert=True)

        for row in df.iterrows():
            yield None, "\t".join([str(row[1]['ts'].timestamp()), key, str(row[1]['temperature'])])


if __name__ == '__main__':
    MRJob_clean_meteo_data.run()