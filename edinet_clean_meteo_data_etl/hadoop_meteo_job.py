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
        columns = [x[0] for x in self.config['output']['fields']]
        value = {}
        for i, c in enumerate(self.config['output']['fields']):
            if i == 0:
                #timestamp is at position 0
                try:
                    value[c[0]] = datetime.utcfromtimestamp(float(ret[0]))
                except:
                    return
            else:
                if c[1] == 'float':
                    try:
                        value[c[0]] = float(ret[i])
                    except:
                        value[c[0]] = np.NaN
                else:
                    value[c[0]] = ret[i]


        key = value.pop('stationId')
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
        df = pd.DataFrame.from_records(values)
        # group it by source and energyType
        columns = [x[0] for x in self.config['output']['fields']]

        df = df.set_index('ts')
        df = df.sort_index()
        df['ts'] = df.index

        raw_data = df[columns].to_dict('records')
        for r in raw_data:
            r.update({"stationId": key})

        ops = [InsertOne(x) for x in raw_data]
        result = self.mongo['meteo_raw_data'].bulk_write(
            [
                DeleteMany(
                    {"stationId": key}),
            ] + ops
        )

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

        znorm_bool = dc.detect_znorm_outliers(df['temperature'], 30, mode="global")
        df['temperature'] = dc.clean_series(df['temperature'], znorm_bool)

        max_outliers = list(df[max_outlier_bool].index)
        negative_outliers = list(df[min_threshold_bool].index)
        znorm_outliers = list(df[znorm_bool].index)
        missing_values = list(df[df.temperature.isnull()].index)

        clean_data = df[columns]('records')
        for r in clean_data:
            r.update({"stationId": key})

        ops = [InsertOne(x) for x in clean_data]
        result = self.mongo['meteo_clean_data'].bulk_write(
            [
                DeleteMany(
                    {"stationId": key}),
            ] + ops
        )

        self.mongo['meteo_data_quality'].update(
            {"stationId": key},
            {"$set":
                {
                    "overlapings": duplicated_values,
                    "gaps": missing_values,
                    "negative_values": negative_outliers,
                    "znorm_outliers": znorm_outliers,
                    "max_outliers": max_outliers}
            }, upsert=True)

        all = [x[0] for x in self.config['output']['fields']]
        for row in df.iterrows():
            return_list = []
            for f in all:
                if f == "ts":
                    return_list.append(str(row[1]['ts'].timestamp()))
                elif f == "stationId":
                    return_list.append(key)
                else:
                    return_list.append(str(row[1][f]))

            yield None, "\t".join(return_list)


if __name__ == '__main__':
    MRJob_clean_meteo_data.run()