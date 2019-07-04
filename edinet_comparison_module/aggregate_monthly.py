import ast
from datetime import datetime
import glob
import numpy as np
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
import json
from pymongo import MongoClient
import pandas as pd

class TSVProtocol(object):

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return k_str, v_str

    def write(self, key, value):
        return '%s' % value


class MRJob_aggregate(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = TSVProtocol

    def mapper_init(self):
        fn = glob.glob('*.json')
        self.config = json.load(open(fn[0]))
        self.devices = self.config['devices']

    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = json.load(open(fn[0]))
        self.company = self.config['company']
        self.devices = self.config['devices']

    def mapper(self, _, doc):
        # emits modelling_units as key
        # emits deviceId, consumption, ts
        columns = [(x[0], x[1]) for x in self.config['hive']['final_table_fields']]
        ret = doc.split('\t')
        try:
            modelling_units = self.devices[ret[0]]
        except:
            return
        d = {}
        for i, c in enumerate(columns):
            if c[0] == "ts":
                d[c[0]] = datetime.fromtimestamp(float(ret[i]))
            elif c[1] == "float":
                try:
                    d[c[0]] = float(ret[i])
                except:
                    d[c[0]] = np.NaN
            else:
                d[c[0]] = ret[i]

        for modelling_unit in modelling_units:
            yield modelling_unit, d


    def reducer(self, key, values):
        # obtain the needed info from the key
        modelling_unit, multipliers = key.split('~')
        multipliers = ast.literal_eval(multipliers)  # string to dict
        multiplier = {}
        for i in multipliers:
            multiplier[i['deviceId']] = i['multiplier']
        columns = [x[0] for x in self.config['hive']['final_table_fields']]
        df = pd.DataFrame.from_records(values, index='ts', columns=columns)
        energy_type = df.energyType.unique()[0]
        grouped = df.groupby('deviceId')
        df_new_daily = None
        for device, data in grouped:
            if device not in multiplier.keys():
                continue
            data = data[~data.index.duplicated(keep='last')]
            data = data.sort_index()
            if df_new_daily is None:
                df_new_daily = data[['value']] * multiplier[device]
            else:
                df_new_daily += data[['value']] * multiplier[device]

        df_new_daily = df_new_daily.sort_index()
        df_value = df_new_daily[['value']].resample('M').sum()
        mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        mongo[self.config['mongodb']['db']].authenticate(
            self.config['mongodb']['username'],
            self.config['mongodb']['password']
        )

        mongo[self.config['mongodb']['db']][self.config['mongodb']['montly_data_collection']].replace_one({
            "modellingUnitId": modelling_unit}, {
            "modellingUnitId": modelling_unit,
            "df": df_value.reset_index().to_dict('records'),
        }, upsert=True)
        mongo.close()

        for ts, row in df_value.iterrows():
            yield None, "fafa".encode("utf-8")
            #yield None, "{}\t{}\t{}\t{}".format(modelling_unit, "111111111", "10", energy_type).encode('utf-8')

if __name__ == '__main__':
    MRJob_aggregate.run()