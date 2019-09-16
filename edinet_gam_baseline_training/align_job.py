import os
from tempfile import NamedTemporaryFile

import happybase
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# # mongo clients libs
from pymongo import MongoClient
# # Generic imports
import glob
import pandas as pd
from json import load
from datetime import datetime
import ast
import pickle
from _model_functions.gam_functions import set_r_environment, train_gaussian_mixture_model, prepare_dataframe, train_linear, clean_linear
import numpy as np
import zlib


def calculate_frequency(dataset):
    if len(dataset.index) > 1:
        return (pd.Series(dataset.index[1:]) - pd.Series(dataset.index[:-1])).value_counts().index[0]
    else:
        return None


class MRJob_align(MRJob):
     
    INTERNAL_PROTOCOL = PickleProtocol
    
    def mapper_init(self):
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        self.mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
        self.mongo[self.config['mongodb']['db']].authenticate(
            self.config['mongodb']['username'],
            self.config['mongodb']['password']
        )
        self.devices = self.config['devices']

    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        r_file = NamedTemporaryFile(delete=False, suffix='.R')
        with open('_model_functions/gam_functions.R', 'r') as rcode:
            r_file.write(bytes(rcode.read(), encoding="utf8"))
        set_r_environment(r_file.name)
        os.unlink(r_file.name)
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
        modelling_unit, multipliers, lat, lon, timezone = key.split('~')
        lat = float(lat)
        lon = float(lon)
        multipliers = ast.literal_eval(multipliers)  # string to dict
        multiplier = {}
        for i in multipliers:
            multiplier[i['deviceId']] = i['multiplier']
        columns = [x[0] for x in self.config['hive']['final_table_fields']]
        df = pd.DataFrame.from_records(values, index='ts', columns=columns)
        energy_type = df.energyType.unique()[0]
        grouped = df.groupby('deviceId')
        df_new_hourly = None
        for device, data in grouped:
            if device not in multiplier.keys():
                continue
            data = data[~data.index.duplicated(keep='last')]
            data = data.sort_index()
            if df_new_hourly is None:
                df_new_hourly = data[['value']] * multiplier[device]
            else:
                df_new_hourly += data[['value']] * multiplier[device]

        weather = df.drop(['value', 'energyType', 'deviceId'], axis=1)
        weather = weather[~weather.index.duplicated(keep='last')]
        df_new_hourly = df_new_hourly.join(weather)
        df_new_hourly = df_new_hourly[self.config['module_config']['model_features']]
        df_new_hourly = df_new_hourly.sort_index()
        df_value = df_new_hourly[['value']].resample('H').sum()
        df_weather = df_new_hourly[["temperature", "windSpeed", "GHI", "windBearing"]].resample('H').max()
        df_new_hourly = df_value.join(df_weather)
        if self.config['save_data_debug']:
            mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
            mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
            )

            mongo[self.config['mongodb']['db']][self.config['module_config']['mongo_debug']].replace_one({"modelling_unit": modelling_unit},{
                "modelling_unit": modelling_unit,
                "multipliers": multipliers,
                "df": df_new_hourly.reset_index().to_dict('records'),
                "lat": lat,
                "lon": lon,
                "timezone": timezone
            }, upsert=True)

            mongo.close()

        freq = calculate_frequency(df_new_hourly)
        whole_day_index = len(np.arange(pd.Timedelta('1 days'), pd.Timedelta('2 days'), freq))
        df_new_hourly = df_new_hourly.resample(freq).asfreq()
        df_new_hourly.index = df_new_hourly.index.tz_localize("UTC")
        df_new_hourly.index = df_new_hourly.index.tz_convert(timezone)
        count = df_new_hourly.groupby([df_new_hourly.index.year, df_new_hourly.index.month, df_new_hourly.index.day])
        complete_days = [datetime(*day).date() for day, x in count if x.count()['value'] >= whole_day_index]
        df_new_hourly = df_new_hourly[df_new_hourly.index.tz_localize(None).floor('D').isin(complete_days)]
        # All data for clustering.
        df_new_hourly = df_new_hourly.assign(
            clustering_values=df_new_hourly.value.rolling(5, center=True, min_periods=1).mean())
        self.increment_counter("M", "O", amount=1)
        model = train_gaussian_mixture_model(df_new_hourly[['value', 'clustering_values']], "clustering_values", [2, 3, 4], timezone)
        try:
            structural = model[0][['time', 's', 'dayhour']]
            structural = structural.set_index('time')
            structural['s'] = pd.to_numeric(structural.s, errors='coerce')
        except Exception as e:
            if "time" in df_new_hourly:
                df_new_hourly.drop("time", axis=1)
            mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
            mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
            )

            mongo[self.config['mongodb']['db']][self.config['module_config']['mongo_error']].replace_one(
                {"modellingUnitId": modelling_unit},
                {
                    "modellingUnitId": modelling_unit,
                    "model": "The clustering is empty",
                    "df": df_new_hourly.reset_index().to_dict('records'),
                    "multipliers": multipliers,
                    "lat": lat,
                    "lon": lon,
                    "timezone": timezone,
                    "exception": str(e),
                    "error": 1
                },
                upsert=True
            )
            mongo.close()
            return
        try:
            df_new_hourly = df_new_hourly.merge(structural, how='right', right_index=True, left_index=True)
            df_new_hourly = df_new_hourly[df_new_hourly.dayhour.isna() == False]
            df_new_hourly = df_new_hourly[df_new_hourly.s.isna() == False]
            count = df_new_hourly.groupby(
                [df_new_hourly.index.year, df_new_hourly.index.month, df_new_hourly.index.day])
            complete_days = [datetime(*day).date() for day, x in count if x.count()['value'] >= whole_day_index]
            df_new_hourly = df_new_hourly[df_new_hourly.index.tz_localize(None).floor('D').isin(complete_days)]
            self.increment_counter("M", "O", amount=1)

            df = prepare_dataframe(model, df_new_hourly, "value", 6, lat, lon, timezone=timezone)
        except Exception as e:
            if "time" in df_new_hourly:
                df_new_hourly.drop("time", axis=1)
            mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
            mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
            )

            mongo[self.config['mongodb']['db']][self.config['module_config']['mongo_error']].replace_one({
                "modellingUnitId": modelling_unit}, {
                "modellingUnitId": modelling_unit,
                "model": "Error preparing dataframe",
                "df": df_new_hourly.reset_index().to_dict('records'),
                "multipliers": multipliers,
                "lat": lat,
                "lon": lon,
                "timezone": timezone,
                "exception": str(e),
                "error": 2
                }, upsert=True)
            mongo.close()
            return

        try:
            e_type = "electricity" if energy_type == "electricityConsumption" else "gas"
            df = df.set_index('time')
            count = df.groupby([df.index.year, df.index.month, df.index.day])
            complete_days = [datetime(*day).date() for day, x in count if x.count()['value'] >= whole_day_index]
            df = df[df.index.tz_localize(None).floor('D').isin(complete_days)]
            self.increment_counter("M", "O", amount=1)
            model_linear = train_linear(model=model, type=e_type, dataframe=df, value_column="value", n_max=6, m_max=0, by_s=False)
            #save model to hbase
            self.increment_counter("M", "O", amount=1)
            model_linear = clean_linear(model_linear)
            pickle_model = pickle.dumps(model_linear)
            pickle_model = zlib.compress(pickle_model, 9)
            table_name = self.config['module_config']['model_table']
            hbase = happybase.Connection(self.config['hbase']['host'], self.config['hbase']['port'])
            hbase.open()
            try:
                hbase.create_table(table_name, {"model": dict()})
            except:
                pass
            hbase_table = hbase.table(table_name)
            max_cell = self.config['module_config']['max_cell']
            row={}
            num_parts=1
            for i, j in enumerate(range(max_cell, len(pickle_model) + max_cell, max_cell)):
                ini = i * max_cell
                end = min(j, len(pickle_model))
                row['model:part{}'.format(i+1)] = pickle_model[ini:end]
                num_parts +=1
            row['model:total'] = str(num_parts)

            hbase_table.put(modelling_unit, row)
            hbase.close()
            self.increment_counter("M", "O", amount=1)
        except Exception as e:
            if "time" in df:
                df.drop("time", axis=1)

            mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
            mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
            )

            mongo[self.config['mongodb']['db']][self.config['module_config']['mongo_error']].replace_one({
                "modellingUnitId": modelling_unit}, {
                "modellingUnitId": modelling_unit,
                "model": "Error training model",
                #"df": df_new_hourly.reset_index().to_dict('records'),
                "multipliers": multipliers,
                "lat": lat,
                "lon": lon,
                "timezone": timezone,
                "exception": str(e),
                "error": 3
            }, upsert=True)
            mongo.close()
            return



if __name__ == '__main__':
    MRJob_align.run()
