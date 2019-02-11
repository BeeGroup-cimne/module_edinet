from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

# # mongo clients libs
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId

# # Generic imports
import glob
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from json import load
from datetime import datetime, timedelta
from time import mktime
from dateutil.relativedelta import relativedelta
import ast
import re
import bee_data_cleaning as dc
from bee_dataframes import create_dataframes
from edinet_models.edinet_models import baseline_calc_pyemis_old, baseline_calc_pyemis_new, monthly_calc

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
        self.task_id = self.config['task_id']

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
        self.devices = self.config['devices']
        self.stations = self.config['stations']
        self.task_id = self.config['task_id']

        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read   

        # emits modelling_units as key
        # emits deviceId, consumption, ts
        try:
            ret = doc.split('\t')
            modelling_units = self.devices[str(ret[0])]
            d = {
                'deviceid': ret[0],
                'date': datetime.fromtimestamp(float(ret[1])),
                'energyType': ret[3]
                }
        except Exception as e:
            self.mongo[self.config['mongodb']['db']]['debug'].update(
                {'task_id': self.task_id},
                {'$push': {'errors': str(e)}},
                upsert=True)

        try:
            d['value'] = float(ret[2])
        except:
            d['value'] = None
        try:
            d['temperature'] = float(ret[4])
        except:
            d['temperature'] = None

        for modelling_unit in modelling_units:
            yield modelling_unit, d

    
    def reducer(self, key, values):
        # obtain the needed info from the key
        modelling_unit, multipliers = key.split('~')
        multipliers = ast.literal_eval(multipliers) #string to dict
        multiplier = {}
        for i in multipliers:
            multiplier[i['deviceId']] = i['multiplier']

        # create dataframe from values list
        v = []
        for i in values:
            v.append(i)
        df = pd.DataFrame.from_records(v, index='date', columns=['value','temperature','date','deviceid','energyType'])
        df = df[~df.index.duplicated(keep='last')]
        df = df.sort_index()

        grouped = df.groupby('deviceid')
        # has to multiply each modelling unit values by multiplier and add them all:
        df_new_hourly = None
        df_weather = None
        for device, data in grouped:
            if device not in multiplier.keys():
                continue
            if df_new_hourly is None:
                df_new_hourly = data[['value']] * multiplier[device]
            else:
                df_new_hourly += data[['value']] * multiplier[device]
            df_weather = data[['temperature']]



        energy_type = df['energyType'].unique()[0]
        #### NYAPA
        hourly_baseline = {}
        # if '80d32f1a-dee3-5e54-a64a-4460fdcfbaff' == modelling_unit:
        #     hourly_baseline = baseline_calc_pyemis_new(df_new_hourly, tdf, model, energy_type, iters=16)
        # else:
        hourly_baseline = baseline_calc_pyemis_old(df_new_hourly, df_weather, energy_type, iters=16)

        baseline = {
            'companyId': int(self.company),
            'devices': str(multipliers),
            'modellingUnitId': modelling_unit,
            '_created': datetime.now()
        }

        #baseline.update(monthly_baseline)
        baseline.update(hourly_baseline)
        mongo = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['collection']]

        mongo.update(
            {'modellingUnitId': modelling_unit, 'companyId': int(self.company)},
            {"$set": baseline},
            upsert=True
        )

        # # meter replacement
        # mongo_modellingUnits = self.mongo[self.config['mongodb']['db']][
        # self.config['mongodb']['modelling_units_collection']]
        # #df = create_dataframes.meter_replacement(modelling_unit, self.company, df, mongo_modellingUnits)
        # # regexp to identify hourly data
        # mongo_building = self.mongo[self.config['mongodb']['db']][
        #     self.config['mongodb']['building_collection']]
        # mongo_reporting = self.mongo[self.config['mongodb']['db']][
        #     self.config['mongodb']['reporting_collection']]

        # get station
        #station = self.stations[str(df.deviceid[0])]

        # apply the multiplier over each deviceId value and sum all the values






        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "get_data"}},
        #     upsert=True)
        #
        # # Detectem si l'edifici te dades horaries.
        # try:
        #     # nomes ho fem per gas i electricitat de moment.
        #     modelling_unit_item = mongo_modellingUnits.find_one(
        #         {"modellingUnitId": modelling_unit, "companyId": self.company})
        #     valid_type = False
        #     # edificis greame
        #     if "energyType" in modelling_unit_item:
        #         if modelling_unit_item["energyType"] in ["electricityConsumption", "gasConsumption", "heatConsumption"]:
        #             valid_type = True
        #     # edificis eloi
        #     if "edinetId" in modelling_unit_item['modellingUnitId']:
        #         energy_type_mm = modelling_unit_item['modellingUnitId'].split("-")[1]
        #         if energy_type_mm in ["electricityConsumption", "gasConsumption"]:
        #             valid_type = True
        # except Exception as e:
        #     print(e)
        #
        # # si el tipus es correcte, hem d aconseguir el buildingId.
        # buildingId = None
        # if valid_type:
        #     # mirem si el podem aconseguir directament (edificis eloi)
        #     building_item = mongo_building.find_one({"modellingUnits": modelling_unit, "companyId": self.company})
        #     if building_item and "buildingId" in building_item:
        #         buildingId = building_item['buildingId']
        #     else:
        #         # Sino, ho mirem al reporting unit (edificis greame)
        #         reporting_item = mongo_reporting.find_one(
        #             {"modelling_Units": modelling_unit, "companyId": self.company})
        #         if reporting_item and "buildingId" in reporting_item:
        #             buildingId = reporting_item['buildingId']
        #
        # # si el buildingId es None no cal fer res.
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "check_hourly_data"}},
        #     upsert=True)
        # if buildingId:
        #     # Comparem per cada device, energy type si hi ha dades horaries o no
        #     # pero nomes ho mirem per les ultimes 12 setmanes
        #     # com que podem tenir dades de diferents taules, agafem el maxim del df per tenir l'ultim timestamp
        #     # i li restem 12 setmanes, despres fem un filtre per obtenir nomes dades a partir d'alli
        #     last_date = max(df.index)
        #     weeks12 = timedelta(days=7 * 12)
        #     starting_date = last_date - weeks12
        #     hourly_data_device = False
        #     for name, group in grouped:
        #         if hourly_data_device:
        #             break
        #         energy_type_grouped = group.groupby('energyType')
        #         for energy_type, energy_type_group in energy_type_grouped:
        #             group_new = energy_type_group.reset_index().drop_duplicates(subset='date',
        #                                                                         keep='last').set_index(
        #                 'date').sort_index()
        #             group_new = group_new[starting_date:]
        #             freq = create_dataframes.calculate_frequency(group_new)
        #             # si no hi ha freq(nomes hi ha un timestamp o cap) ignora el device i ves al seguent
        #             if not freq:
        #                 continue
        #             day_delta = timedelta(hours=1)
        #             if freq <= day_delta:
        #                 # Si hi han dades horaries,  amb un frequencia mes petita que 1 dia en aquest modelling unit
        #                 # ho marquem i acabem amb el bucle
        #                 hourly_data_device = True
        #                 break
        #
        #     # Guardem a mongo el resultat per aquest device i energytype
        #     mongo_building.update(
        #         {"buildingId": buildingId, "companyId": self.company},
        #         {"$set": {"hourlyDataDevice.{}".format(modelling_unit): hourly_data_device}},
        #         upsert=False,
        #     )
        #     # Finalment, agafem el nou recurs de building i mirem si hi ha cap dispositiu amb dades horaries.
        #     building_doc = mongo_building.find_one({"buildingId": buildingId, "companyId": self.company})
        #     if building_doc['hourlyDataDevice']:
        #         mongo_building.update(
        #             {"buildingId": buildingId, "companyId": self.company},
        #             {"$set": {"hourlyData": any(building_doc['hourlyDataDevice'].values())}},
        #             upsert=False,
        #         )
        # mongo = self.mongo[self.config['mongodb']['db']][self.config['mongodb']['collection']]
        # mongo_weather = self.mongo[self.config['mongodb']['db']][
        #     self.config['mongodb']['weather_collection']]
        #
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "starting weather gathering"}},
        #     upsert=True)
        # # get station temperatures list
        # station_doc = mongo_weather.find_one({'stationId': station},{'values': True, 'timestamps': True, })
        # # if not station, finish.
        # if not station_doc:
        #     return

        # # create temperature dataframe
        # temps = []
        # for ts, t in zip(station_doc['timestamps'], station_doc['values']):
        #     val = {'date': ts, 'temperature': t}
        #     temps.append(val)
        # tdf = pd.DataFrame.from_records(temps, index='date', columns=['temperature', 'date'])
        # if model != 'Weekly30Min':
        #     tdf = tdf.reset_index().drop_duplicates(subset='date', keep='last').set_index('date')
        #     tdf = tdf.resample('60T').asfreq()  # weather a horari, el weather esta guardat a 30 min
        #
        # # delete temperature outlayers
        # outliers = dc.detect_min_threshold_outliers(tdf['temperature'], -50)
        # tdf['temperature'] = dc.clean_series(tdf['temperature'], outliers)
        # outliers = dc.detect_max_threshold_outliers(tdf['temperature'], 50)
        # tdf['temperature'] = dc.clean_series(tdf['temperature'], outliers)
        # outliers = dc.detect_znorm_outliers(tdf['temperature'], 30, mode="rolling", window=720)
        # tdf['temperature'] = dc.clean_series(tdf['temperature'], outliers)
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "finish_weather gathering and starting data"}},
        #     upsert=True)
        # df_new_montly = create_dataframes.create_daily_dataframe(grouped, multiplier)
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "finish data gathering and starting baseline monthly"}},
        #     upsert=True)
        # monthly_baseline = monthly_calc(modelling_unit, tdf, self.company, multipliers, model, df_new_montly)
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "finished monthly and start geting hourly_data"}},
        #     upsert=True)
        # df_new_hourly = create_dataframes.create_hourly_dataframe(grouped, multiplier, model)
        #
        # modellingUnit_doc = mongo_modellingUnits.find_one(
        #     {'companyId': int(self.company), 'modellingUnitId': modelling_unit})
        # # modellingUnit_created = modellingUnit_doc[
        # #     '_created'] if modellingUnit_doc and '_created' in modellingUnit_doc else datetime(1999, 1, 1)
        # # modellingUnit_updated = modellingUnit_doc[
        # #     '_updated'] if modellingUnit_doc and '_updated' in modellingUnit_doc else datetime(1999, 1, 1)
        # # created_updated = max([modellingUnit_created, modellingUnit_updated])
        # # yesterday = datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0) - timedelta(days=1)
        # # if created_updated > yesterday:
        # #     calculation_is_needed = True
        # # else:
        # #     calculation_is_needed = False
        # #
        # # # last ts already calculated
        # # ts_already_calculated = mongo.find_one({'modellingUnitId': modelling_unit, 'companyId': int(self.company)},
        # #                                        {'timestamps': 1})
        # # last_ts_already_calculated = ts_already_calculated['timestamps'][-1] if ts_already_calculated else 1
        # #
        # # # new condition to calculate or not
        # # if ts_list and last_ts_already_calculated == ts_list[-1] and not calculation_is_needed:
        # #     return
        #
        # check if it is waterConsumption or not
        # energy_type = modellingUnit_doc['energyType'] if modellingUnit_doc and \
        #                                              'energyType' in modellingUnit_doc else None
        # #### NYAPA
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "start hourly baseline"}},
        #     upsert=True)
        # hourly_baseline = {}
        # if '80d32f1a-dee3-5e54-a64a-4460fdcfbaff' == modelling_unit:
        #     hourly_baseline = baseline_calc_pyemis_new(df_new_hourly, tdf, model, energy_type, iters=16)
        # else:
        #     hourly_baseline = baseline_calc_pyemis_old(df_new_hourly, tdf, model, energy_type, iters=16)
        #
        # self.mongo[self.config['mongodb']['db']]['debug'].update(
        #     {'task_id': self.task_id},
        #     {'$push': {'debug': "finished hourly baseline"}},
        #     upsert=True)
        #
        # baseline =  {
        #     'companyId': int(self.company),
        #     'devices': str(multipliers),
        #     'modellingUnitId': modelling_unit,
        #     'stationId': station,
        #     '_created': datetime.now()
        # }
        #
        # baseline.update(monthly_baseline)
        # baseline.update(hourly_baseline)
        #
        # mongo.update(
        #     {'modellingUnitId': modelling_unit, 'companyId': int(self.company)},
        #     {"$set": baseline},
        #     upsert = True
        # )


if __name__ == '__main__':
    MRJob_align.run()    