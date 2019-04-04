#-*- coding: utf-8 -*-
"""


revision process for:
  "tertiaryElectricityConsumption_1092915978"
  "tertiaryElectricityConsumption_7104124143"
  "gasConsumption_1092915978"
  "gasConsumption_5052736858"
  "gasConsumption_3230658933"
  "gasConsumption_7104124143"
  "gasConsumption_8801761586"

"""
import calendar
from datetime import datetime, timedelta

import happybase
import json
import sys
import pandas as pd
import pyhs2
from hive_functions import create_hive_table_from_hbase_table
from pymongo import MongoClient


def calculate_frequency(dataset):
    if len(dataset.index) > 1:
        return (pd.Series(dataset.index[1:]) - pd.Series(dataset.index[:-1])).value_counts().index[0]
    else:
        return None

def datetime_to_timestamp(ts):
    # Input data is always in UTC and the timestamp stored in HBase must be in UTC timezone.
    try:
        return calendar.timegm(ts.to_pydatetime().utctimetuple())
    except:
        return None



config = json.load(open("module_edinet/config.json"))

hbase = happybase.Connection(config['hbase']['host'], int(config['hbase']['port']), timeout=90000)
hive = pyhs2.connect(host=config['hive']['host'],
                             port=int(config['hive']['port']),
                             authMechanism='PLAIN', user=config['hive']['username'], password="")



def export_table(table_name):
    old_keys = [["ts","bigint"], ["deviceId","string"]]
    columns = [["value", "float", "m:v"]]
    cur = hive.cursor()

    old_table_hive = create_hive_table_from_hbase_table(cur, table_name, table_name, old_keys, columns, "migration000001")

    cur.execute("select * from {}".format(old_table_hive))
    data = []
    while cur.hasMoreRows:
        try:
            ret_val = cur.fetchone()
            if not ret_val:
                print("returned None")
                continue
            key, value = ret_val
            key = json.loads(key)
            data.append({"device": key['deviceid'], "ts": datetime.utcfromtimestamp(float(key['ts'])), "value": value})
        except Exception as e:
            print("key: {}".format(key))
            print("value: {}".format(value))
            print ("******************************************************")


    df = pd.DataFrame(data)
    sentence = "DROP TABLE {}".format(old_table_hive)
    cur.execute(sentence)
    return df
    df.to_csv("exported-{}.csv".format(table_name))

def export_table_2(table_name):
    old_keys = [["ts_ini","bigint"], ["ts_end","bigint"], ["deviceId","string"]]
    columns = [["value", "float", "m:v"]]
    cur = hive.cursor()

    old_table_hive = create_hive_table_from_hbase_table(cur, table_name, table_name, old_keys, columns, "migration000001")

    cur.execute("select * from {}".format(old_table_hive))
    data = []
    while cur.hasMoreRows:
        try:
            ret_val = cur.fetchone()
            if not ret_val:
                print("returned None")
                continue
            key, value = ret_val
            key = json.loads(key)
            if key['ts_ini']:
                data.append({"device": key['deviceid'], "ts_end": datetime.utcfromtimestamp(float(key['ts_end'])), "ts_ini": datetime.utcfromtimestamp(float(key['ts_ini'])), "value": value})
            else:
                data.append({"device": key['deviceid'], "ts_end": datetime.utcfromtimestamp(float(key['ts_end'])), "ts_ini": None, "value": value})

        except Exception as e:
            print("key: {}".format(key))
            print("value: {}".format(value))
            print ("******************************************************")


    df = pd.DataFrame(data)
    sentence = "DROP TABLE {}".format(old_table_hive)
    cur.execute(sentence)

    df.to_csv("exported-{}.csv".format(table_name))


def read_analysis(file):
    df = pd.read_csv(file)
    df = df.set_index("ts")
    df.index = pd.to_datetime(df.index)
    for _, x in df.groupby("device"):
        print(calculate_frequency(x))
        print(x[["value"]])

def read_analysis_2(file):
    df = pd.read_csv(file)
    df = df.set_index("ts_ini")
    df.index = pd.to_datetime(df.index)
    df.ts_end = pd.to_datetime(df.ts_end)
    for _, x in df.groupby("device"):
        print(x.ts_end-x.index)
        #print(x[["ts_end","value"]])

def clean_error(df, table_name):
    df = df.set_index("ts")
    df.index = pd.to_datetime(df.index)
    #new_table_name = "edinet_billing_{}".format(table_name)
    # try:
    #     hbase.create_table(new_table_name, {'m': dict()})
    # except:
    #     pass
    # hbase_table = hbase.table(new_table_name)

    for device, x in df.groupby("device"):
        dfx = x[x.index <= datetime.now()]
        #dfx = dfx[dfx.index.day>20]
        dfx['ts_end'] = dfx.index
        dfx['ts_ini'] = dfx.ts_end.shift(+1)+timedelta(days=1)
        print(dfx)
        # batch = hbase_table.batch()
        # for _, v in dfx.iterrows():
        #     key = "{}~{}~{}".format(datetime_to_timestamp(v['ts_ini']),
        #                             datetime_to_timestamp(v['ts_end']),
        #                             device)
        #     row = {"m:v": str(v['value'])}
        #     batch.put(key, row)
        # batch.send()


import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
import plotly.offline as py
import plotly.graph_objs as go
from plotly import tools


mongo_old= MongoClient("37.59.27.175", 27017)
mongo_old = mongo_old["edinet_rest_service"]
mongo_old.authenticate("cimne-edinet","3nm1C--3d1n3t")

mongo_new = MongoClient("217.182.160.171", 27017)
mongo_new = mongo_new['edinet']
mongo_new.authenticate("bgruser", "gR0uP_b33u$er")

def review_devices(mongo_old, mongo_new):
    energy_type_map={"tertiaryElectricityConsumption":"electricityConsumption", "monthlyElectricityConsumption": "electricityConsumption"}
    data_old = mongo_old['raw_data'].find({})
    devices = data_old.distinct('deviceId')
    for deviceId in devices[0:10]:
        data_old = mongo_old['raw_data'].find({"deviceId": deviceId})
        data_new = mongo_new['raw_data'].find({"device": deviceId})
        data_map = {}
        for i, x in enumerate(data_old):
            df = pd.DataFrame({"ts": x['timestamps'], "value": x['values']})
            df.index = pd.to_datetime(df["ts"])
            energy_type = energy_type_map[x['type']] if x['type'] in energy_type_map else x['type']
            data_map["{}_{}".format(energy_type,x['companyId'])] = {'old': df.values.sum()}
        for x in data_new:
            if x['data_type'] == 'metering':
                df2 = pd.DataFrame.from_records(x['raw_data'])
                df2.index = pd.to_datetime(df2.ts)
                energy_type = energy_type_map[x['energy_type']] if x['energy_type'] in energy_type_map else x['energy_type']
                data_map["{}_{}".format(energy_type, x['companyId'])] = {'new': df2.values.sum()}

            if x['data_type'] == 'billing':
                df2 = pd.DataFrame.from_records(x['raw_data'])
                df2.index = pd.to_datetime(df2.ts_end)
                energy_type = energy_type_map[x['energy_type']] if x['energy_type'] in energy_type_map else x['energy_type']
                data_map["{}_{}".format(energy_type, x['companyId'])] = {'new': df2.values.sum()}

    return return_list


def get_raw_data(deviceId, mongo_old, mongo_new):
    data_old = mongo_old['raw_data'].find({"deviceId": deviceId})
    data_new = mongo_new['raw_data'].find({"device": deviceId})
    fig = tools.make_subplots(rows=data_old.count(), cols=1, shared_xaxes=False)
    chart_map = {}
    for i, x in enumerate(data_old):
        df = pd.DataFrame({"ts":x['timestamps'], "value": x['values']})
        df.index = pd.to_datetime(df["ts"])
        data_1 = go.Scatter(x=df.index.tolist(), y=df.value.tolist(), name=str('old {}'.format(x['companyId'])))
        fig.append_trace(data_1,i+1,1)
        chart_map[str(x['companyId'])] = i+1

    print(chart_map)
    for x in data_new:
        if x['data_type'] == 'metering':
            df2 = pd.DataFrame.from_records(x['raw_data'])
            df2.index = pd.to_datetime(df2.ts)
            data_2 = go.Scatter(x=df2.index.tolist(), y=df2.value.tolist(), name=str('new {}'.format(x['source'])))
            fig.append_trace(data_2,chart_map[str(x['source'])],1)
        if x['data_type'] == 'billing':
            df2 = pd.DataFrame.from_records(x['raw_data'])
            df2.index = pd.to_datetime(df2.ts_end)
            data_2 = go.Scatter(x=df2.index.tolist(), y=df2.value.tolist(), name=str('new {}'.format(x['source'])))
            fig.append_trace(data_2,chart_map[str(x['source'])],1)

    py.plot(fig, filename='basic-line',)

df = get_raw_data("ES0031405013365002YV0F",mongo_old, mongo_new)
df = get_raw_data("2ebd708e-255f-58d0-93a8-fad0c260e44f",mongo_old, mongo_new)
df = get_raw_data("85789d3a-41fc-5ecb-addc-a8190f3d06f3",mongo_old, mongo_new)
df = get_raw_data("IT001E68702847",mongo_old, mongo_new)
df = get_raw_data("01611310047317",mongo_old, mongo_new)
df = get_raw_data("85789d3a-41fc-5ecb-addc-a8190f3d06f3",mongo_old, mongo_new)
df = get_raw_data("ES0031406042792001RP0F",mongo_old, mongo_new)
df = get_raw_data("ES0217010126037241LD",mongo_old, mongo_new)
df = get_raw_data("oil heating",mongo_old, mongo_new)
df = get_raw_data("02765274",mongo_old, mongo_new)