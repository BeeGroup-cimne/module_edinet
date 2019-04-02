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
