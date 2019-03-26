from datetime import datetime, timedelta

import happybase
import json
import sys
import pandas as pd
import pyhs2
from hive_functions import create_hive_table_from_hbase_table

table_name = sys.argv[1]

new_table_name = "edinet_billing_{}".format(table_name)

config = json.load(open("module_edinet/config.json"))

hbase = happybase.Connection(config['hbase']['host'], int(config['hbase']['port']), timeout=90000)
hive = pyhs2.connect(host=config['hive']['host'],
                             port=int(config['hive']['port']),
                             authMechanism='PLAIN', user=config['hive']['username'], password="")


tables = hbase.tables()
if new_table_name in tables:
    raise Exception("Table already migrated")

old_keys = [["bucket","bigint"], ["ts_end","bigint"], ["deviceId","string"]]
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
        data.append({"device": key['deviceid'], "ts_end": datetime.utcfromtimestamp(float(key['ts_end'])), "value": value})
    except Exception as e:
        print("key: {}".format(key))
        print("value: {}".format(value))
        print ("******************************************************")


df = pd.DataFrame(data)
sentence = "DROP TABLE {}".format(old_table_hive)
cur.execute(sentence)

removed_data_points = 0
for device, df_data in df.groupby("device"):
    df_data.index = df_data.ts_end
    df_data.sort_index(inplace=True)
    #add ts_ini as the previous ts_end +1 day. Until gap detection
    df_data['ts_ini'] = df_data.ts_end.shift(+1)+timedelta(days=1)
    if len(df_data) >= 3:
        df_data['days'] = (df_data['ts_end'] - df_data['ts_end'].shift(+1)).dt.days
        df_data['mean_consumption'] = df_data['value'] / df_data['days']
        # Calculo el mode, la mitjana i la mediana dels dies
        mode_days = df_data['days'].mode()
        mean_days = df_data['days'].mean()
        median_days = df_data['days'].median()
        # Calculo el mode, la mitjana i la mediana del consum
        mean_consumption = df_data['mean_consumption'].mean()
        median_consumption = df_data['mean_consumption'].median()
        mode_consumption = df_data['mean_consumption'].mode()

        df_data['ts_ratio_mean'] = df_data['days'] / mean_days
        df_data['value_ratio_mean'] = mean_consumption / df_data['mean_consumption']
        df_data['value_ratio_mean'] = [x if x <= 3 else 3 for x in df_data['value_ratio_mean']]

        # gap detection

        df_data['gap_ts'] = [1 if x > 1.7 else 0 for x in df_data['ts_ratio_mean']]
        df_data['gap_value'] = [1 if x > 1.7 else 0 for x in df_data['value_ratio_mean']]
        df_data['gap_total'] = df_data['gap_ts'] * df_data['gap_value']
    else:
        df_data['gap_total'] = 0
    removed_data_points += len(df_data[df_data.gap_total==1])
    df_end = df_data[df_data.gap_total==0][['ts_ini','ts_end','value']]
    #save data to new hbase table
    hbase_table = hbase.table(new_table_name)
    batch = hbase_table.batch()
    for v in df_end.iterrows():
        key = "{}~{}~{}".format(v['ts_ini'], v['ts_end'], device)
        row = {"m:v",v['value']}
        batch.put(key, row)
    batch.send()
print("Removed {} data points, that is a {}% of total data".format(removed_data_points, removed_data_points*100/len(df)))