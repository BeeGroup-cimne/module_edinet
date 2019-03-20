#-*- coding: utf-8 -*-
"""
use migrate_metering to migrate:

 edinet:
 - unknownConsumption_1092915978
 - unknownReadings_1092915978
 - electricityConsumption_1092915978
 - waterConsumption_1092915978
 - electricityReadings_1092915978

 dexma:
 - electricityConsumption_5052736858

 inergy:
 - electricityConsumption_3230658933

 manuals:
 - electricityConsumption_7104124143

 gemweb
 - electricityConsumption_8801761586
 - tertiaryElectricityConsumption_8801761586

use migrate_csv_inergy to migrate:
 inergy:
 - tertiaryElectricityConsumption_3230658933


danger:
 gas consumption ??
 - gasConsumption_1092915978
 - gasConsumption5052736858
 - gasConsumption_3230658933
 - gasConsumption_7104124143
 - gasConsumption_8801761586
 bad tertiarys
 - tertiaryElectricityConsumption_1092915978
 - tertiaryElectricityConsumption_7104124143

"""

import happybase
import json
import sys
table_name = sys.argv[1]

old_table_name = "edinet_old_{}".format(table_name)

table_name = "edinet_metering_{}".format(table_name)

config = json.load(open("module_edinet/config.json"))

hbase = happybase.Connection(config['hbase']['host'], int(config['hbase']['port']), timeout=90000)

tables = hbase.tables()

if table_name in tables:
    raise Exception("Table already migrated")

hbase.create_table(table_name, {"m":dict()})

new_table = hbase.table(table_name)

old_table = hbase.table(old_table_name)
buffer = 10000
key = None
last_row = None
while True:
    print(key)
    if key:
        cursor = old_table.scan(row_start=key, limit=buffer)
    else:
        cursor = old_table.scan(limit=buffer)
    batch = new_table.batch()
    for x in cursor:
        key, value = x
        new_key = "~".join(key.split('~')[1:])
        batch.put(new_key, value)
    batch.send()
    if key == last_row:
        break
    last_row = key
