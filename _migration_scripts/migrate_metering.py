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
