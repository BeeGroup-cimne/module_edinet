## Edinet clean hourly data

This module is used to clean the data in a hourly base, taking the information from both the metering and billing tables.

It will read the data from Hbase, apply different treatment for metering data, and generate a clean Hive table for the use of the rest of analitycs modules.

_*Only metering data with granularity under 1 hour will be used_

#### data clean
The module will identify the following timeseries errors.

Outliers over a maximum thershold (usually the teorical maximum for a "energyType" consumption device)
Outliers under a minimum threshold (negative consumption can not occurr)
Outliers following a znorm with a moving window of one week
Duplicated/overlapping data (Data that has been introudced twice or more times to the system. We keep the last appearing information)
Missing/Gaps data = Detected days with missing data, or concurrent periods.


**All the treated information can be found in a raw data MongoDB collection**   
### module configuration

```json
{
    "measures":[
        {
            "type": "metering",
            "hbase_table": "edinet_metering",
            "temp_input_table": "edinet_metering_data_input",
            "hbase_keys": [["ts","bigint"], ["deviceId","string"]],
            "hbase_columns": [["value", "float", "m:v"], ["accumulated", "float", "m:a"]],
            "hive_fields": [["ts", "bigint"], ["deviceId", "string"], ["value", "float"], ["accumulated", "float"],
                ["energyType", "string"], ["source", "string"]],
            "sql_sentence_select": [["ts", "{var}.key.ts"], ["deviceId", "{var}.key.deviceId"] ,["value", "{var}.value"],
                ["accumulated", "{var}.accumulated"], ["energyType", "'{energy_type}' as energyType"], ["source", "'{source}' as source"]],
            "sql_where_select": "{var}.key.ts >= UNIX_TIMESTAMP('{ts_from}','yyyy-MM-dd HH:mm:ss') AND {var}.key.ts <= UNIX_TIMESTAMP('{ts_to}','yyyy-MM-dd HH:mm:ss')",
            "measures": "/tmp/edinet_clean_hourly_data/{UUID}/measures_metering",
            "clean_output_file": "/tmp/edinet_clean_hourly_data/{UUID}/measures_metering_clean",
            "clean_output_table": "edinet_clean_metering"
        }
    ],
    "output": {
        "output_file_name": "/data/edinet/hourly_consumption",
        "output_hive_table": "edinet_hourly_consumption",
        "fields": [["ts", "bigint"], ["deviceId", "string"], ["value", "float"], ["energyType", "string"], ["source", "string"],["data_type", "string"]],
        "sql_sentence_select": [["ts", "{var}.ts"], ["deviceId", "{var}.deviceId"] ,["value", "{var}.value"],
            ["energyType", "{var}.energyType"], ["source", "{var}.source"], ["data_type", "'{data_type}' as data_type"]]
    },
    "max_threshold": {
        "electricityConsumption": 1400,
        "gasConsumption": 7000,
        "default": 10000
    }
}
```

### How to run:

``` python
from module_edinet.edinet_clean_hourly_data_etl.task import ETL_clean_hourly
from datetime import datetime
params = {
    "result_companyId": "1092915978",
    "data_companyId": ["3230658933","1512441458"],
    "ts_to": datetime(2018,12,01)
}
t = ETL_clean_hourly()
t.run(params) 
```
 