## Edinet clean meteo data

This module is used to clean the meteo data.

It will read the data from Hbase, apply different treatment for metering data, and generate a clean Hive table for the use of the rest of analitycs modules.

#### data clean
The module will identify the following timeseries errors.

Outliers over a maximum thershold (tehorical max temp)
Outliers under a minimum threshold (tehorical min temp)
Outliers following a znorm with a moving window of one week
Duplicated/overlapping data (Data that has been introudced twice or more times to the system. We keep the last appearing information)
Missing/Gaps data = Detected days with missing data, or concurrent periods.


**All the treated information can be found in a raw data MongoDB collection**   
### module configuration

```json
{
    "measures":[
        {
            "type": "meteo",
            "hbase_table": "edinet_meteo",
            "temp_input_table": "edinet_meteo_data_input",
            "hbase_keys": [["ts","bigint"], ["stationId","string"]],
            "hbase_columns": [["temperature", "float", "m:temperature"]],
            "hive_fields": [["ts", "bigint"], ["stationId", "string"], ["temperature", "float"]],
            "sql_sentence_select": [["ts", "{var}.key.ts"], ["stationId", "{var}.key.stationId"] ,
                ["temperature", "{var}.temperature"]],
            "sql_where_select": "{var}.key.ts >= UNIX_TIMESTAMP('{ts_from}','yyyy-MM-dd HH:mm:ss') AND {var}.key.ts <= UNIX_TIMESTAMP('{ts_to}','yyyy-MM-dd HH:mm:ss')",
            "measures": "/tmp/edinet_clean_meteo_data/{UUID}/measures_meteo",
            "clean_output_file": "/tmp/edinet_clean_meteo_data/{UUID}/measures_meteo_clean",
            "clean_output_table": "edinet_clean_meteo"
        }
    ],
    "output": {
        "output_file_name": "/data/edinet/meteo_data",
        "output_hive_table": "edinet_meteo",
        "fields": [["ts", "bigint"],["stationId", "string"], ["temperature", "float"]],
        "sql_sentence_select": [["ts", "{var}.ts"], ["stationId", "{var}.stationId"] ,["temperature", "{var}.temperature"]]
    },
    "threshold": {
        "max": 70,
        "min": -70
    }
}
```

### How to run:

``` python
from module_edinet.edinet_clean_meteo_data_etl.task import ETL_clean_meteo
from datetime import datetime
params = {
    "result_companyId": "1092915978",
    "ts_to": datetime(2018,12,01)
}
t = ETL_clean_meteo()
t.run(params) 
```
 