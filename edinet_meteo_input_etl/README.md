## Edinet meteo measures

This module is used to upload to the backend the meteo information for a meteo station.

It will read the data from "MongoDB" and put it to the HBASE table.

### module configuration

```json
{
    "mongodb": {
        "collection": "metering_DB_name"
    },
    "hbase_table": {
        "name": "HBase table name",
        "key": [
            "timestamp",
            "deviceId"
        ],
        "cf": [
            {
                "fields": [
                    "v"
                ],
                "name": "m"
            }
        ]
    },
    "backup_folder": "folder for backup",
    "error_measures": "folder to store erroneous data points"


}
```

### How to run:

``` python
from module_edinet.edinet_meteo_input_etl.task import ETL_mh_hadoop
from datetime import datetime
params = {}
t = ETL_mh_hadoop()
t.run(params) 
```
 