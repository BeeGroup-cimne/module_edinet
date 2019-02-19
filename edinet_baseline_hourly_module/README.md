## Edinet calculate horuly baseline

This module is used to calculate the hourly baseline using the PyEMIS library

It will read the clean data from HIVE, select the training period, train the model, and calculate the predicition.


### module configuration

```json
{
  "paths": {
    "all": "/tmp/edinet_baseline/{UUID}",
    "measures": "/tmp/edinet_baseline/{UUID}/measures",
    "stations": "/tmp/edinet_baseline/{UUID}/stations"
  },
  "mongodb": {
    "modelling_units_collection": "modelling_units",
    "building_collection": "buildings",
    "collection": "baselines",
    "weather_collection": "stations_measures",
    "reporting_collection": "reporting_units"
  }
}
```

### How to run:

``` python
from module_edinet.edinet_baseline_hourly_module.task import BaselineModule
from datetime import datetime
params = {
   'result_companyId': 1092915978,
   'type': 'electricityConsumption',
   'ts_to': datetime(2018, 6, 1, 23, 59, 59)
}
t = BaselineModule()
t.run(params) 
```
 