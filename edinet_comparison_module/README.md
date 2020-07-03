## Edinet comparison module

This module is used to compare the consumption of different buildings

It will gather the information of the building, and it's consumptions and compare them using the desired groups.

### module configuration

```json
{
  "paths": {
    "all": "/tmp/edinet_comparison/{UUID}",
    "monthly_aggregation": "/tmp/edinet_comparison/{UUID}/monthly",
    "output_monthly_aggregation": "/tmp/edinet_comparison/{UUID}/output_monthly",
    "building_info": "/tmp/edinet_comparison/{UUID}/building_info",
    "benchmarking_data": "/tmp/edinet_comparison/{UUID}/benchmarking_data"
  },
  "settings": {
    "comparation_criteria": ["type", "organization+type"],
    "min_elements_benchmarking": 10,
    "breaks":[5, 25, 50, 75, 90]
   },
  "mongodb":{
    "buildings_collection": "buildings",
    "modelling_units_collection": "modelling_units",
    "montly_data_collection": "monthly_aggregation",
    "benchmarking_collection": "benchmarking",
    "reporting_collection": "reporting_units"
  },
  "hive": {
    "final_table_fields": [["deviceId", "string", "a"], ["ts", "int" ,"a"], ["value", "float", "a"], ["energyType", "string", "a"], ["source", "string", "a"]],
    "benchmarking_table_fields": [["modellingunit", "string", "a"], ["ts", "int" ,"a"], ["value", "float", "a"],
                                  ["energyType", "string", "a"], ["type", "string" ,"b"], ["organization", "string", "b"]],
    "job_table_name": "edinet_aggregate_month_input",
    "benchmarking_table": "edinet_benchmarking_input",
    "output_monthly_aggregation": "edinet_monthly_aggregation",
    "building_info_table": "edinet_building_info"
  },
  "companies_preferences": ["7104124143", "5052736858", "8801761586","3230658933","1092915978"]
}
```

### How to run:

``` python
from module_edinet.edinet_comparison_module.task import ComparisonModule
from datetime import datetime
params = {
    'result_companyId': 1092915978,
    'ts_to': datetime(2019,7,9)
}
t = ComparisonModule()
t.run(params) 
```
 