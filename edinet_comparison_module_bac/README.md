## Edinet calculate building benchmarking

This module is used to calculate the benchmarking of consumption depending on different criterias.

It takes the buildings, organized with different fields or criterias and calculates the group's stadistical study.

For each building, search the best group to compare. and show the results of both calculations.

It will read the clean data from HIVE.
### module configuration

```json
{
  "paths": {
    "all": "/tmp/edinet_comparison/{UUID}",
    "results_similarUsers": "/tmp/edinet_comparison/{UUID}/results_similarUsers",
    "measures": "/tmp/edinet_comparison/{UUID}/measures",
    "joined": "/tmp/edinet_comparison/{UUID}/joined",
    "summary_result_dir": "hdfs:///tmp/edinet_comparison/{UUID}/mean_values_text",
    "result_dir": "hdfs:///tmp/edinet_comparison/{UUID}/result"
  },
  "settings": {
    "similar_users_groups": {
      "min_consumption_percentile": 5,
      "mongo_collection_best_criteria": "edinet_similar_groups_best_criteria",
      "group_volume_limits": [
          60,
          2000,
          8000
      ],
      "fixed_percentage_for_cost": [
          50
      ],
      "max_consumption_percentile": 95,
      "group_volume_penalties": [
          50,
          3
      ],
      "hbase_table_best_criteria": "edinet_similar_groups_best_criteria",
      "n_months_for_best_criteria": [
          12
      ],
      "mongo_collection_dist": "edinet_similar_groups_description",
      "cases": [
          {
              "contracts_to_avoid": null
          }
      ],
      "hbase_table_dist": "edinet_similar_groups_description"
    },
    "minimum_valid_power": 250,
    "percentage_max_power": 80,
    "max_consumption_per_day": 1000,
    "balance_temperature_for_hdd_cdd_calculation": 20
  },
  "mongodb":{
    "buildings_collection": "buildings",
    "reporting_collection": "reporting_units",
    "modelling_units_collection": "modelling_units",
    "collection": "comparisons"
  }
}
```

### How to run:

``` python
from module_edinet.edinet_comparison_module.task import ComparisonModule
from datetime import datetime
params = {
    'result_companyId': 1092915978,
    'ts_to': datetime(2017, 12, 31, 23, 59, 59),
    'criteria': ['entityId + postalCode + useType', 'entityId + useType']
 }
t = ComparisonModule()
t.run(params) 
```
 