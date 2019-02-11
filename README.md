# EDINET Data Analysis

## Modules available.
- [edinet_billing_measures_etl](edinet_billing_measures_etl/README.md)
- [edinet_metering_measures_etl](edinet_metering_measures_etl/README.md)
- edinet_meteo_measures
- edinet_clean_daily
- edinet_clean_hourly
- edinet_clean_meteo
- edinet_baseline_hourly
- edinet_baseline_monthly

## Instructions to run the modules
### edinet_baseline
```python
from datetime import datetime
from module_edinet.tasks import edinet_baseline
params = {
   'companyId': 1092915978,
   'companyId_toJoin': [3230658933],
   'type': 'electricityConsumption',
   'ts_to': datetime(2016, 12, 31, 23, 59, 59)
}
edinet_baseline.delay(params)
```

