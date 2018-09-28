# EDINET Backend project

## Modules available.
- edinet_baseline

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

