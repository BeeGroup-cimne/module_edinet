## Edinet billing measures

This module is used to upload to the backend the billing information of the devices.

It will read the data from "MongoDB" and put it to the HBASE table.

### How to run:

``` python
from module_edinet.edinet_billing_measures_etl.task import ETL_mh_hadoop_billing
from datetime import datetime
params = {}
t = ETL_mh_hadoop_billing()
t.run(params)
```
 