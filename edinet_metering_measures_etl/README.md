## Edinet metering measures

This module is used to upload to the backend the metering information of the devices.

It will read the data from "MongoDB" and put it to the HBASE table.

### How to run:

``` python
from module_edinet.edinet_metering_measures_etl.task import ETL_mh_hadoop_tertiary
from datetime import datetime
params = {}
t = ETL_mh_hadoop_tertiary()
t.run(params) 
```
 