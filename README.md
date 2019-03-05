# EDINET Data Analysis
Project to execute the data analysis required for the Edinet and similar projects.

## Configuration of module in 10 steps

1- Clone the edinet in the "projects" folder.

2- Set the config.json with the proper configuration

{
	"mongodb" : {
		"host": "hostname",
		"port": 1234,
		"username": "username",
		"password": "password",
		"db": "database"
	},
	"hdfs" : {
		"host": "hostname",
		"port": 1234
	},
	"hive" : {
		"host": "hostname",
		"port": 1234,
		"username": "username"
	},
	"hbase" : {
		"host": "hostname",
		"port": 1234
	},
	"report": {
		"collection": "module_task_reports",
		"etl_collection": "etl_task_reports",
		"timeline_collection": "etl_automatic_launch_timeline"
	},
	"config_execution": {
		"path": "tmp_path/config_execution",
		"tertiary_periods" : "tertiary_periods",
		"tou_periods": "tou_periods",
		"last_contracts_ETL": "last_execution_contracts_ETL",
		"streaming_jar": "/usr/lib/hadoop/hadoop-streaming.jar"
	}
}
3- Set the variables in general_variables.sh

#! /bin/bash

# virtualenv exec path
export virtualenv_path=/path/to/virtualenv

# pip server certificate
export cert=/path/to/devpi/certificate.pem

4- Set the module config.jsonif required(if it includes the same information as parent config.json it will override this information)

5- Set the variables in module_variables.sh

#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py #if no changes

# module name
task_name= "name of the module"

# celery queue to add this task
queue=modules # or ETL

#whether the virtualenv should be (re)installed each execution or not
debug=0

#current dir path
pwd=`pwd`

6- Install the modules by running . install.sh in the parent directory.

7- Add the module/tasks.py to celery router.

## Modules structure.
The project is structured in three different phases of modules:

### 1. Data gathering
Used to obtain the data from different resources and upload it to HBase
- [edinet_billing_measures_etl](edinet_billing_measures_etl/README.md)
- [edinet_metering_measures_etl](edinet_metering_measures_etl/README.md)
- [edinet_meteo_measures](edinet_meteo_input_etl/README.md)

### Data cleaning and errors detection
Used to clean and detect the errors on the uploaded data.
- [edinet_clean_daily](edinet_clean_daily_data_etl/README.md)
- [edinet_clean_hourly](edinet_clean_hourly_data_etl/README.md)
- [edinet_clean_meteo](edinet_clean_meteo_data_etl/README.md)

### Analytics
- [edinet_baseline_hourly](edinet_baseline_hourly_module/README.md)
- [edinet_baseline_monthly](edinet_baseline_monthly_module/README.md)
- [edinet_comparison](edinet_comparison_module/README.md)

