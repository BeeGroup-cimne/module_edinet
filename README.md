# CREATE A DATA ANALYSIS PROJECT

## Follow this 10 steeps:

1- Copy the project_template in the "celery" folder.

2- Delete the `.git` folder of the template project and start a new git for the project
```bash
rm -rf .git
```
3- Set the `config.json` with the proper configuration
```json
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
```

4- Set the variables in `general_variables.sh`
```bash
#! /bin/bash

# virtualenv exec path
export virtualenv_path=/path/to/virtualenv

# pip server certificate
export cert=/path/to/devpi/certificate.pem

```

5- Copy and edit the example module folder `module_template`

6- Write the code of the new module. All mapreduce required files must be in the same folder or installed in the virtualenv through the `requirements.txt` file

7- Set the module `config.json`if required(if it includes the same information as parent `config.json` it will override this information)

8- Set the variables in `module_variables.sh`
```bash
#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_baseline

# celery queue to add this task
queue=modules

#whether the virtualenv should be (re)installed each execution or not
debug=0

#current dir path
pwd=`pwd`
```

9- Install the modules by running `. install.sh` in the parent directory.

10- Add the module/tasks.py to celery router.
