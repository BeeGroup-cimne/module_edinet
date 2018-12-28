#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_weather_hadoop_etl

# celery queue to add this task
queue=etl

#whether the virtualenv should be (re)installed each execution or not
debug=0

#current dir path
pwd=`pwd`