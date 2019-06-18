#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_baseline_hourly_module

#python to use
python_v=/usr/bin/python2.7

#pythonpath to add (list)
python_path=(
)

# export variables
to_export=(
)

# celery queue to add this task
queue=modules

#whether the virtualenv should be (re)installed each execution or not
debug=0

#current dir path
pwd=`pwd`




