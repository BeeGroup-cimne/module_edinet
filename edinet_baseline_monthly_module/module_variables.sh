#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_baseline_monthly_module

# celery queue to add this task
queue=modules

#whether the virtualenv should be (re)installed each execution or not
debug=0

#current dir path
pwd=`pwd`