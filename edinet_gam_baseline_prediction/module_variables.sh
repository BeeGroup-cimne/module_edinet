#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_gam_baseline_prediction

#python to use
python_v=/usr/local/Cellar/python/3.7.1/bin/python3.7

#pythonpath to add (list)
python_path=(
    /Volumes/DataDisk/docker/module_edinet/model_functions
    /Volumes/DataDisk/docker/module_edinet/model_functions2
)

# export variables
to_export=(
    R_HOME=/Library/Frameworks/R.framework/Resources
    R_HOME3=/Library/Frameworks/R.framework/Resources3
)

# celery queue to add this task
queue=modules

#whether the virtualenv should be (re)installed each execution or not
debug=1

#current dir path
pwd=`pwd`