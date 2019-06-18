#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=task.py

# module name
task_name=edinet_gam_baseline_prediction

#python to use
python_v=/usr/bin/python3.5

#pythonpath to add (list)
python_path=(
    /opt/BeeDataBackend/module_edinet/model_functions
)

# export variables
to_export=(
    R_HOME=/usr/lib/R
)

# celery queue to add this task
queue=modules

#whether the virtualenv should be (re)installed each execution or not
debug=1

#current dir path
pwd=`pwd`
