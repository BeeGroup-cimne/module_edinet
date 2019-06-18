#! /bin/bash

. ../general_variables.sh
# name of exec file of module
task_exec_file=<task file executable>

# module name
task_name=<name of the module(dir name)>

#python to use
python_v=/usr/local/Cellar/python/3.7.1/bin/python3.7

#pythonpath to add (list)
python_path=(
    <path_to_add_1>
    <path_to_add_2>
)

# export variables
to_export=(
    <variable_to_add>=<value>
    R_HOME=/Library/Frameworks/R.framework/Resources
)

# celery queue to add this task
queue=<queue: (etl, module)>

#whether the virtualenv should be (re)installed each execution or not
debug=<(0,1)>

#current dir path
pwd=`pwd`