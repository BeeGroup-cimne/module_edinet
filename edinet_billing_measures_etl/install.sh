#! /bin/bash
if [ ! -f "module_variables.sh" ]; then
    echo "module not configured"
    return
fi
. module_variables.sh

function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }
sed "s/{{task_exec_file}}/$task_exec_file/; s/{{task_name}}/$task_name/; s/{{queue}}/$queue/" ../.modules_config_files/launcher.py > launcher.py

ve_p="${virtualenv_path//\//\\/}"
ce_p="${cert//\//\\/}"
pw_t="${pwd//\//\\/}"
py_v="${python_v//\//\\/}"
py_pa=`join_by : ${python_path[@]}`
py_pa="${py_pa//\//\\/}"
en_var="`printf 'export %s;' ${to_export[@]}`"
en_var="${en_var//\//\\/}"
sed "s/{{python_path}}/$py_pa/; s/{{environment}}/$en_var/; s/{{pwd}}/$pw_t/; s/{{task_name}}/$task_name/; s/{{debug}}/$debug/; s/{{virtualenv_path}}/$ve_p/;s/{{python_v}}/$py_v/; s/{{cert}}/$ce_p/" ../.modules_config_files/mrjob.conf > mrjob.conf

$virtualenv_path venv -p $python_v
. venv/bin/activate
pip install -r $pwd/requirements.txt --cert $cert
deactivate