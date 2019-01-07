#! /bin/bash
. module_variables.sh

sed "s/{{task_exec_file}}/$task_exec_file/; s/{{task_name}}/$task_name/; s/{{queue}}/$queue/" ../.modules_config_files/launcher.py > launcher.py

ve_p="${virtualenv_path//\//\\/}"
ce_p="${cert//\//\\/}"
pw_t="${pwd//\//\\/}"

sed "s/{{pwd}}/$pw_t/; s/{{task_name}}/$task_name/; s/{{debug}}/$debug/; s/{{virtualenv_path}}/$ve_p/; s/{{cert}}/$ce_p/" ../.modules_config_files/mrjob.conf > mrjob.conf

$virtualenv_path venv
. venv/bin/activate
pip install -r $pwd/requirements.txt --cert $cert
