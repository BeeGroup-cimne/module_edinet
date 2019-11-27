from celery_backend import app
import os
import subprocess
import json

task_exec_file = "{{task_exec_file}}"
task_name = "{{task_name}}"
queue = "{{queue}}"

@app.task(name=task_name, queue=queue)
def {{task_name}}(params):
    path = os.path.dirname(__file__)
    venv = "{}/venv/bin/python".format(path)
    file_exec = "{}/{}".format(path, task_exec_file)
    x = subprocess.call([venv, file_exec, json.dumps(params)])

