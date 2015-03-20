# -*- coding: utf-8 -*-

# Use this file to easily define all of your cron jobs.
#
# It's helpful to understand cron before proceeding.
# http://en.wikipedia.org/wiki/Cron
#
# Learn more: http://github.com/fengsp/plan
import sys
import os
import shutil
import subprocess

from plan import Plan

from utils.config import get_environment, get_task_specs


config = get_environment('plan')
config['environment'] = dict(config['environment'])
cron = Plan(**config)
log_path = os.path.abspath(os.path.join(config.path,'logs'))
modules_path = os.path.abspath(os.path.join(config.path,'modules'))

for task in get_environment('tasks'):
    task_specs = get_task_specs(task['name'])
    cron.script(**task_specs)

if __name__ == "__main__":
    command = sys.argv[1]
    if command == 'write':
        if not os.path.exists('logs'):
            os.makedirs('logs')
        c = subprocess.Popen("crabd-check", stdout=subprocess.PIPE, shell=True)
        print(c)
    elif command == 'clear':
        if os.path.exists('logs'):
            shutil.rmtree('logs', ignore_errors=True)
        c = subprocess.Popen("killall crabd", stdout=subprocess.PIPE, shell=True)
        print(c)
    cron.run(command) #write, check, update or clear the crontab
