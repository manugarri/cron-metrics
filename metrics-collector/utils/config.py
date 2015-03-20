'''Get configuration from a file config.ini'''
import os
import base64
import json

from configure import Configuration


config_file = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'config.yml'))
config = Configuration.from_file(config_file).configure()

class Env(object):
  def __init__(self, adict):
    self.__dict__.update(adict)

def get_environment(section):
    return getattr(config, section)

def get_task_config(task_name):
    for task in config.tasks:
        if task['name'] == task_name:
            return Env(task['config'])
    raise Exception('NO CONFIG FOUND')

def get_io_config(io_name):
    for io_device in config.io:
        if io_device['name'] == io_name:
            return Env(io_device['config'])
    raise Exception('NO CONFIG FOUND')

def decode_var(var, jsonize=False):
    var = base64.b64decode(var)
    if jsonize:
        var = json.loads(var)
    return var

def get_task_specs(task_name):
    config = get_environment('plan')
    log_path = os.path.abspath(os.path.join(config.path,'logs'))
    for task in get_environment('tasks'):
        if task['name'] == task_name:
            task_specs = task['task_specs']
            task_specs['output'] = {
                    'stdout': os.path.join(log_path, task['name'] + '.out.log'),
                    'stderr': os.path.join(log_path, task['name'] + '.err.log')
                }
            if not task_specs.get('task'):
                task_specs['task'] = os.path.join('modules', task['name'],task['name'] + '_main.py')
            else:
                task_specs['task'] = os.path.join('modules', task['name'],task_specs['task'])
            return task_specs
    else:
        raise Exception('TASK NOT FOUND')
