# cron-metrics
Implementation of a monitoring system of interval cron tasks in Python. Useful for metrics tasks that need to run every n minutes.

Implementation requiring Plan and Crab. 
* [Plan](http://plan.readthedocs.org/) to create the crontab.
* [Crab](http://crab.readthedocs.org/) to provide a dashboard to monitor the tasks.



####Installation

1. `pip install -r requirements.txt`
2. Crab specific steps:

  2.1 Need to add a `crabd.ini` file in either `~/.crab` or `/etc/crab`. 
  
  2.2 recreate `crabdb.db`.
  
  2.3 Download jquery and Font Awesome and install them on the `res` folder (specified in `crabd.ini`).
  
  2.4 Port 8000 needs to be accesible on the machine (if using other port, need to change it in crabd.ini)
  
3. Modify `config.yml` (example included in `config.yml.example`).
  3.1. Change the `plan` user to the user in charge of running the crontab.
  3.2. Change the `path` in the `plan` section to the path where your `modules` folder will be located.

####Usage
1. To add a task:
* Create a folder on the `/modules` folder. That task should have one entrypoint (one file run as `__main__`).
 (preferably named `module_main.py`.
* Add that task on the `task` section of `config.yml`.
  * the `name` of the task should be the name of the folder you created for it.
  * task_specs should include the [plan](http://plan.readthedocs.org/job_definition.html) dsl for setting up a cron.
  
* Use `python start_cron.py write` to create the crontab with all the tasks, and start the crab dashboard.
You can go to `localhost:8000` to see it in action.

* Use `python start_cron.py clear` to empy the crontab, clean the `/logs` folder and kill `crabd`
