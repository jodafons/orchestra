# Orchestra  

This package is used to manager tasks and jobs inside the cluster. This tools uses the kubernetes as backend
to create and remove jobs (or pods) inside of the cluster. The schedule is responsible to check the task
status, calculate the job priority and append or remove jobs from the queue. After calculate and get the 
queue ordered by job priority, the pilot send the jobs to the slot. If the slot is available, then one
job will be move to inside of slot and the kubernetes will take care to launch and manager all cluster
resources to alocate this.



### Rules

- The job calculation is happing every 5 minutes;
- When a task is registered into the database, the schedule will take one job and send to the cluster.
If this job return success than the task will be swith to running state and all jobs will be assigned;

### Know Bugs:

- There is a bug into the priority calculation (LCGRule or schedule calc.);


### Requirements (python packages):
- python 3;
- sqlalchemy;
- kubernetes;
- benedict;
- numpy
- Gaugi

### Setup:

```bash
# in your root dir
source setup_modules.sh
source setup_modules.sh --head
source setup.sh
```

# Usage

### Registry The Dataset Into the Orchestra:

Into the `/mnt/cluster-volume/jodafons/files`, for example, you must run the follow commands to registry all uploaded filesas datasets into the orchestra database. A `dataset` is a name who point to a file or an directory path (key, value) into the storage. The dataset name must start with `user.username.*` always (i.e. `user.jodafons.my_config_files`).

Registry the data file into the orchestra database as `user.jodafons.my_data_file.pic`:

```bash
# you must add this prefix in your dataset name: user.username.(...)
orchestra_registry.py -d user.jodafons.my_data_file.pic -p my_data_file.pic --cluster LPS
```

Registry all configurations files (in the directory) into the orchestra database as `user.jodafons.my_config_files`: 
```bash
# you must add this prefix in your dataset name: user.username.(...)
orchestra_registry.py -d user.jodafons.my_config_files -p my_config_files/ --cluster LPS
```
Here, `-d` is the dataset name and `-p` is the path of the file or directory that will be used.


### Task Creation:


After organize your user directory into the storage with the data/configuration files into the `files` directory you will be able to create a task. This command must run inside of your user directory (here, into the `/mnt/cluster-volume/jodafons/`).
The task name must follow the same rule defined in the dataset policy name.

```bash
maestro.py task create \
    -c user.jodafons.my_configs_files \
    -d user.jodafons.my_data_file \
    -t user.jodafons.my_task_tutorial \
    --containerImage $USER/my_orchestra_tutorial \
    --exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT" \
    --bypass \
```

The `--exec` command contruction must follow some rules to work:

- The `%DATA` tag will be substitute by the data file path (storage) into the orchestra. (This tag is mandatory); 
- The `%IN` tag will be substitute by the configuration file (storage) path into the orchestra. (This tag is mandatory); 
- The `%OUT` tag will be substitute by the output file path (storage)into the orchestra. (This tag is mandatory); 

**NOTE**: The `--bypass` will skip the 10 jobs tester. Do not use this command if you are not sure that your task will works on LPS Cluster.

**NOTE**: The orchestra allow some custom commands like:
- `--exec " . /setup_envs.sh && python3 /job_tuning.py -d %DATA -c %IN -o %OUT"`, run the `setup_envs.sh` script if you need to do some other things before start;
- `--exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT && python3 /after_job.py"`, run the `after_job.py` script if you need to do some other things in the end;



### Print All Tasks:

```bash
maestro.py task list -u jodafons
```


### Delete Task:

This command will remove the task from the orchestra database.

```bash
maestro.py task delete -t user.jodafons.my_first_task
```

### Retry Task:

```bash
mestro.py task retry -t user.jodafons.my_first_task
```



### References:
- Docker;
- kubernetes;
- Rancher


