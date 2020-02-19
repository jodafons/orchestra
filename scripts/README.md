


# Orchestra Hands-On (LPS Cluster Support):

This document will provide a simple tutorial to show the job concept implemented into the orchestra code, 
As example, this tutorial will use a docker container based on the tensorflow image (with GPU support) and some examples to explain all steps to create a configuration and data file, write the tuning job script, prepare the docker file and push to the public repository and finally how to launch this example into the LPS cluster.

## Prepare the Docker Container:

### Create the Job Tuning Script:

The tuning job script (in python language) is a script where will be executed all steps needed by the training and the fitting. The job script must receive three mandatory argument to works using the orchestra job schemma:

- A data file path where was stored your trainig data (Implemented as `-d` or `--dataFile`);
- A job configuration file path where was store all training parameters and other things needed by your tuning script (Implemented as `-c` or `--configFile`);
- The output path where will be store the job result (Implemented as `-o` or `--output`);

Eventually, you can include more parameters in your job but the orchestra will works at least with these three 
parameters (mandatory). The schemma behind the orchestra job is:

- Create a task with N jobs where N is the number of job configurations created (i.e: if you apply the cross validation method with 10 sorts and 10 inits this will produce 100 jobs configurations);
- Each slot (or job node) will be allocated to run a specific job configuration. (This will be consumed by the orchestra schedule);
- When a job is assigned with the finalized status, one file (i.e: my_output_sort_1_init_2.pic) will be saved into output file assigned fot this task. 


> **NOTE**: See this python [script](https://github.com/jodafons/saphyra/blob/master/analysis/RingerTuning_2020/tunings/Zee/v10/create_jobs.py) for reference



## Prepare the Docker Container:

You must have a [dockerhub](https://hub.docker.com/) account to run the follow commands below. This image example uses an OS based on Linux (Ubuntu) builded by the tensorflow group with GPU support. This [Dockerfile](https://github.com/jodafons/orchestra/tree/master/doc/tutorial/docker) is used in this example.

### How to build the image
Compilation process to build the image using.
```bash
# build the docker image
source buildthis.sh
```

### How to run as bash (Just for Validation, No Mandatory):
Use this command if you would like to enter inside of the container in bash mode .
```bash
# run the docker image
source runthis.sh
```

### How to push to your public repository:
You must login before push your image into the docker repository. This container must be public.
```bash
docker push jodafons/saphyra
```



## Submit Jobs into the Orchestra (LPS Cluster):

To run the steps below you will must have an account into the LPS Cluster front-end (zeus, `146.164.147.170` into the LPS network). If you don't have an account please contat the administrator.


### Setup Orchestra For Users (Mandatory):

```bash
# Setup orchestra for users
export PYTHONPATH=/opt/orchestra/build:$PYTHONPATH
export PATH=/opt/orchestra/scripts:$PATH
```

### Storage Organization:

The LPS Cluster uses a different storage (first machine on top) to store all cluster user account datas. This storage is only visiable into the private cluster network and can not be externalized. Into the Zeus machine this storage was mounted in `/mnt/cluster-volume/`. Each user will have an file into the storage path (i.e: `/mnt/cluster-volume/jodafons`).


> **Note**: The zeus ip address is `146.164.147.170`. (i.e: To connect just run `ssh jodafons@146.164.147.170` into the LPS `bastion` machine)


### Registry The Dataset Into the Orchestra:

Registry the data file into the orchestra database as `user.jodafons.my_data_file.pic`:

```bash
# you must add this prefix in your dataset name: user.username.(...)
maestro.py castor upload -d user.jodafons.my_data_file.pic -p my_data_file.pic
```

Registry all configurations files (in the directory) into the orchestra database as `user.jodafons.my_config_files`: 
```bash
# you must add this prefix in your dataset name: user.username.(...)
maestro.py castor upload -d user.jodafons.my_config_files -p my_config_files
```
Here, `-d` is the dataset name and `-p` is the path of the file or directory that will be used.


### Task Creation:


After organize your user directory into the storage with the data/configuration files into the `files` directory you will be able to create a task. The task name must follow the same rule defined in the dataset policy name.

```bash
maestro.py task create  \
    -c user.jodafons.my_configs_files \
    -d user.jodafons.my_data_file \
    -t user.jodafons.my_task_tutorial \
    --containerImage jodafons/saphyra \
    --exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT" \
    --bypass \
```

The `--exec` command contruction must follow some rules to work:

- The `%DATA` tag will be substitute by the data file path (storage) into the orchestra. (This tag is mandatory); 
- The `%IN` tag will be substitute by the configuration file (storage) path into the orchestra. (This tag is mandatory); 
- The `%OUT` tag will be substitute by the output file path (storage)into the orchestra. (This tag is mandatory); 

> **NOTE**: The `--bypass` will skip the 10 jobs tester. Do not use this command if you are not sure that your task will works on LPS Cluster.

> **NOTE**: The orchestra allow some custom commands like:
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
> **WARNING**: You must remove the task directory (`user.jodafons.my_first_task`) by hand to relaunch a task with same name.

### Retry Task:

```bash
maestor.py task -t user.jodafons.my_first_task
```

### Download Outputs

```bash
maestor.py castor download -t user.jodafons.my_first_task
```


### Delete a Dataset

```bash
maestor.py castor delete -d user.jodafons.my_configs_files
```
























