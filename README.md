# Orchestra  


## Setup your own cluster:

If you do not run these steps below before, please follow this section. Before we starts you must set your email to send automatic messanges using external APIs. This messages will be very important to monitoring your job status.

**NOTE**: All steps here must be run into the LPS cluster.

After that, let's create your orchestra configuration file. First, go to your home dir and create the file `.orchestra.json` with these attributes:

```
{
 "postgres":""postgres://db_username:db_password@db.lps.ufrj.br:5432/your_db_name"
 "email":"your_email@lps.ufrj.br"
 "password":"your_email_password"
 "job_complete_file_name":".complete"
}
```
and save it.

### Download the container:

Donwload the image:
```
singularity pull docker://jodafons/orchestra:base
```
Run it!
```
singularity run orchestra_base.sif
```

and setup all orchstra envs inside of the container:
```
source /setup_envs.sh
```

### Create your database:

Setup the database:
```
maestro.py user init
```

and create your user:
```
maestro.py user create -n username -e username@lps.ufrj.br
```

### Setup all available nodes:

Let's create one node with name `caloba21` with 2 `gpus` slots and none `cpu` slots.
```
maestro.py node create -ec 0 -mc 0 -eg 2 -mc 2 -n caloba21
```

Now, let's create one node with name `caloba51` with 40 `cpus` slots and none `gpus` slots.
```
maestro.py node create -ec 40 -mc 40 -eg 0 -mc 0 -n caloba51
```


## Upload your files:

In this section we will registry some files into the database manager (`castor`). First, let's create some files into your home dir to keep everything organized.

```
cd ~
mkdir tasks
mkdir data
```








### Setup:

Download the orchestra repository into your directory and setup unsing the follow commands:

```bash
source setup_modules.sh
source setup_modules.sh --head
source setup.sh
```
 The `setup.sh` will set the orchestra to the python path and call the `setup_envs.sh` scripts. If you don't have rights to download the `partiture` package (maybe you are not a LPS admin) you will need to setup some envs by hand just like this:
 
 ```
 export CLUSTER_POSTGRES_URL="postgres://suaurl@postgres:password"
 export CLUSTER_API_PORT="port_to_external_all_maestro_services"
 export CLUSTER_RANCHER_CREDENTIALS="path_to_rancher_credentials.yaml"
 ```


# Usage

It's possible to use the orchestra with maestro api on your local machine. To do this you must have an account (LPS).

- To install the API on your local machine: pip3 install lps_maestro
- The documentation can be found here: https://maestro-lps.readthedocs.io/en/latest/


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
    --queue nvidia
```

The `--exec` command contruction must follow some rules to work:

- The `%DATA` tag will be substitute by the data file path (storage) into the orchestra. (This tag is mandatory); 
- The `%IN` tag will be substitute by the configuration file (storage) path into the orchestra. (This tag is mandatory); 
- The `%OUT` tag will be substitute by the output file path (storage)into the orchestra. (This tag is mandatory); 

**NOTE**: The `--bypass` will skip the 10 jobs tester. Do not use this command if you are not sure that your task will works on LPS Cluster.

**NOTE**: The orchestra allow some custom commands like:
- `--exec " . /setup_envs.sh && python3 /job_tuning.py -d %DATA -c %IN -o %OUT"`, run the `setup_envs.sh` script if you need to do some other things before start;
- `--exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT && python3 /after_job.py"`, run the `after_job.py` script if you need to do some other things in the end;

**NOTE**: The cluster support multiple queues. The queue name can be:
- `nvidia`: For GPU only;
- `cpu_small`: Jobs with low CPU cost. One node can run more the one cpu slot;
- `cpu_large`: Dedicated jobs with higher cpu consume. Usually one node is allocated to run the job.


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
maestro.py task retry -t user.jodafons.my_first_task
```



### References:
- Docker;
- kubernetes;
- Rancher


