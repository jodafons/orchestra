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


### Task Creation:

```bash
maestro.py task create \
    -c user.jodafons.my_configs_files \
    -d user.jodafons.my_data_file \
    -t user.jodafons.my_task_tutorial \
    --containerImage $USER/my_orchestra_tutorial \
    --exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT" \
    --bypass \
    --queue gpu
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
- `gpu`: For GPU only;
- `cpu`: Dedicated jobs with higher cpu consume. Usually one node is allocated to run the job.


### Print All Tasks:

```bash
maestro.py task list
```


### Delete Task:

This command will remove the task from the orchestra database.

```bash
maestro.py task delete --id 0
```

### Retry Task:

```bash
maestro.py task retry --id 0
```


```mermaid
  stateDiagram-v2
      REGISTERED-->TESTING
      TESTING-->RUNNING
      RUNNING-->FINALIZED
      RUNNING-->DONE
      DONE-->REGISTERED
      RUNNING-->KILL
      KILL-->KILLED
      KILLED-->REGISTERED
      TESTING-->BROKEN
      BROKEN-->REGISTERED
```
