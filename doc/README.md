


# Orchestra Tutorial (LPS Cluster Support):

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


See this python [script](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/docker/job_tuning.py) for reference



## Prepare Data and Config Files:

Just run these python scripts to create the data and configurations files that will be used in this tutorial.

- See this data [creation](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/create_data.py) file for reference;
- See this [configuration](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/create_configs.py) file for reference;


## Prepare the Docker Container:

You must have a [dockerhub](https://hub.docker.com/) account to run the follow commands below. This image example uses an OS based on Linux (Ubuntu) builded by the tensorflow group with GPU support. This [Dockerfile](https://github.com/jodafons/orchestra/tree/master/doc/tutorial/docker) is used in this example.

### How to build the image
Compilation process to build the image using.
```bash
# build the docker image
source buildthis.sh
```

### How to run as bash (Just for Validation, No Mandatory)
Use this command if you would like to enter inside of the container in bash mode .
```bash
# run the docker image
source runthis.sh
```

### How to push to your public repository
You must login before push your image into the docker repository. This container must be public.
```bash
docker push jodafons/orchestra_tutorial
```



## Prepare the Workspace (LPS Cluster):

To run the steps below you will must have an account into the LPS Cluster front-end (zeus, 146.164.147.170 into the LPS network). If you don't have an account please contat the administrator.

### Storage Organization:

The LPS Cluster uses a different storage (first machine on top) to store all cluster user account datas. This storage is only visiable into the private cluster network and can not be externalized. Into the Zeus machine this storage was mounted in `/mnt/cluster-volume/`. Each user will have an file into the storage path (i.e: /mnt/cluster-volume/jodafons).

The orchestra policy follow some rules
- The data file must be store into `/mnt/cluster-volume/jodafons/files/my_data_file.pic`. Where files is the directory that will be used to hold all datasets;
- The configuration data must be store into `/mnt/cluster-volume/jodafons/files/my_config_files/` where `my_config_files` is an directory the store all configuration files;

To build this workspace you just upload all created files (using scp) into the zeus machine and than organize your storage file as presented.

Note: The zeus ip address is 146.164.147.170.


## Registry The Dataset Into the Orchestra:

Into the `/mnt/cluster-volume/jodafons/files`, for example, you must run the follow commands to registry all uploaded filesas datasets into the orchestra database. An dataset is a name who point to a file or an directory path (key, value) into the storage. The dataset name must be start as user.username.* always (i.e. user.jodafons.my_config_files).

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


## Create the Task:

```bash
orchestra_create.py  \
    -c user.$USER.my_configs_example \
    -o output \
    -d user.$USER.my_data_example \
    -t user.$USER.my_task_example \ # The place where will save all output files
    --containerImage $USER/my_orchestra_tutorial \ # the container into the docker repository
    --exec "python3 /job_tuning.py -d %DATA -c %IN -o %OUT" \
    --cluster LPS \
    --bypass \



```





























