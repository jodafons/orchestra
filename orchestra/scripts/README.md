
# LPS Cluster scripts:

## LPS cluster envs:

The script `setup_envs.sh` hold all configuration (postgres and rancher credentials) for the Zeus/LPS cluster machine;


## Run the cluster 

Copy `run_cluster.sh` to `/etc/init.d` into the zeus machine (cluster front-end). The script will start all cluster services after the boot.


## Reset database

This script `run_reset_database.py` will reset all task/jobs status to `registered` and swith all jobs to no `gpu` mode.

## Create database by hand

This script `run_create_database.py` will initalize the database into the postgres server.

## Main services:

- Maestro api: Allow the user to check and launch jobs from external machine (outside of LPS); Script `run_maestro.py`;
- Orchestra: The cluster service (`run_orchestra.py`);

