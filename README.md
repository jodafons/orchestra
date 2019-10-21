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


### Requirements (python packages):
- python 3;
- sqlalchemy;
- kubernetes;
- benedict;
- numpy

### Setup:

```bash
# in your root dir
source setup_modules.sh
source setup_modules.sh --head
source setup.sh
```

### Usage


### References:
- Docker;
- kubernetes;
- Rancher


