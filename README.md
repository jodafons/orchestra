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

```bash
orchestra_create.py  \
        --et 0 --eta 0 \
    -c user.jodafons.job_config.ringer.v10.RingerNet.10sorts.1inits \
    -o output \
        -d user.jodafons.data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_V97_et0_eta0.npz \
        -t user.jodafons.my_task \
        --containerImage jodafons/gpu-base \
        --secondaryData="{'%REF':'user.jodafons.data17_13TeV.AllPeriods.sgn.probes_lhmedium_EGAM1.bkg.VProbes_EGAM7.GRL_v97_et0_eta0.ref.pic.gz'}" \
        --exec ". /setup_envs.sh && python3 /code/saphyra/Analysis/RingerNote_2018/tunings/v10/job_tuning.py -d %DATA -c %IN -r %REF -o %OUT -t $TASK -u jodafons" \
    --cluster LPS \
    --bypass  \
    --dry_run
```



### References:
- Docker;
- kubernetes;
- Rancher


