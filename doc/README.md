


# Orchestra tutorial:

Here, we will provide a simple tutorial to show the job concept implemented in the orchestra, the docker
file contruction using a simple tensorflow image, the job preparation into the server and the job laucher
scripts assistent.

### Create my python job:

The python is a script where will be executed all training steps. The job script must receive by argument:

- A data file path where is store your trainig data;
- A job configuration file path where are store all training parameters and other things;
- The output name where will be store the job result;

Evetually, you can include more parameters in your job but the orchestra will only works with these three 
parameters. The logical behind this is:

- Each slot (or job node) will be allocated to run a specifi job configuration (i.e, if you must apply the cross validation method with 10 sorts and 10 inits, you can produce 100 job configurations and your stack will be conposed by these 100 jobs and the orchestra will consume this stack);
- For each slot, we will point the data file path. Here we have only one dataset and all nodes will look to this file;
- When the job is finalized, one output file with an specifical name (for the current slot, i.e: my_output_sort_1_init_2.pic) will be store at the user file in the rancher storage.

Its recommended that your python script has at least these argumentes to receive these parameters by the orchestra:


```python
import argparse
import sys,os


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-c','--configFile', action='store',
        dest='configFile', required = True,
            help = "The job config file that will be used to configure the job (sort and init).")

parser.add_argument('-o','--outputFile', action='store',
        dest='outputFile', required = False, default = None,
            help = "The output tuning name.")

parser.add_argument('-d','--dataFile', action='store',
        dest='dataFile', required = False, default = None,
            help = "The data/target file used to train the model.")


if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()
```

See this python [script](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/docker/job_tuning.py) for reference



### Prepare my data and config files:

These files will be used in the next step.

- See this data [creation](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/create_data.py) file for reference;
- See this [configuration](https://github.com/jodafons/orchestra/blob/master/doc/tutorial/create_configs.py) file for reference;



### Prepare my docker container:

You must have an docker hub account and your image must be public. Follow the procedures below to build and push your image
to your public docker repository. This image uses the Ubuntu (tensorflow) as base and included the follow packages
current installed:

- keras and tensorflow
- scipy and numpy

A example of docker build and some usefull scripts can be found [here](https://github.com/jodafons/orchestra/tree/master/doc/tutorial/docker).


### How to build the image

```bash
# build the docker image
source buildthis.sh
```

### How to run as bash

```bash
# run the docker image
source runthis.sh
```

### How to push to your public repository

You must login before push your image into the docker repository.
```bash
# push
docker push ${USER}/my_orchestra_tutorial
```

The idea to use a docker container here is:

- The user has the total control of all dependences;
- easy to implement and share (with containers) with the group;
- The adminsitrator is not responsible for this.




### Prepare your workspace inside of the server :

At this point we have all files and the docker container into your docker hub repository (must be public). Now, lets connect to 
upload all files into the lab and connect to the cluster front-end (you must have access).

```bash
# run the docker image
scp -r *.pic $USER@bastion.lps.ufrj.br:~/
```
connect to the LPS server by ssh and upload your files in your home to 146.164.147.170 (cluster) machine using scp.

```bash
# Into the lps bastion
scp -r *.pic $USER@146.164.147.170:~/
```

and connect by ssh into 146.164.147.170 machine. 

Into the /mnt/cluster-volume/$USER file you must create an file folder where will be used to hold all file dirs.

- /mnt/cluster-volume/$USER/files/my_data/ is the file where you will put your data file created locally by the create_data.py script;
- /mnt/cluster-volume/$USER/files/my_configs/ is the file where you will put your config file created locally by the create_config.py script;


### Registry your data into the orchestra:

In this step you will register all files into the file data manager used by orchestra. Use the follow commands to registy all files.


Registry the data file into the orchestra database:

```bash
# you must add this prefix in your dataset name: user.username.(...)
orchestra_registry.py -d user.$USER.my_data_example -p my_data/ --cluster LPS
```

Registry all config files into the orchestra database:

```bash
# you must add this prefix in your dataset name: user.username.(...)
orchestra_registry.py -d user.$USER.my_configs_example -p my_configs/ --cluster LPS
```

### Launch your job into the orchestra:

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





























