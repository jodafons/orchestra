
from orchestra import Schedule, Pilot, LCGRule
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *



# Create all services
schedule      = Schedule( "Schedule", LCGRule(),  max_update_time = 0.5*MINUTE )
db            = OrchestraDB(cluster=Cluster.LPS)
#orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )
orchestrator  = Orchestrator( "/home/rancher/devs/orchestra/orchestra/data/job_template.yaml",  "/home/rancher/devs/orchestra/orchestra/data/lps_cluster.yaml" )

# create the pilot
pilot = Pilot(db, schedule, orchestrator,
              bypass_gpu_rule=True,
              run_slots = True,
              update_task_boards = True,
              timeout = None, # run forever
              queue_name = Queue.LPS, cluster=Cluster.LPS )


# start!
pilot.initialize()
pilot.execute()
pilot.finalize()


