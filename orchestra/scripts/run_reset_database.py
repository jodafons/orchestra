from orchestra import Schedule, Pilot, LCGRule
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *


db            = OrchestraDB(cluster=Cluster.LPS)


for user in db.getAllUsers():
  for task in user.getAllTasks():
    if task.id==8:
      for job in task.getAllJobs():
        print('id = %d, status = %s'%(job.id,job.getStatus()) )

