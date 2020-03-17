from orchestra import Schedule, Pilot, LCGRule
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *


db            = OrchestraDB(cluster=Cluster.LPS)


for user in db.getAllUsers():
  for task in user.getAllTasks():
    task.setStatus('registered')
    task.isGPU=False
    for job in task.getAllJobs():
      job.isGPU = False
      job.setStatus('registered')

db.commit()
