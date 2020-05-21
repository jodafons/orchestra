from orchestra import Schedule, Pilot, LCGRule
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *


db            = OrchestraDB(cluster=Cluster.LPS, url="postgres://postgres:postgres@localhost:5432/lps_dev")

#name='mverissi'
#name='plisboa'
#name='pedrosergiot'
name='gabriel-milan'

#queue='nvidia'
queue='cpu_small'

user = db.getUser(name)
for task in user.getAllTasks():
  task.queueName=queue
  for job in task.getAllJobs():
    job.queueName=queue

db.commit()

