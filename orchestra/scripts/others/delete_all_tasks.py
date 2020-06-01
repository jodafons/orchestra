from orchestra import TaskParser
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *


db = OrchestraDB(cluster=Cluster.LPS, url="postgres://postgres:postgres@localhost:5432/lps_prod")

for user in db.getAllUsers():

  for task in user.getAllTasks():

    TaskParser(db).delete( task.getTaskName(), force=True )




