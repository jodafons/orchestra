#!/usr/bin/env python3

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
import argparse
import sys,os
import hashlib

logger = Logger.getModuleLogger("orchestra_delete")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()



parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                    help = "The task name to be append into the db.")



if len(sys.argv)==1:
  logger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()



# Connect to DB
from orchestra.db import OrchestraDB
from orchestra.db import Task, Job
db = OrchestraDB( args.url )


# check task policy
taskname = args.task
taskname = taskname.split('.')
if taskname[0] == 'user':
  logger.fatal('The task name must starts with: user.%USER.taskname.')
username = taskname[1]
if username in db.getAllUsers():
  logger.fatal('The username does not exist into the database. Please, report this to the db manager...')




try:
  user = db.getUser( username )
except:
  logger.fatal("The user name (%s) does not exist into the data base", username)


try:
  task = db.getTask( args.taskname )
except:
  logger.fatal("The task name (%s) does not exist into the data base", args.taskname)


id = task.id
for job in task.getAllJobs():
  try:
    if job.getStatus() == Status.FAILED:
      job.setStatus(Status.ASSIGNED)
  except:
    logger.fatal("Impossible to assigned this job for (%d) task", id)

db.commit()
db.close()














