#!/usr/bin/env python

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
import argparse
import sys,os
import hashlib

mainLogger = Logger.getModuleLogger("orchestra_delete")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()



parser.add_argument('-u','--username', action='store', dest='username', required=True,
                    help = "The username.")


parser.add_argument('--url', action='store', dest='url', required=True,
                    help = "The URL to the entry point database")


parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                    help = "The task name to be append into the db.")



if len(sys.argv)==1:
  mainLogger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()



# Connect to DB
from orchestra.db import OrchestraDB
from orchestra.db import Task, Job
db = OrchestraDB( args.url )

try:
  user = db.getUser( args.username )
except:
  mainLogger.fatal("The user name (%s) does not exist into the data base", args.username)


try:
  task = db.getTask( args.taskname )
except:
  mainLogger.fatal("The task name (%s) does not exist into the data base", args.taskname)


id = task.id
for job in task.getAllJobs():
  try:
    if job.getStatus() == Status.FAILED:
      job.setStatus(Status.ASSIGNED)
  except:
    mainLogger.fatal("Impossible to assigned this job for (%d) task", id)

db.commit()
db.close()














