

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
import argparse
import sys,os
import hashlib

mainLogger = Logger.getModuleLogger("job")
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
from ringerdb import RingerDB
from ringerdb import Task, Job, Model
db = RingerDB( args.url )

try:
  user = db.getUser( args.username )
except:
  mainLogger.fatal("The user name (%s) does not exist into the data base", args.username)


try:
  task = db.getTask( args.taskname )
except:
  mainLogger.fatal("The task name (%s) does not exist into the data base", args.taskname)


id = task.id


try:
  db.session().query(Model).filter(Model.taskId==id).delete()
except Exception as e:
  mainLogger.fatal("Impossible to remove Model lines from (%d) task", id)


try:
  db.session().query(Job).filter(Job.taskId==id).delete()
except Exception as e:
  mainLogger.fatal("Impossible to remove Job lines from (%d) task", id)


try:
  db.session().query(Task).filter(Task.id==id).delete()
except Exception as e:
  mainLogger.fatal("Impossible to remove Task lines from (%d) task", id)

db.commit()
db.close()














