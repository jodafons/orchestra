#!/usr/bin/env python3

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
import os
import argparse
import sys,os
import hashlib
from orchestra.db import *


logger = Logger.getModuleLogger("orchestra_create")
parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-c','--configFile', action='store',
                    dest='configFile', required = True,
                    help = "The job config file that will be used to configure the job (sort and init).")


parser.add_argument('-o','--outputFile', action='store',
                    dest='outputFile', required = True,
                    help = "The output tuning name.")


parser.add_argument('-d','--dataFile', action='store',
                    dest='dataFile', required = True,
                    help = "The data/target file used to train the model.")


parser.add_argument('--exec', action='store', dest='execCommand', required=True,
                    help = "The exec command")


parser.add_argument('--containerImage', action='store', dest='containerImage', required=True,
                    help = "The container image point to docker hub. The container must be public.")


parser.add_argument('-t','--task', action='store', dest='task', required=True,
                    help = "The task name to be append into the db.")


parser.add_argument('--sd','--secondaryData', action='store', dest='secondaryData', required=False,  default="{}",
                    help = "The secondary datasets to be append in the --exec command. This should be:" +
                    "--secondaryData='{'REF':'path/to/my/extra/data',...}'")


parser.add_argument('--gpu', action='store_true', dest='gpu', required=False, default=False,
                    help = "Send these jobs to GPU slots")


parser.add_argument('--et', action='store', dest='et', required=False,default=None,
                    help = "The ET region (ringer staff)")


parser.add_argument('--eta', action='store', dest='eta', required=False,default=None,
                    help = "The ETA region (ringer staff)")


parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                    help = "Use this as debugger.")


parser.add_argument('--bypass', action='store_true', dest='bypass_test_job', required=False, default=False,
                    help = "Bypass the job test.")

parser.add_argument('--cluster', action='store', dest='cluster', required=False, default='LPS',
                    help = "The name of your cluster (LPS/CERN/SDUMONT/LOBOC)")

parser.add_argument('--storagePath', action='store', dest='storagePath', required=False, default='/mnt/cluster-volume',
                    help = "The path to the storage in the cluster.")



if len(sys.argv)==1:
  logger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()


# create the database manager
db = OrchestraDB()


# check task policy (user.username)
taskname = args.task
taskname = taskname.split('.')
if taskname[0] != 'user':
  logger.fatal('The task name must starts with: user.%USER.taskname.')


# check task policy (username must exist into the database)
username = taskname[1]
if username in db.getAllUsers():
  logger.fatal('The username does not exist into the database. Please, report this to the db manager...')


# Check if the task exist into the databse
print(db.getUser(username).getTask(args.task))
if db.getUser(username).getTask(args.task) is not None:
  logger.fatal("The task exist into the database. Abort.")


# check data (file) is in database
if db.getDataset(username, args.dataFile) is None:
  logger.fatal("The file (%s) does not exist into the database. Should be registry first.", args.dataFile)


# check configFile (file) is in database
if db.getDataset(username, args.configFile) is None:
  logger.fatal("The config file (%s) does not exist into the database. Should be registry first.", args.configFile)


# Get the secondary data as dict
secondaryData = eval(args.secondaryData)


# check secondary data paths exist is in database
for key in secondaryData.keys():
  if db.getDataset(username, secondaryData[key]) is None:
    logger.fatal("The secondary data file (%s) does not exist into the database. Should be registry first.", secondaryData[key])


# check if task exist into the storage
storagePath = args.storagePath+'/'+username+'/'+args.task
if os.path.exists(storagePath):
  logger.fatal("The task dir exsit into the storage. Contact the administrator.")
else:
  # create the task dir
  logger.info("Create the task dir in %s", storagePath)
  os.system( 'mkdir %s '%(storagePath) )


# create the data (file) link in the storage path
dataLink = db.getDataset(username, args.dataFile).getAllFiles()[0].getPath()
logger.info("Create data link (%s)", dataLink)
os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/', args.dataFile) )


# create the secondary data (file) link in the storage path
for key in secondaryData.keys():
  dataLink = db.getDataset(username, secondaryData[key]).getAllFiles()[0].getPath()
  os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/',secondaryData[key]) )
  logger.info("Create secondary data link (%s)", dataLink)


# create the output file
logger.info("Create the output file")
os.system('mkdir %s/%s'%(storagePath,args.outputFile))


# create the config file link
dataLink = db.getDataset(username, args.configFile).getAllFiles()[0].getPath()
dataLink = str('/').join(dataLink.split('/')[0:-1]) # get only the dir
os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/',args.configFile) )







# Create the base path (docker volume)
basepath = '/volume/'+args.task

# Create the pseudo data path (docker volume)
dataFile=basepath+'/'+args.dataFile

# Create the pseudo output file path (docker volume)
outputFile = basepath+'/'+args.outputFile

# Check the exec command (docker volume)
execCommand = args.execCommand

# check exec command policy
if not '%DATA' in execCommand:
  logger.fatal( "The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")
if not '%IN' in execCommand:
  logger.fatal( "The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
if not '%OUT' in execCommand:
  logger.fatal( "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")

# parser the secondary data in the exec command
for key in secondaryData.keys():
  if not key in execCommand:
    logger.fatal("The exec command must include %s into the string. This will substitute to %s when start",key, secondaryData[key])
  secondaryData[key] = basepath+'/'+ secondaryData[key]


# create the task into the database
if not args.dry_run:
  try:
    user = db.getUser( username )
    task = db.createTask( user, args.task, args.configFile, args.dataFile, args.outputFile,
                        args.containerImage, args.cluster,
                        secondaryDataPath=args.secondaryData,
                        templateExecArgs=args.execCommand,
                        etBinIdx=args.et,
                        etaBinIdx=args.eta,
                        isGPU=args.gpu,
                        )
    task.setStatus('hold')
  except Exception as e:
    logger.fatal(e)

# create all jobs. Must be loop over all config files
configFiles = db.getDataset(username, args.configFile).getAllFiles()


for idx, file in enumerate(configFiles):

  # create the pseudo path (docker volume)
  configFile = basepath+'/'+args.configFile+'/'+file.getPath().split('/')[-1]

  command = execCommand
  command = command.replace( '%DATA' , dataFile  )
  command = command.replace( '%IN'   , configFile)
  command = command.replace( '%OUT'  , outputFile)

  for key in secondaryData:
    command = command.replace( key  , secondaryData[key])
  print("\n"+command+"\n")

  if not args.dry_run:
    job = db.createJob( task, configFile, idx, execArgs=command, isGPU=args.gpu, priority=-1 )
    job.setStatus('assigned' if args.bypass_test_job else 'registered')


if not args.dry_run:
  task.setStatus( 'running' if args.bypass_test_job else 'registered'  )
  db.commit()
  db.close()





