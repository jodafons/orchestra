#!/usr/bin/env python3

import argparse
from Gaugi.messenger import LoggingLevel, Logger
from Gaugi import load
import glob
import numpy as np
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



if len(sys.argv)==1:
  logger.info(parser.print_help())
  sys.exit(1)

args = parser.parse_args()




if not args.dry_run:
  db = OrchestraDB()

# check task policy
taskname = args.task
taskname = taskname.split('.')
if taskname[0] != 'user':
  logger.fatal('The task name must starts with: user.%USER.taskname.')
username = taskname[1]
if username in db.getAllUsers():
  logger.fatal('The username does not exist into the database. Please, report this to the db manager...')



# Create the base path to point to the volume
basepath = '/volume/'+args.task
dataFile=basepath+'/'+args.dataFile

# Create the output path
configFiles = glob.glob(args.configFile+'/*')
logger.verbose("We will launch %d jobs into the cluster.",len(configFiles) )
print("We will launch %d jobs into the cluster."%len(configFiles))

# Create the output file path
outputFile = basepath+'/'+args.outputFile

# Check the exec command
execCommand = args.execCommand


if not '%DATA' in execCommand:
  logger.fatal( "The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")
if not '%IN' in execCommand:
  logger.fatal( "The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
if not '%OUT' in execCommand:
  logger.fatal( "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")


# Check for secondary data file
secondaryData = eval(args.secondaryData)
for key in secondaryData:
  if not key in execCommand:
    logger.fatal("The exec command must include %s into the string. This will substitute to %s when start",key, secondaryData[key])
  #secondaryData[key] = basepath+'/'+ glob.glob(secondaryData[key]+'/*')[0]
  secondaryData[key] = basepath+'/'+ secondaryData[key]







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


for idx, configFile in enumerate(configFiles):

  command = execCommand
  command = command.replace( '%DATA' , dataFile  )
  command = command.replace( '%IN'   , basepath+'/'+configFile)
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





