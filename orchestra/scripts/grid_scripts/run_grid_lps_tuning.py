

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

parser.add_argument('-t','--taskName', action='store', dest='taskname', required=True,
                    help = "The task name to be append into the db.")


parser.add_argument('--secondaryData', action='store', dest='secondaryData', required=False,
                    default="{}", help = "The task name to be append into the db.")

parser.add_argument('-u','--username', action='store', dest='username', required=True, 
                    help = "The username.")

parser.add_argument('--url', action='store', dest='url', required=True,
                    help = "the url to the entry point database")

parser.add_argument('--gpu', action='store', dest='gpu', required=False, default=False, 
                    help = "Send to GPU slots")

parser.add_argument('--containerImage', action='store', dest='containerImage', required=True,
                    help = "The container image point to docker hub. The container must be public.")


 

if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()



# Connect to DB
from ringerdb import RingerDB
from ringerdb import Task, Job

db = RingerDB( args.username, args.url, dry_run=True )

# Create the base path to point to the volume
basepath = '/volume/'+args.username+'/'+args.taskname

# Create the data input path
dataFile = glob.glob(args.dataFile+'/*')
if len(dataFile) > 1:
  mainLogger.fatal("The data container (file) must have only one file.")

dataFile=basepath+'/'+dataFile[0]

# Create the output path
configFiles = glob.glob(args.configFile+'/*')
mainLogger.info("We will launch %d jobs into the cluster.",len(configFiles) )

# Create the output file path
outputFile = basepath+'/'+args.outputFile

# This should be assigned as LPS name to works
cluster = "LPS" #/CERN


# Check the exec command
execCommand = args.execCommand

if not '%DATA' in execCommand:
  mainLogger.fatal( "The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")
if not '%IN' in execCommand:
  mainLogger.fatal( "The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
if not '%OUT' in execCommand:
  mainLogger.fatal( "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")


# Check for secondary data file
secondaryData = eval(args.secondaryData)
for key in secondaryData:
  if not key in execCommand:
    mainLogger.fatal("The exec command must include %s into the string. This will substitute to %s when start",key, secondaryData[key])
  secondaryData[key] = basepath+'/'+ glob.glob(secondaryData[key]+'/*')[0]





# task, config, inut, output, cluster
task = db.createTask( args.taskname, args.configFile, args.dataFile, args.outputFile,
                      args.containerImage, cluster,
                      # Extra
                      secondaryDataPath=args.secondaryData,
                      templateExecArgs=args.execCommand,
                      etBinIdx=None,
                      etaBinIdx=None,
                      isGPU=args.gpu,
                      )

# Lets parser all varaibles and send to the db.


for idx, configFile in enumerate(configFiles):
  #print("Adding job (%d) with config (%s)" %(idx,f))
  # Create the exec args just for good practicy. This is not used in LCG/CERN grid
  command = execCommand
  command = command.replace( '%DATA' , dataFile  )
  command = command.replace( '%IN'   , basepath+'/'+configFile)
  
  
  hash_object = hashlib.sha1(str.encode(configFile))
  output = hash_object.hexdigest()
  output = outputFile+'/user.'+args.username+'.'+ output+'/'
  command = ('mkdir %s && %s') % (output,command)

  command = command.replace( '%OUT'  , output)
  # Fill all secondary datas
  for key in secondaryData:
    command = command.replace( key  , secondaryData[key])

  print(command)
  job = db.createJob( task, configFile, idx, execArgs=command, isGPU=args.gpu, priority=-1 )

#db.commit()
db.close()





