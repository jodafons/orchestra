#!/usr/bin/env python3

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import csvStr2List, expandFolders
from Gaugi import load

# Connect to DB
from orchestra.constants import CLUSTER_VOLUME
from orchestra.db import OrchestraDB
from orchestra.db import Dataset,File
from orchestra import Status, Cluster


# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib
import argparse



class DatasetParser( Logger ):

  def __init__(self, db, subparser=None):

    Logger.__init__(self)
    self.__db = db
    if subparser:
      # Upload dataset using the dataset CLI
      upload_parser = argparse.ArgumentParser(description = 'Dataset upload command lines.', add_help = False)
      upload_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                  help = "The dataset name used to registry into the database. (e.g: user.jodafons...)")
      upload_parser.add_argument('-p','--path', action='store', dest='path', required=True,
                                  help = "The path to the dataset")
      # Download dataset using the dataset CLI
      download_parser = argparse.ArgumentParser(description = 'Dataset donwload command lines.', add_help = False)
      download_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                   help = "The dataset name to be downloaded")
      # Delete dataset using the dataset CLI
      delete_parser = argparse.ArgumentParser(description = 'Dataset Delete command lines.', add_help = False)
      delete_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                   help = "The dataset name to be removed")
      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'Dataset List command lines.', add_help = False)
      list_parser.add_argument('-u', '--user', action='store', dest='username', required=True,
                                   help = "List all datasets for a selected user.")

      # Datasets
      subparser.add_parser('upload', parents=[upload_parser])
      subparser.add_parser('download', parents=[download_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])



  def compile( self, args ):
    # Dataset CLI
    if args.parent == 'upload':
      self.upload(args.datasetname, args.path)
    elif args.parent == 'download':
      self.download(args.datasetname)
    elif args.parent == 'delete':
      self.delete(args.datasetname)
    elif args.parent == 'list':
      self.list(args.username)




  #
  # Print dataset
  #
  def print( self, datasetname ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The dataset name must starts with: user.%USER.taskname.')
    username = datasetname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')

    db = self.__db.getDataset( username, datasetname )
    if not db:
      MSG_FATAL( self, "The dataset exist into the database")

    # Loop over all files inside of this dataset
    print ( "| %s | %s | %d |".format(username, datasetname, len(ds.files)) )



  #
  # List datasets
  #
  def list( self, username ):

    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')

    # Loop over all datasets inside of the username
    for ds in self.__db.getAllDatasets( username ):
      print ( "| %s | %s | %d |"%(username, ds.dataset, len(ds.files)) )





  def delete( self, datasetname ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The dataset name must starts with: user.%USER.taskname.')
    username = datasetname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    if not self.__db.getDataset( username, datasetname ):
      MSG_FATAL( self, "The dataset exist into the database")


    # prepare to remove from database
    ds = self.__db.getDataset( username, datasetname )

    for file in ds.getAllFiles():
      # Delete the file inside of the dataset
      self.__db.session().query(File).filter( File.id==file.id ).delete()

    # Delete the dataset
    self.__db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()


    # The path to the dataset in the cluster
    file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
    file_dir = file_dir.replace('//','/')

    # Delete the file from the storage
    # check if this path exist
    if os.path.exists(file_dir):
      command = 'rm -rf {FILE}'.format(FILE=file_dir)
      print(command)
    else:
      MSG_WARNING(self, "This dataset does not exist into the database (%s)", file_dir)

    self.__db.commit()

  #
  # Download the dataset in the cluster storage to the current directory
  #
  def download( self, datasetname ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The dataset name must starts with: user.%USER.taskname.')
    username = datasetname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    if not self.__db.getDataset( username, datasetname ):
      MSG_FATAL( self, "The dataset exist into the database")


    # The path to the dataset in the cluster
    file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname

    # check if this path exist
    if not os.path.exists(file_dir):
      MSG_FATAL(self, "This dataset does not exist into the database (%s)", file_dir)

    # copy to the current directory
    os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=file_dir,DESTINATION=datasetname) )



  #
  # Upload and create the dataset into the cluster storage/database
  #
  def upload( self , datasetname, path ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The dataset name must starts with: user.%USER.taskname.')
    username = datasetname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    if self.__db.getDataset( username, datasetname ):
      MSG_FATAL( self, "The dataset exist into the database")

    # Let's registry and upload into the database
    try:
      # Create the new dataset
      ds  = Dataset( username=username, dataset=datasetname, cluster=self.__db.getCluster())

      # check if file exist into the storage
      # Get file and assure file name is OK
      filename = path
      destination_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname

      # treat path string with duplicate /
      destination_dir = destination_dir.replace('//','/')

      # If dir doesn't exist, creates it
      if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

      os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=filename, DESTINATION=destination_dir) )
      # Loop over files
      for path in expandFolders(destination_dir):
        print("Registry %s into %s"%(path,datasetname))
        hash_object = hashlib.md5(str.encode(path))
        ds.addFile( File(path=path, hash=hash_object.hexdigest()) )
      self.__db.createDataset(ds)
      self.__db.commit()
    except:
        MSG_FATAL( self, "Impossible to registry the dataset(%s)", datasetname)






class TaskParser(Logger):


  def __init__(self , db, subparser=None):

    Logger.__init__(self)
    self.__db = db

    if subparser:
      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)
      create_parser.add_argument('-c','--configFile', action='store',
                          dest='configFile', required = True,
                          help = "The job config file that will be used to configure the job (sort and init).")
      create_parser.add_argument('-d','--dataFile', action='store',
                          dest='dataFile', required = True,
                          help = "The data/target file used to train the model.")
      create_parser.add_argument('--exec', action='store', dest='execCommand', required=True,
                          help = "The exec command")
      create_parser.add_argument('--containerImage', action='store', dest='containerImage', required=True,
                          help = "The container image point to docker hub. The container must be public.")
      create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                          help = "The task name to be append into the db.")
      create_parser.add_argument('--sd','--secondaryDS', action='store', dest='secondaryDS', required=False,  default="{}",
                          help = "The secondary datasets to be append in the --exec command. This should be:" +
                          "--secondaryData='{'REF':'path/to/my/extra/data',...}'")
      create_parser.add_argument('--gpu', action='store_true', dest='gpu', required=False, default=False,
                          help = "Send these jobs to GPU slots")
      create_parser.add_argument('--et', action='store', dest='et', required=False,default=None,
                          help = "The ET region (ringer staff)")
      create_parser.add_argument('--eta', action='store', dest='eta', required=False,default=None,
                          help = "The ETA region (ringer staff)")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--bypass', action='store_true', dest='bypass_test_job', required=False, default=False,
                          help = "Bypass the job test.")


      retry_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                    help = "The task name to be retry")
      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                    help = "The task name to be remove")

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry', parents=[retry_parser])
      subparser.add_parser('remove', parents=[delete_parser])


  def compile( self, args ):
    # Task CLI
    if args.parent == 'create':
      self.create(args.taskname, args.dataFile, args.configFile, args.secondaryDS,
                  args.execCommand,args.containerImage,args.et,args.eta,args.gpu,
                  args.bypass_test_job, args.dry_run)
    elif args.parent == 'retry':
      self.retry(args.taskname)
    elif args.parent == 'remove':
      self.delete(args.taskname)





  def create( self, taskname, dataFile,
                    configFile, secondaryDS,
                    execCommand, containerImage, et=None, eta=None, gpu=False,
                    bypass_test_job=False, dry_run=False):


    # check task policy (user.username)
    if taskname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The task name must starts with: user.%USER.taskname.')

    # check task policy (username must exist into the database)
    username = taskname.split('.')[1]

    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')


    # Check if the task exist into the databse
    if self.__db.getUser(username).getTask(taskname) is not None:
      MSG_FATAL( self, "The task exist into the database. Abort.")


    # check data (file) is in database
    if self.__db.getDataset(username, dataFile) is None:
      MSG_FATAL( self, "The file (%s) does not exist into the database. Should be registry first.", dataFile)


    # check configFile (file) is in database
    if self.__db.getDataset(username, configFile) is None:
      MSG_FATAL( self, "The config file (%s) does not exist into the database. Should be registry first.", configFile)


    # Get the secondary data as dict
    secondaryDS = eval(secondaryDS)


    # check secondary data paths exist is in database
    for key in secondaryDS.keys():
      if self.__db.getDataset(username, secondaryDS[key]) is None:
        MSG_FATAL( self, "The secondary data file (%s) does not exist into the database. Should be registry first.", secondaryDS[key])




    # check exec command policy
    if not '%DATA' in execCommand:
      MSG_FATAL( self,  "The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")
    if not '%IN' in execCommand:
      MSG_FATAL( self, "The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
    if not '%OUT' in execCommand:
      MSG_FATAL( self, "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")


    # parser the secondary data in the exec command
    for key in secondaryDS.keys():
      if not key in execCommand:
        MSG_FATAL( selrf, "The exec command must include %s into the string. This will substitute to %s when start",key, secondaryDS[key])


    # check if task exist into the storage
    outputFile = CLUSTER_VOLUME +'/'+username+'/'+taskname

    if os.path.exists(outputFile):
      MSG_FATAL(self, "The task dir exist into the storage. Contact the administrator.")
    else:
      # create the task dir
      MSG_INFO(self, "Creating the task dir in %s", outputFile)
      os.system( 'mkdir %s '%(outputFile) )



    # check if the output exist into the dataset base
    if self.__db.getDataset(username, taskname ):
      MSG_FATAL( self, "The output dataset exist. Please, remove than or choose another name for this task", taskname )


    # create the task into the database
    if not dry_run:
      try:
        user = db.getUser( username )
        task = db.createTask( user, taskname, configFile, dataFile, taskname,
                            containerImage, self.__db.getCluster(),
                            secondaryDataPath=secondaryDS,
                            templateExecArgs=execCommand,
                            etBinIdx=et,
                            etaBinIdx=eta,
                            isGPU=gpu,
                            )
        task.setStatus('hold')
      except Exception as e:
        MSG_FATAL(self, e)

    configFiles = db.getDataset(username, configFile).getAllFiles()

    _dataFile = db.getDataset(username, dataFile).getAllFiles()[0].getPath()
    _dataFile = _dataFile.replace( CLUSTER_VOLUME, '/volume' ) # to docker path

    _outputFile = '/volume/'+username+'/'+taskname # to docker path
    _secondaryDS = {}

    for key in secondaryDS.keys():
      _secondaryDS[key] = self.__db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()
      _secondaryDS[key] = _secondaryDS[key].replace(CLUSTER_VOLUME, '/volume') # to docker path

    for idx, file in enumerate(configFiles):

      _configFile = file.getPath()
      _configFile = _configFile.replace(CLUSTER_VOLUME, '/volume') # to docker path

      command = execCommand
      command = command.replace( '%DATA' , _dataFile  )
      command = command.replace( '%IN'   , _configFile)
      command = command.replace( '%OUT'  , _outputFile)

      for key in _secondaryDS:
        command = command.replace( key  , _secondaryDS[key])

      if not args.dry_run:
        job = db.createJob( task, _configFile, idx, execArgs=command, isGPU=gpu, priority=-1 )
        job.setStatus('assigned' if bypass_test_job else 'registered')


    if not args.dry_run:
      task.setStatus( 'running' if bypass_test_job else 'registered'  )
      try: # Create the new dataset
        ds  = Dataset( username=username, dataset=taskname, cluster=self.__db.getCluster())
        ds.addFile( File(path=_outputFile, hash='' ) )
        self.__db.createDataset(ds)
      except:
        MSG_FATAL( self, "Impossible to registry the dataset(%s)", datasetname)


      self.__db.commit()
      self.__db.close()





  def delete( self, taskname ):

    if taskname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The task name must starts with: user.%USER.taskname.')
    username = taskname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')

    try:
      user = self.__db.getUser( username )
    except:
      MSG_FATAL( self , "The user name (%s) does not exist into the data base", username)

    try:
      task = self.__db.getTask( taskname )
    except:
      MSG_FATAL( self, "The task name (%s) does not exist into the data base", args.taskname)

    id = task.id

    try:
      self.__db.session().query(Job).filter(Job.taskId==id).delete()
    except Exception as e:
      MSG_FATAL( self, "Impossible to remove Job lines from (%d) task", id)

    try:
      self.__db.session().query(Task).filter(Task.id==id).delete()
    except Exception as e:
      MSG_FATAL( self, "Impossible to remove Task lines from (%d) task", id)

    try:
      self.__db.session().query(Board).filter(Task.id==id).delete()
    except Exception as e:
      MSG_WARNING( self, "Impossible to remove Task board lines from (%d) task", id)

    self.__db.commit()

    # Remove the dataset
    DatasetParser(self.__ds).delete(taskname)






  def retry( self, taskname ):


    if taskname.split('.')[0] != 'user':
      MSG_FATAL( self, 'The task name must starts with: user.%USER.taskname.')
    username = taskname.split('.')[1]
    if username in self.__db.getAllUsers():
      MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')

    try:
      user = self.__db.getUser( username )
    except:
      MSG_FATAL( self , "The user name (%s) does not exist into the data base", username)

    try:
      task = self.__db.getTask( taskname )
    except:
      MSG_FATAL( self, "The task name (%s) does not exist into the data base", args.taskname)



    for job in task.getAllJobs():
      try:
        if job.getStatus() == Status.FAILED:
          job.setStatus(Status.ASSIGNED)
      except:
        MSG_FATAL( self, "Impossible to assigned this job for (%d) task", id)

    self.__db.commit()
    self.__db.close()









parser = argparse.ArgumentParser()

subparser = parser.add_subparsers(dest='parent')

# create the database manager
db = OrchestraDB(cluster = Cluster.LPS)

engine = [
            DatasetParser(db, subparser),
            TaskParser(db, subparser),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for e in engine:
  e.compile(args)



























