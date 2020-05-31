
__all__ = ["TaskParser"]

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import csvStr2List, expandFolders
from Gaugi import load
from Gaugi import StatusCode

# Connect to DB
from orchestra.constants import CLUSTER_VOLUME
from orchestra.db import OrchestraDB
from orchestra.db import Task,Dataset,File, Board, Job
from orchestra import Status, Cluster, Signal
from sqlalchemy import and_, or_

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib
import argparse


def getStatus(status):
  from Gaugi import Color
  if status == 'registered':
    return Color.CWHITE2+"REGISTERED"+Color.CEND
  elif status == 'assigned':
    return Color.CWHITE2+"ASSIGNED"+Color.CEND
  elif status == 'testing':
    return Color.CGREEN2+"TESTING"+Color.CEND
  elif status == 'running':
    return Color.CGREEN2+"RUNNING"+Color.CEND
  elif status == 'done':
    return Color.CGREEN2+"DONE"+Color.CEND
  elif status == 'failed':
    return Color.CGREEN2+"DONE"+Color.CEND
  elif status == 'killed':
    return Color.CRED2+"KILLED"+Color.CEND
  elif status == 'finalized':
    return Color.CRED2+"FINALIZED"+Color.CEND
  elif status == 'broken':
    return Color.CRED2+"BROKEN"+Color.CEND
  elif status == 'hold':
    return Color.CRED2+"HOLD"+Color.CEND
  elif status == 'removed':
    return Color.CRED2+"REMOVED"+Color.CEND
  elif status == 'to_be_removed':
    return Color.CRED2+"REMOVING"+Color.CEND
  elif status == 'to_be_removed_soon':
    return Color.CRED2+"REMOVING"+Color.CEND



class TaskParser(Logger):


  def __init__(self , db, args=None):

    Logger.__init__(self)
    self.__db = db

    if args:

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
      create_parser.add_argument('--et', action='store', dest='et', required=False,default=None,
                          help = "The ET region (ringer staff)")
      create_parser.add_argument('--eta', action='store', dest='eta', required=False,default=None,
                          help = "The ETA region (ringer staff)")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--bypass', action='store_true', dest='bypass_test_job', required=False, default=False,
                          help = "Bypass the job test.")
      create_parser.add_argument('--queue', action='store', dest='queue', required=True, default='cpu_small',
                          help = "The cluste queue [cpu_small, nvidia or cpu_large]")
      create_parser.add_argument('--is_dummy_data', action='store_true', dest='is_dummy_data', required=False,
                          help = "The data input is a dummy dataset. ignore the %DATA")
      create_parser.add_argument('--is_dummy_config', action='store_true', dest='is_dummy_config', required=False,
                          help = "The config input is a dummy dataset. ignore the %IN")



      retry_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                    help = "The task name to be retry")


      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser.add_argument('-t','--task', action='store', dest='taskname', required=False,
                    help = "The task name to be remove")
      delete_parser.add_argument('--remove', action='store_true', dest='remove', required=False,
                    help = "Remove all files for this task into the storage. Beware when use this flag becouse you will lost your data too.")


      list_parser = argparse.ArgumentParser(description = '', add_help = False)
      list_parser.add_argument('-u','--user', action='store', dest='username', required=True,
                    help = "The username.")

      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('-t','--task', action='store', dest='taskname', required=False,
                    help = "The taskname to be killed.")

      queue_parser = argparse.ArgumentParser(description = '', add_help = False)
      queue_parser.add_argument('-n','--name', action='store', dest='name', required=False,
                    help = "The queue name")





      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry', parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('kill', parents=[kill_parser])
      subparser.add_parser('queue', parents=[queue_parser])
      args.add_parser( 'task', parents=[parent] )




  def compile( self, args ):
    # Task CLI
    if args.mode == 'task':

      # create task
      if args.option == 'create':
        status , answer = self.create(args.taskname, args.dataFile, args.configFile, args.secondaryDS,
                                      args.execCommand,args.containerImage,args.et,args.eta,
                                      args.bypass_test_job, args.dry_run, args.queue, args.is_dummy_data,
                                      args.is_dummy_config)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)


      # retry option
      elif args.option == 'retry':
        status, answer = self.retry(args.taskname)
        if status.isFailure():
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer
              )

      # delete option
      elif args.option == 'delete':
        status , answer = self.delete(args.taskname, args.remove)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      # list all tasks
      elif args.option == 'list':
        status, answer = self.list(args.username)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      # kill option
      elif args.option == 'kill':
        status, answer = self.kill(args.taskname)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'queue':
        status, answer = self.queue(args.name)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)



      else:
        MSG_FATAL(self, "option not available.")




      #
      # Check the status and disp the answer
      #
      if status.isFailure():
        MSG_FATAL()




  #
  # Create the new task
  #
  def create( self, taskname,
                    dataFile,
                    configFile,
                    secondaryDS,
                    execCommand,
                    containerImage,
                    et=None,
                    eta=None,
                    bypass_test_job=False,
                    dry_run=False,
                    queue='cpu_small',
                    is_dummy_data=False,
                    is_dummy_config=False):


    # check task policy (user.username)
    if taskname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The task name must starts with user.$USER.taskname.')


    # check task policy (username must exist into the database)
    username = taskname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL,'The username does not exist into the database.')


    from orchestra.constants import allow_queue_names
    if not queue in allow_queue_names:
      return (StatusCode.FATAL, "The queue with name %s does not exist. Please check the name of all available queues"% queue )


    if not queue in self.__db.getUser(username).getAllAllowedQueues():
      return (StatusCode, "You not allowed to create tasks using this queue: %s. Please contact the administrator."% queue )


    # Check if the task exist into the databse
    if self.__db.getUser(username).getTask(taskname) is not None:
      return (StatusCode.FATAL, "The task exist into the database. Abort.")


    # check data (file) is in database
    if self.__db.getDataset(username, dataFile) is None:
      return (StatusCode.FATAL, "The file (%s) does not exist into the database. Should be registry first."%dataFile)


    # check configFile (file) is in database
    if self.__db.getDataset(username, configFile) is None:
      return (StatusCode.FATAL, "The config file (%s) does not exist into the database. Should be registry first."%configFile)


    # Get the secondary data as dict
    secondaryDS = eval(secondaryDS)


    # check secondary data paths exist is in database
    for key in secondaryDS.keys():
      if self.__db.getDataset(username, secondaryDS[key]) is None:
        return (StatusCode.FATAL , "The secondary data file (%s) does not exist into the database. Should be registry first."% secondaryDS[key] )

    # check exec command policy
    if (not is_dummy_data) and (not '%DATA' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")

    if (not is_dummy_config) and (not '%IN' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%IN' into the string. This will substitute to the configFile when start.")

    if not '%OUT' in execCommand:
      return (StatusCode.FATAL, "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")


    # parser the secondary data in the exec command
    for key in secondaryDS.keys():
      if not key in execCommand:
        return (StatusCode.FATAL,  ("The exec command must include %s into the string. This will substitute to %s when start")%(key, secondaryDS[key]) )



    # check if the output exist into the dataset base
    if self.__db.getDataset(username, taskname ):
      return (StatusCode.FATAL, "The output dataset exist. Please, remove than or choose another name for this task")


    # Check if the board monitoring for this task exist into the database
    if self.__db.session().query(Board).filter( Board.taskName==taskname ).first():
      return (StatusCode.FATAL, "There is a board monitoring with this taskname. Contact the administrator." )


    # check if task exist into the storage
    outputFile = CLUSTER_VOLUME +'/'+username+'/'+taskname

    if os.path.exists(outputFile):
      MSG_WARNING(self, "The task dir exist into the storage. Beware!")
    else:
      # create the task dir
      MSG_INFO(self, "Creating the task dir in %s", outputFile)
      os.system( 'mkdir %s '%(outputFile) )


    # create the task into the database
    if not dry_run:
      try:
        user = self.__db.getUser( username )
        task = self.__db.createTask( user, taskname, configFile, dataFile, taskname,
                            containerImage, self.__db.getCluster(),
                            secondaryDataPath=secondaryDS,
                            templateExecArgs=execCommand,
                            etBinIdx=et,
                            etaBinIdx=eta,
                            queue=queue,
                            )
        task.setSignal(Signal.WAITING)
        task.setStatus('hold')

        configFiles = self.__db.getDataset(username, configFile).getAllFiles()

        _dataFile = self.__db.getDataset(username, dataFile).getAllFiles()[0].getPath()
        _dataFile = _dataFile.replace( CLUSTER_VOLUME, '/volume' ) # to docker path

        _secondaryDS = {}

        for key in secondaryDS.keys():
          _secondaryDS[key] = self.__db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()
          _secondaryDS[key] = _secondaryDS[key].replace(CLUSTER_VOLUME, '/volume') # to docker path

        for idx, file in enumerate(configFiles):

          _outputFile = '/volume/'+username+'/'+taskname+ '/job_configId_%d'%idx # to docker path
          _configFile = file.getPath()
          _configFile = _configFile.replace(CLUSTER_VOLUME, '/volume') # to docker path

          command = execCommand
          command = command.replace( '%DATA' , _dataFile  )
          command = command.replace( '%IN'   , _configFile)
          command = command.replace( '%OUT'  , _outputFile)

          for key in _secondaryDS:
            command = command.replace( key  , _secondaryDS[key])

          job = self.__db.createJob( task, _configFile, idx, execArgs=command, priority=-1 )
          job.setStatus('assigned' if bypass_test_job else 'registered')


        desired_id = self.__db.session().query(Dataset).order_by(Dataset.id.desc()).first().id + 1
        ds  = Dataset( id=desired_id, username=username, dataset=taskname, cluster=self.__db.getCluster(), task_usage=True)
        desired_id = self.__db.session().query(File).order_by(File.id.desc()).first().id + 1
        ds.addFile( File(path=outputFile, hash='', id=desired_id ) ) # the real path
        self.__db.createDataset(ds)
        self.createBoard( user, task )
        task.setStatus('registered')
        self.__db.commit()
      except Exception as e:
        MSG_ERROR(self,e)
        return (StatusCode.FATAL, "Unknown error.")


    return (StatusCode.SUCCESS, "Succefully created.")



  def delete( self, taskname, remove=False ):


    if taskname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The task name must starts with user.$USER.taskname.')

    username = taskname.split('.')[1]

    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL, 'The username does not exist into the database.')


    task = self.__db.getTask( taskname )
    if not task:
      return (StatusCode.FATAL, "The task name (%s) does not exist into the data base"%taskname )


    # Check possible status before continue
    if not task.getStatus() in [Status.BROKEN, Status.KILLED, Status.FINALIZED, Status.DONE, Status.TO_BE_REMOVED, Status.TO_BE_REMOVED_SOON]:
      return (StatusCode.FATAL, "The task with current status %s can not be deleted. The task must be in done, finalized, killed or broken status."% task.getStatus() )


    id = task.id

    # remove all jobs that allow to this task
    try:
      self.__db.session().query(Job).filter(Job.taskId==id).delete()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Impossible to remove Job lines from (%d) task"%id )


    # remove the task table
    try:
      self.__db.session().query(Task).filter(Task.id==id).delete()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Impossible to remove Task lines from (%d) task"%id )

    # remove the board table
    try:
      self.__db.session().query(Board).filter(Board.taskId==id).delete()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Impossible to remove Task board lines from (%d) task"%id )


    # prepare to remove from database
    ds = self.__db.getDataset( username, taskname )

    if not ds.task_usage:
      return (StatusCode.FATAL, "This dataset is not task usage. There is something strange..." )


    for file in ds.getAllFiles():
      # Delete the file inside of the dataset
      self.__db.session().query(File).filter( File.id==file.id ).delete()


    # Delete the dataset
    self.__db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()

    if remove:
      # The path to the dataset in the cluster
      file_dir = CLUSTER_VOLUME + '/' + username + '/' + taskname
      file_dir = file_dir.replace('//','/')
      # Delete the file from the storage
      # check if this path exist
      if os.path.exists(file_dir):
        command = 'rm -rf {FILE}'.format(FILE=file_dir)
        print(command)
      else:
        MSG_ERROR(self, "This dataset does not exist into the database (%s)", file_dir)


    self.__db.commit()
    return (StatusCode.SUCCESS, "Succefully deleted.")








  def list( self, username ):

    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL, 'The username does not exist into the database.' )


    from Gaugi import Color

    from prettytable import PrettyTable
    t = PrettyTable([
                      Color.CGREEN2 + 'Queue'       + Color.CEND,
                      Color.CGREEN2 + 'Taskname'    + Color.CEND,
                      Color.CGREEN2 + 'Registered'  + Color.CEND,
                      Color.CGREEN2 + 'Assigned'    + Color.CEND,
                      Color.CGREEN2 + 'Testing'     + Color.CEND,
                      Color.CGREEN2 + 'Running'     + Color.CEND,
                      Color.CRED2   + 'Failed'      + Color.CEND,
                      Color.CGREEN2 + 'Done'        + Color.CEND,
                      Color.CRED2   + 'kill'        + Color.CEND,
                      Color.CRED2   + 'killed'      + Color.CEND,
                      Color.CRED2   + 'broken'      + Color.CEND,
                      Color.CGREEN2 + 'Status'      + Color.CEND,
                      ])

    user = self.__db.getUser(username)
    tasks = user.getAllTasks()

    def count( jobs, status ):
      total=0
      for job in jobs:
        if job.status==status:  total+=1
      return total


    for task in tasks:
      jobs = task.getAllJobs()
      queue         = task.queueName
      taskName      = task.taskName
      registered    = count( jobs, Status.REGISTERED)
      assigned      = count( jobs, Status.ASSIGNED  )
      testing       = count( jobs, Status.TESTING   )
      running       = count( jobs, Status.RUNNING   )
      done          = count( jobs, Status.DONE      )
      failed        = count( jobs, Status.FAILED    )
      kill          = count( jobs, Status.KILL      )
      killed        = count( jobs, Status.KILLED    )
      broken        = count( jobs, Status.BROKEN    )
      status        = task.status
      t.add_row(  [queue, taskName, registered,  assigned, testing, running, failed,  done, kill, killed, broken, getStatus(status)] )

    return (StatusCode.SUCCESS, t)






  def kill( self, taskname ):

    if taskname.split('.')[0] != 'user':
      return ( StatusCode.FATAL , 'The task name must starts with: user.%USER.taskname.' )

    username = taskname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return ( StatusCode.FATAL, 'The username does not exist into the database.')

    try:
      task = self.__db.getTask( taskname )
      if task is None:
        return (StatusCode.FATAL, "Task does not exist into the database.")
      # Send kill signal to the task
      task.setSignal( Signal.KILL )
      self.__db.commit()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Unknown error." )

    return (StatusCode.SUCCESS, "Succefully killed.")




  def retry( self, taskname ):

    if taskname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The task name must starts with: user.%USER.taskname.')

    username = taskname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return  (StatusCode.FATAL, 'The username does not exist into the database.')

    try:
      task = self.__db.getTask( taskname )
      if task is None:
        return (StatusCode.FATAL, "The task does not exist into the database.")
      # Send retry signal to the task
      task.setSignal( Signal.RETRY )
      self.__db.commit()
    except Exception as e:
      MSG_ERROR(self, e)
      return (StatusCode.FATAL, "Unknown error." )

    return (StatusCode.SUCCESS, "Succefully retry.")





  #
  # This is for monitoring purpose. Should be used to dashboard view
  #
  def createBoard( self , user, task):

    desired_id = self.__db.session().query(Board).order_by(Board.id.desc()).first().id + 1
    board = Board( username=user.username, taskId=task.id, taskName=task.taskName, id=desired_id )
    board.jobs = len(task.getAllJobs())
    board.registered = board.jobs
    board.assigned=board.testing=board.running=board.failed=board.done=board.killed=0
    board.status = task.status
    self.__db.session().add(board)



  def queue( self , queuename ):

    from orchestra.constants import allow_queue_names
    if not queuename in allow_queue_names:
      return (StatusCode.FATAL, "The queue with name %s does not exist. Please check the name of all available queues"% queue )






    from Gaugi import Color


    from prettytable import PrettyTable
    t = PrettyTable([
                      Color.CGREEN2 + 'username'    + Color.CEND,
                      Color.CGREEN2 + 'Queue'       + Color.CEND,
                      Color.CGREEN2 + 'Taskname'    + Color.CEND,
                      Color.CGREEN2 + 'job'         + Color.CEND,
                      Color.CGREEN2 + 'priority'    + Color.CEND,
                      Color.CGREEN2 + 'Status'      + Color.CEND,
                      ])
    from sqlalchemy import and_, or_
    from sqlalchemy import desc


    assigned_jobs = self.__db.session().query(Job).filter(Job.cluster=='LPS').filter(  and_( Job.status==Status.ASSIGNED ,
        Job.queueName==queuename) ).order_by(desc(Job.priority)).limit(10).with_for_update().all()
    assigned_jobs.reverse()

    running_jobs = self.__db.session().query(Job).filter(Job.cluster=='LPS').filter(  and_( Job.status==Status.RUNNING ,
        Job.queueName==queuename) ).order_by(desc(Job.priority)).with_for_update().all()
    running_jobs.reverse()


    for job in running_jobs:
      t.add_row(  [job.getUserName(), job.getQueueName(), job.getTaskName(), job.configId, job.getPriority(), getStatus(job.status)] )
    for job in assigned_jobs:
      t.add_row(  [job.getUserName(), job.getQueueName(), job.getTaskName(), job.configId, job.getPriority(), getStatus(job.status)] )

    return (StatusCode.SUCCESS, t)







