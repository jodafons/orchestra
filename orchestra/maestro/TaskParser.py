
__all__ = ["TaskParser"]

from Gaugi import LoggingLevel, Logger
from Gaugi.macros import *
from Gaugi import StatusCode, Color, expand_folders, progressbar

# Connect to DB
from orchestra.db import Task,Dataset,File,Job
from orchestra import Status, Signal, getStatus
from sqlalchemy import and_, or_
from prettytable import PrettyTable

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib

from orchestra.utils import getConfig
config = getConfig()


#
# Task parser
#
class TaskParser(Logger):


  def __init__(self , db, args=None):

    Logger.__init__(self)
    self.__db = db

    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('-v','--volume', action='store', dest='volume', required=True,
                          help = "The volume")
      create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                          help = "The task name to be append into the db.")
      create_parser.add_argument('-c','--configFile', action='store',
                          dest='configFile', required = True,
                          help = "The job config file that will be used to configure the job (sort and init).")
      create_parser.add_argument('-d','--dataFile', action='store',
                          dest='dataFile', required = True,
                          help = "The data/target file used to train the model.")
      create_parser.add_argument('--sd','--secondaryDS', action='store', dest='secondaryDS', required=False,  default="{}",
                          help = "The secondary datasets to be append in the --exec command. This should be:" +
                          "--secondaryData='{'REF':'path/to/my/extra/data',...}'")
      create_parser.add_argument('--exec', action='store', dest='execCommand', required=True,
                          help = "The exec command")
      create_parser.add_argument('--queue', action='store', dest='queue', required=True, default='gpu',
                          help = "The cluste queue [gpu or cpu]")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--bypass', action='store_true', dest='bypass', required=False, default=False,
                          help = "Bypass the job test.")
      create_parser.add_argument('--bypass_local_test', action='store_true', dest='bypass_local_test', required=False, default=False,
                          help = "Bypass the local job test.")


      
      # Create Task
      repro_parser = argparse.ArgumentParser(description = '', add_help = False)


      repro_parser.add_argument('-v','--volume', action='store', dest='volume', required=True,
                         help = "The volume")
      repro_parser.add_argument('--new_task', action='store', dest='new_taskname', required=True,
                         help = "The new task name after the reprocessing phase")
      repro_parser.add_argument('--old_task', action='store', dest='old_taskname', required = True,
                         help = "The old task name that will be used into the reprocessing phase")
      repro_parser.add_argument('-d','--dataFile', action='store', dest='dataFile', required = True,
                         help = "The data/target file used to train the model.")
      repro_parser.add_argument('--sd','--secondaryDS', action='store', dest='secondaryDS', required=False,  default="{}",
                         help = "The secondary datasets to be append in the --exec command. This should be:" +
                         "--secondaryData='{'REF':'path/to/my/extra/data',...}'")
      repro_parser.add_argument('--exec', action='store', dest='execCommand', required=True,
                         help = "The exec command")
      repro_parser.add_argument('--queue', action='store', dest='queue', required=True, default='gpu',
                         help = "The cluste queue [gpu or cpu]")
      repro_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                         help = "Use this as debugger.")





      retry_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)

      retry_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)

      retry_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)



      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)

      delete_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)

      delete_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)
      delete_parser.add_argument('--remove', action='store_true', dest='remove', required=False,
                    help = "Remove all files for this task into the storage. Beware when use this flag becouse you will lost your data too.")
      delete_parser.add_argument('--force', action='store_true', dest='force', required=False,
                    help = "Force delete.")



      list_parser = argparse.ArgumentParser(description = '', add_help = False)
      list_parser.add_argument('-u','--user', action='store', dest='username', required=False, default=config['username'],
                    help = "The username.")
      list_parser.add_argument('-a','--all', action='store_true', dest='all', required=False,
                    help = "List all tasks.")
      list_parser.add_argument('-i','--interactive', action='store_true', dest='interactive', required=False,
                    help = "List all tasks interactive mode.")







      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)
      kill_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      kill_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)



      queue_parser = argparse.ArgumentParser(description = '', add_help = False)
      queue_parser.add_argument('-n','--name', action='store', dest='name', required=False,
                    help = "The queue name")


      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('repro', parents=[repro_parser])
      subparser.add_parser('retry', parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('kill', parents=[kill_parser])
      subparser.add_parser('queue', parents=[queue_parser])
      args.add_parser( 'task', parents=[parent] )




  def compile( self, args ):

    def get_task_ids( _args ):
      if _args.id_list:
        id_list = _args.id_list
      elif _args.id_min and _args.id_max:
        id_list = list( range( _args.id_min, _args.id_max+1 ) )
      else:
        MSG_FATAL(self, "Invalid input.")
      return id_list

    # Task CLI
    if args.mode == 'task':

      # create task
      if args.option == 'create':
        status , answer = self.create(args.volume,
                                      args.taskname,
                                      args.dataFile,
                                      args.configFile,
                                      args.secondaryDS,
                                      args.execCommand,
                                      args.queue,
                                      args.bypass,
                                      args.dry_run,
                                      bypass_local_test=args.bypass_local_test)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      # create task
      elif args.option == 'repro':
        status , answer = self.repro(args.volume,
                                      args.new_taskname,
                                      args.dataFile,
                                      args.old_taskname,
                                      args.secondaryDS,
                                      args.execCommand,
                                      args.queue,
                                      args.dry_run)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)




      # retry option
      elif args.option == 'retry':
        task_id_list = get_task_ids(args)
        status, answer = self.retry(task_id_list)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      # delete option
      elif args.option == 'delete':
        task_id_list = get_task_ids(args)
        status , answer = self.delete(task_id_list, remove=args.remove, force=args.force)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      # list all tasks
      elif args.option == 'list':
        status, answer = self.list(args.username, args.all, args.interactive)
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      # kill option
      elif args.option == 'kill':
        task_id_list = get_task_ids(args)
        status, answer = self.kill(task_id_list)
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
  def create( self, volume,
                    taskname,
                    dataFile,
                    configFile,
                    secondaryDS,
                    execCommand,
                    queue='gpu',
                    bypass=False,
                    dry_run=False,
                    force_dummy=False,
                    bypass_local_test=False):



    # check task policy (user.username)
    if taskname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The task name must starts with user.$USER.taskname.')

    # check task policy (username must exist into the database)
    username = taskname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL,'The username does not exist into the database.')

    if self.__db.getUser(username).getTask(taskname) is not None:
      return (StatusCode.FATAL, "The task exist into the database. Abort.")


    #
    # Check if all datasets are registered into the database
    #

    if self.__db.getDataset(username, dataFile) is None:
      return (StatusCode.FATAL, "The file (%s) does not exist into the database. Should be registry first."%dataFile)

    if self.__db.getDataset(username, configFile) is None:
      return (StatusCode.FATAL, "The config file (%s) does not exist into the database. Should be registry first."%configFile)


    secondaryDS = eval(secondaryDS)
    for key in secondaryDS.keys():
      if self.__db.getDataset(username, secondaryDS[key]) is None:
        return (StatusCode.FATAL , "The secondary data file (%s) does not exist into the database. Should be registry first."% secondaryDS[key] )


    #
    # check exec command policy
    #
    if (not '%DATA' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")

    if (not '%IN' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%IN' into the string. This will substitute to the configFile when start.")

    if not '%OUT' in execCommand:
      return (StatusCode.FATAL, "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")

    for key in secondaryDS.keys():
      if not key in execCommand:
        return (StatusCode.FATAL,  ("The exec command must include %s into the string. This will substitute to %s when start")%(key, secondaryDS[key]) )



    #
    # Create the output file
    #
    outputFile = volume +'/'+taskname

    if os.path.exists(outputFile):
      MSG_WARNING(self, "The task dir exist into the storage. Beware!")
    else:
      # create the task dir
      MSG_INFO(self, "Creating the task dir in %s", outputFile)
      os.system( 'mkdir -p %s '%(outputFile) )



    #
    # create the task into the database
    #
    if not dry_run:
      try:
        user = self.__db.getUser( username )


        task = self.__db.createTask( user, taskname, configFile, dataFile, outputFile, "" ,
                                     secondaryDataPath=secondaryDS,
                                     templateExecArgs=execCommand,
                                     queueName=queue)


        task.setSignal(Signal.WAITING)
        task.setStatus(Status.HOLD)

        configFiles = self.__db.getDataset(username, configFile).getAllFiles()

        _dataFile = self.__db.getDataset(username, dataFile).getAllFiles()[0].getPath()

        _secondaryDS = {}

        for key in secondaryDS.keys():
          _secondaryDS[key] = self.__db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()

        offset_job_id = self.__db.generateId(Job)
        
        for idx, file in progressbar( enumerate(configFiles), prefix='Creating...' ):

          _outputFile = outputFile+ '/job_configId_%d'%idx

          _configFile = file.getPath()

          command = execCommand
          command = command.replace( '%DATA' , _dataFile  )
          command = command.replace( '%IN'   , _configFile)
          command = command.replace( '%OUT'  , _outputFile)

          for key in _secondaryDS:
            command = command.replace( key  , _secondaryDS[key])

          
          job = self.__db.createJob( task, _configFile, idx, execArgs=command, priority=-1, id = offset_job_id+idx )
          job.setStatus('assigned' if bypass else 'registered')


        task.setStatus('registered')

        if not bypass_local_test:
          # Test locally before send to the database
          if self.__test_job_locally( task.getAllJobs()[0] ):
              self.__db.commit()
          else:
              return (StatusCode.FATAL, "Local test failed.")
        else:
          self.__db.commit()

      except Exception as e:
        MSG_ERROR(self,e)
        return (StatusCode.FATAL, "Unknown error.")

    return (StatusCode.SUCCESS, "Succefully created.")



  def delete( self, task_id_list, remove=False, force=False ):


    for id in task_id_list:
       

        # Get task by id
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
          return (StatusCode.FATAL, "The task with id (%d) does not exist into the data base"%id )

        MSG_INFO( self, 'Delete task (%d) with name: %s', id, task.taskName)
        # Check possible status before continue
        if not force:
          if not task.getStatus() in [Status.BROKEN, Status.KILLED, Status.FINALIZED, Status.DONE]:
            return (StatusCode.FATAL, "The task with current status %s can not be deleted. The task must be in done, finalized, killed or broken status."% task.getStatus() )

        # remove all jobs that allow to this task
        try:
          self.__db.session().query(Job).filter(Job.taskId==id).delete()
          self.__db.commit()
        except Exception as e:
          MSG_WARNING(self,e)


        # remove the task table
        try:
          self.__db.session().query(Task).filter(Task.id==id).delete()
          self.__db.commit()
        except Exception as e:
          MSG_WARNING(self,e)


    return (StatusCode.SUCCESS, "Succefully deleted.")









  def list( self, username, list_all, interactive=True ):


    def count( jobs ):
      status_list = [Status.REGISTERED, Status.ASSIGNED, Status.TESTING, Status.RUNNING, Status.DONE, Status.FAILED, Status.KILL, Status.KILLED, Status.BROKEN]
      total = { str(key):0 for key in status_list }
      for job in jobs:
        for s in status_list:
          if job.status==s: total[str(s)]+=1
      return total

    def get_table( username, list_all ):
      user = self.__db.getUser(username)
      tasks = user.getAllTasks()
      t = PrettyTable([

                        Color.CGREEN2 + 'TaskID'      + Color.CEND,
                        Color.CGREEN2 + 'Taskname'    + Color.CEND,
                        Color.CGREEN2 + 'Queue'       + Color.CEND,
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

      for task in tasks:
        jobs = task.getAllJobs()
        if not list_all and (task.status == 'done'):
          continue
        total = count(jobs)
        id            = task.id
        taskName      = task.taskName
        queue         = task.queueName
        registered    = total[ Status.REGISTERED]
        assigned      = total[ Status.ASSIGNED  ] 
        testing       = total[ Status.TESTING   ] 
        running       = total[ Status.RUNNING   ] 
        done          = total[ Status.DONE      ] 
        failed        = total[ Status.FAILED    ] 
        kill          = total[ Status.KILL      ] 
        killed        = total[ Status.KILLED    ] 
        broken        = total[ Status.BROKEN    ] 
        status        = task.status
        t.add_row(  [task.id, taskName, queue, registered,  assigned, testing, running, failed,  done, kill, killed, broken, getStatus(status)] )
      return t


    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL, 'The username does not exist into the database.' )


    
    if interactive:
        from orchestra.utils import Clock
        from orchestra.maestro import NodeParser
        SECONDS=1.
        clock = Clock(10*SECONDS)
        nodes = NodeParser(self.__db)
        while True:
            if clock():
                import os
                _, t_nodes = nodes.list()
                t_tasks = get_table(username, list_all)
                os.system("clear")
                print(t_nodes)
                print(t_tasks)
    else:
        t = get_table(username, list_all)
        return (StatusCode.SUCCESS, t)






  def kill( self, task_id_list ):

    for id in task_id_list:
      try:
        # Get task by id
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (StatusCode.FATAL, "The task with id (%d) does not exist into the data base"%id )
        MSG_INFO( self, 'Kill task (%d) with name: %s', id, task.taskName)
        # Send kill signal to the task
        task.setSignal( Signal.KILL )
        self.__db.commit()
      except Exception as e:
        MSG_ERROR(self,e)
        return (StatusCode.FATAL, "Unknown error." )

    return (StatusCode.SUCCESS, "Succefully killed.")




  def retry( self, task_id_list ):

    for id in task_id_list:
      try:
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (StatusCode.FATAL, "The task with id (%d) does not exist into the data base"%id )
        
        if task.status == Status.DONE:
            return (StatusCode.FATAL, "The task with id (%d) is in DONE status. Can not retry."%id )

        MSG_INFO( self, 'Retry task (%d) with name: %s', id, task.taskName)
        task.setSignal( Signal.RETRY )
        self.__db.commit()
      except Exception as e:
        MSG_ERROR(self, e)
        return (StatusCode.FATAL, "Unknown error." )

    return (StatusCode.SUCCESS, "Succefully retry.")





  def queue( self , queuename ):

    t = PrettyTable([
                      Color.CGREEN2 + 'username'    + Color.CEND,
                      Color.CGREEN2 + 'Queue'       + Color.CEND,
                      Color.CGREEN2 + 'Taskname'    + Color.CEND,
                      Color.CGREEN2 + 'job'         + Color.CEND,
                      Color.CGREEN2 + 'priority'    + Color.CEND,
                      Color.CGREEN2 + 'Status'      + Color.CEND,
                      ])

    assigned_jobs = self.__db.session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
        Job.queueName==queuename) ).order_by(desc(Job.priority)).limit(10).with_for_update().all()
    assigned_jobs.reverse()

    running_jobs = self.__db.session().query(Job).filter(  and_( Job.status==Status.RUNNING ,
        Job.queueName==queuename) ).order_by(desc(Job.priority)).with_for_update().all()
    running_jobs.reverse()

    for job in running_jobs:
      t.add_row(  [job.getUserName(), job.getQueueName(), job.getTaskName(), job.configId, job.getPriority(), getStatus(job.status)] )
    for job in assigned_jobs:
      t.add_row(  [job.getUserName(), job.getQueueName(), job.getTaskName(), job.configId, job.getPriority(), getStatus(job.status)] )

    return (StatusCode.SUCCESS, t)







  #
  # Create the new task
  #
  def repro( self, volume,
                    new_taskname,
                    dataFile,
                    old_taskname,
                    secondaryDS,
                    execCommand,
                    queue='gpu',
                    dry_run=False):



    # check task policy (user.username)
    if new_taskname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The task name must starts with user.$USER.taskname.')

    # check task policy (username must exist into the database)
    username = new_taskname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers() ]:
      return (StatusCode.FATAL,'The username does not exist into the database.')

    if self.__db.getUser(username).getTask(new_taskname) is not None:
      return (StatusCode.FATAL, "The task exist into the database. Abort.")


    #
    # Check if all datasets are registered into the database
    #

    if self.__db.getDataset(username, dataFile) is None:
      return (StatusCode.FATAL, "The file (%s) does not exist into the database. Should be registry first."%dataFile)

    if self.__db.getUser(username).getTask(old_taskname) is None:
      return (StatusCode.FATAL, "The task file (%s) does not exist into the database."%old_taskname)


    secondaryDS = eval(secondaryDS)
    for key in secondaryDS.keys():
      if self.__db.getDataset(username, secondaryDS[key]) is None:
        return (StatusCode.FATAL , "The secondary data file (%s) does not exist into the database. Should be registry first."% secondaryDS[key] )


    #
    # check exec command policy
    #
    if (not '%DATA' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")

    if (not '%IN' in execCommand):
      return (StatusCode.FATAL,"The exec command must include '%IN' into the string. This will substitute to the configFile when start.")

    if not '%OUT' in execCommand:
      return (StatusCode.FATAL, "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")

    for key in secondaryDS.keys():
      if not key in execCommand:
        return (StatusCode.FATAL,  ("The exec command must include %s into the string. This will substitute to %s when start")%(key, secondaryDS[key]) )



    #
    # Create the output file
    #
    outputFile = volume +'/'+new_taskname

    if os.path.exists(outputFile):
      MSG_WARNING(self, "The task dir exist into the storage. Beware!")
    else:
      # create the task dir
      MSG_INFO(self, "Creating the task dir in %s", outputFile)
      os.system( 'mkdir -p %s '%(outputFile) )



    #
    # create the task into the database
    #
    if not dry_run:
      try:
        user = self.__db.getUser( username )


        task = self.__db.createTask( user, new_taskname, old_taskname, dataFile, outputFile, "",
                                     secondaryDataPath=secondaryDS,
                                     templateExecArgs=execCommand,
                                     queueName=queue)


        task.setSignal(Signal.WAITING)
        task.setStatus(Status.HOLD)


        tunedFiles =  expandFolders( self.__db.getUser(username).getTask(old_taskname).getTheOutputStoragePath() )

        _dataFile = self.__db.getDataset(username, dataFile).getAllFiles()[0].getPath()

        _secondaryDS = {}

        for key in secondaryDS.keys():
          _secondaryDS[key] = self.__db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()


        for idx, _tunedFile in enumerate(tunedFiles):

          _outputFile = outputFile+ '/job_configId_%d'%idx

          command = execCommand
          command = command.replace( '%DATA' , _dataFile  )
          command = command.replace( '%IN'   , _tunedFile)
          command = command.replace( '%OUT'  , _outputFile)

          for key in _secondaryDS:
            command = command.replace( key  , _secondaryDS[key])

          job = self.__db.createJob( task, _tunedFile, idx, execArgs=command, priority=-1 )

        task.setStatus('registered')
        self.__db.commit()
      except Exception as e:
        MSG_ERROR(self,e)
        return (StatusCode.FATAL, "Unknown error.")

    return (StatusCode.SUCCESS, "Succefully created.")





  def __test_job_locally( self, job ):

    from orchestra.Slots import CPUSlot
    from orchestra import Consumer
    slot = CPUSlot( "temp_slot")

    extra_envs = {'LOCAL_TEST':'1'}
    consumer = Consumer( job, slot, None, extra_envs=extra_envs )
    consumer.initialize()

    while True:
        if consumer.status() == Status.PENDING:
            if consumer.execute().isFailure():
                consumer.finalize()
                return False
        elif consumer.status() == Status.FAILED:
            consumer.finalize()
            return False
        elif consumer.status() == Status.RUNNING:
            continue
        elif consumer.status() == Status.DONE:
            consumer.finalize()
            return True
        else:
            continue

