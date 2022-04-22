
__all__ = ["TaskParser"]


from curses import OK
import glob, traceback, os, argparse

from orchestra.db import Task,Job
from orchestra.enums import State, Signal
from orchestra.utils import *

from sqlalchemy import and_, or_
from prettytable import PrettyTable
from tqdm import tqdm



from orchestra.utils import get_config
config = get_config()


def remove_extension(f, extensions="json|h5|pic|gz|tgz|csv"):
      for ext in extensions.split("|"):
        if f.endswith('.'+ext):
          return f.replace('.'+ext)
      return f

#
# Task parser
#
class TaskParser:


  def __init__(self , db, args=None):

    self.__db = db

    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('-v','--volume', action='store', dest='volume', required=True,
                          help = "The volume")
      create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                          help = "The task name to be append into the db.")
      create_parser.add_argument('-i','--inputfile', action='store',
                          dest='inputfile', required = True,
                          help = "The input config file that will be used to configure the job (sort and init).")
      create_parser.add_argument('--exec', action='store', dest='execCommand', required=True,
                          help = "The exec command")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      

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
      list_parser.add_argument('-a','--all', action='store_true', dest='all', required=False,
                    help = "List all tasks.")
      list_parser.add_argument('-i','--interactive', action='store_true', dest='interactive', required=False,
                    help = "List all tasks interactive mode.")


      retry_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be retry", type=int)
      retry_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      retry_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)


      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('--id', action='store', nargs='+', dest='id_list', required=False, default=None,
                    help = "All task ids to be removed", type=int)
      kill_parser.add_argument('--id_min', action='store',  dest='id_min', required=False,
                    help = "Down taks id limit to apply on the loop", type=int, default=None)
      kill_parser.add_argument('--id_max', action='store',  dest='id_max', required=False,
                    help = "Upper task id limit to apply on the loop", type=int, default=None)





      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry', parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('kill', parents=[kill_parser])
      args.add_parser( 'task', parents=[parent] )




  def compile( self, args ):


    def get_task_ids( _args ):
      if _args.id_list:
        ids = _args.id_list
      elif _args.id_min and _args.id_max:
        ids= list( range( _args.id_min, _args.id_max+1 ) )
      return ids

    # Task CLI
    if args.mode == 'task':

      # create task
      if args.option == 'create':
        ok , answer = self.create(args.volume,
                                  args.taskname,
                                  args.inputfile,
                                  args.execCommand,
                                  True,
                                  args.dry_run)

        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)


      # retry option
      elif args.option == 'retry':
        ok, answer = self.retry(get_task_ids(args))
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      # delete option
      elif args.option == 'delete':
        ok , answer = self.delete(get_task_ids(args), force=args.force)
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      # list all tasks
      elif args.option == 'list':
        ok, answer = self.list(args.all, args.interactive)
        if not ok:
          MSG_FATAL(answer)
        else:
          print(answer)

      # kill option
      elif args.option == 'kill':
        ok, answer = self.kill(get_task_ids(args))
        if not ok:
          MSG_FATAL(answer)
        else:
          MSG_INFO(answer)

      else:
        MSG_FATAL("option not available.")






  #
  # Create the new task
  #
  def create( self, volume,
                    taskname,
                    inputfile,
                    execCommand,
                    bypass=False,
                    dry_run=False,
                    ):

    if self.__db.task(taskname) is not None:
      return (False, "The task exist into the database. Abort.")

    if (not '%IN' in execCommand):
      return (False,"The exec command must include '%IN' into the string. This will substitute to the configFile when start.")

   
    output = volume +'/'+taskname


    
    if os.path.exists(output):
      MSG_WARNING("The task dir exist into the storage. Beware!")
    else:
      # create the task dir
      MSG_INFO("Creating the task dir in %s"%output)
      os.system( 'mkdir -p %s '%(output) )


    try:

      task       = self.__db.create_task( taskname, output )
      task.sig   = Signal.WAITING
      task.state = "hold"
  
      offset = self.__db.generateId(Job)
      inputfiles = glob.glob(inputfile+'/*', recursive=True)
      
      for idx, file in tqdm( enumerate(inputfiles) ,  desc= 'Creating... ', ncols=100):
        command = execCommand
        command = command.replace( '%IN'   , file)
        jobname = remove_extension( file.split('/')[-1] )
    
        job = self.__db.create_job( task, jobname, file, command, id=offset+idx )
        job.state = 'registered'

      task.state = 'registered'

      if test_job_locally( task.jobs[0] ):
        self.__db.session().add(task)
        self.__db.commit()
        return (True, "Succefully created.")

      else:
        return (False, "Local test failed.")
     
    except Exception as e:
      traceback.print_exc()
      return (False, "Unknown error.")





  def delete( self, task_id_list, remove=False, force=False ):


    for id in task_id_list:

      # Get task by id
      task = self.__db.session().query(Task).filter(Task.id==id).first()
      if not task:
        return (False, "The task with id (%d) does not exist into the data base"%id )
      
      # Check possible status before continue
      if not force:
        if not task.state in [Signal.BROKEN, Signal.KILLED, Signal.FINALIZED, Signal.DONE]:
          return (False, "The task with current status %s can not be deleted. The task must be in done, finalized, killed or broken Signal."% task.getStatus() )
      
      # remove all jobs that allow to this task
      try:
        self.__db.session().query(Job).filter(Job.taskid==id).delete()
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()

      # remove the task table
      try:
        self.__db.session().query(Task).filter(Task.id==id).delete()
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()


    return (True, "Succefully deleted.")





  def list( self, list_all=False, interactive=False):


    # helper function to print my large table
    def ptable( tasks, list_all ):
      t = PrettyTable([

                        'TaskID'    ,
                        'Taskname'  ,
                        'Registered',
                        'Assigned'  ,
                        'Testing'   ,
                        'Running'   ,
                        'Failed'    ,
                        'Done'      ,
                        'kill'      ,
                        'killed'    ,
                        'broken'    ,
                        'State'     ,
                        ])

      def count( jobs ):
        states = [State.REGISTERED, State.ASSIGNED, State.TESTING, 
                  State.RUNNING   , State.DONE    , State.FAILED, 
                  State.KILL      , State.KILLED  , State.BROKEN]
        total = { str(key):0 for key in states }
        for job in jobs:
          for s in states:
            if job.state==s: total[str(s)]+=1
        return total

      for task in tasks:
        jobs = task.jobs
        if not list_all and (task.state == State.DONE):
          continue
        total = count(jobs)
        registered    = total[ State.REGISTERED]
        assigned      = total[ State.ASSIGNED  ] 
        testing       = total[ State.TESTING   ] 
        running       = total[ State.RUNNING   ] 
        done          = total[ State.DONE      ] 
        failed        = total[ State.FAILED    ] 
        kill          = total[ State.KILL      ] 
        killed        = total[ State.KILLED    ] 
        broken        = total[ State.BROKEN    ] 
        state         = task.state

        t.add_row(  [task.id, task.taskname, registered,  assigned, 
                     testing, running, failed,  done, kill, killed, broken, 
                     state] )
      return t



    if interactive:
        from orchestra.Pilot import Clock
        from orchestra import SECOND
        import os
        clock = Clock(10*SECOND)
        while True:
            if clock():
                tasks = self.__db.tasks()

                os.system("clear")
                print(ptable(tasks, list_all))
    else:
        tasks = self.__db.tasks()
        t = ptable(tasks, list_all)
        return (True, t)






  def kill( self, task_id_list ):

    for id in task_id_list:
      try:
        # Get task by id
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (False, "The task with id (%d) does not exist into the data base"%id )
        # Send kill signal to the task
        task.signal = Signal.KILL
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully killed.")




  def retry( self, task_id_list ):

    for id in task_id_list:
      try:
        task = self.__db.session().query(Task).filter(Task.id==id).first()
        if not task:
            return (False, "The task with id (%d) does not exist into the data base"%id )
        
        if task.state == State.DONE:
            return (False, "The task with id (%d) is in DONE Signal. Can not retry."%id )

        task.signal = Signal.RETRY
        self.__db.commit()
      except Exception as e:
        traceback.print_exc()
        return (False, "Unknown error." )

    return (True, "Succefully retry.")









