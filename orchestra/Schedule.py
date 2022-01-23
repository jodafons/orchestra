
__all__ = ["schedule"]


from Gaugi import Logger, StatusCode
from Gaugi.macros import *

from sqlalchemy import and_, or_, desc

from orchestra.enums import *
from orchestra.db.models import *

import time


#
# Schedule
#
class Schedule(Logger):

  #
  # Constructor
  #
  def __init__(self):
    Logger.__init__(self)
    self.__states = []


  def setDatabase(self, db):
    self.__db = db


  def setPostman(self, postman):
    self.__postman = postman


  def db(self):
    return self.__db


  def postman(self):
    return self.__postman


  #
  # initialize
  #
  def initialize(self):
    return StatusCode.SUCCESS


  #
  # execute
  #
  def execute(self):

    self.treatRunningJobsNotAlive()
    self.calculate()
    self.db().commit()
    return StatusCode.SUCCESS


  #
  # finalize
  #
  def finalize(self):
    return StatusCode.SUCCESS


  #
  # run state machine for eacj task
  #
  def calculate(self):

    for user in self.db().getAllUsers():
      for task in user.getAllTasks():
        self.run(task)
    return StatusCode.SUCCESS


  #
  # Get the list of jobs ordered by the priority for CPU
  #
  def getQueue( self, njobs , queuename):
    try:
      #jobs = self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
      #  Job.queueName==queuename) ).order_by(Job.id).limit(njobs).with_for_update().all()
 
      jobs = self.db().session().query(Job).filter(  Job.status==Status.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()


      jobs.reverse()
      return jobs
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  #
  # Get all running jobs into the job list
  #
  def getAllRunningJobs(self):
    try:
      return self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING) ).with_for_update().all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []




  #
  # Execute the correct state machine for this task
  #
  def run(self, task):

    # Get the current state information
    current_state = task.getStatus()
    # Run all state triggers to find the correct transiction
    for source, triggers, destination in self.__states:

      # Check if the current state is equal than this state
      if source == current_state:
        passed = True
        MSG_INFO( self,  "Current status is: %s"%current_state )
        # Execute all triggers into this state
        for trigger in triggers:
          passed = getattr(self, trigger)(task)
          #MSG_INFO(self, trigger +  ' = ' + str(passed) )
          if not passed:
            break
        if passed:
          task.setStatus( destination )
          break


  def treatRunningJobsNotAlive(self):

    jobs = self.getAllRunningJobs()
    for job in jobs:
      if not job.isAlive():
        job.setStatus( Status.ASSIGNED )



  #
  # Add Transiction and state into the schedule machine
  #
  def add_transiction( self, source, destination, trigger ):
    if type(trigger) is not list:
      trigger=[trigger]
    self.__states.append( (source, trigger, destination) )




  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def broken_all_jobs( self, task ):

    try:
      for job in task.getAllJobs():
        job.setStatus( Status.BROKEN )
      task.setSignal( Signal.WAITING )
      return True
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def retry_all_jobs( self, task ):

    try:
      if task.getSignal() == Signal.RETRY:
        for job in task.getAllJobs():
          job.setStatus( Status.REGISTERED )
        task.setSignal( Signal.WAITING )
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Retry all jobs with failed status after the user sent the retry signal to the task db
  #
  def retry_all_failed_jobs( self, task ):

    try:
      user = task.getUser()
      for job in task.getAllJobs():
        if job.getStatus() != Status.DONE:
          job.setPriority(1000)
          job.setStatus( Status.ASSIGNED )
      task.setSignal( Signal.WAITING )
      return True
      #if task.getSignal() == Signal.RETRY:
      #  for job in task.getAllJobs():
      #    if job.getStatus() == Status.FAILED:
      #      job.setPriority(1000)
      #      job.setStatus( Status.ASSIGNED )
      #  task.setSignal( Signal.WAITING )
      #  return True
      #else:
      #  return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False




  #
  # Send kill status for all jobs after the user sent the kill singal to the task db
  #
  def kill_all_jobs( self, task ):

    try:
      if task.getSignal() == Signal.KILL:
        for job in task.getAllJobs():
          if job.getStatus() != Status.RUNNING:
            job.setStatus( Status.KILLED )
          else:
            job.setStatus( Status.KILL )
        task.setSignal( Signal.WAITING )
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False




  #
  # Check if all jobs into this task were killed
  #
  def all_jobs_were_killed( self, task ):

    try:
      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
      if ( len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.KILLED ) ).all()) == total ):
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Check if the test job is completed
  #
  def test_job_pass( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.getAllJobs()[0]
      if job.getStatus() == Status.DONE:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Check if the test job still running
  #
  def test_job_still_running( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.getAllJobs()[0]
      if job.getStatus() == Status.RUNNING:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False


  #
  # Check if the test job fail
  #
  def test_job_fail( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.getAllJobs()[0]
      if job.getStatus() == Status.FAILED or job.getStatus() == Status.BROKEN:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Check if all jobs into this taks is in registered status
  #
  def all_jobs_are_registered( self, task ):

    try:
      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
      if len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.REGISTERED ) ).all()) == total:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Assigned the first job in the list to test
  #
  def assigned_one_job_to_test( self, task ):

    try:
      # Get the user from the task
      user = task.getUser()
      priority = 1000
      # Get the first job from the list of jobs into this task
      job = task.getAllJobs()[0]
      job.setPriority( priority )
      job.setStatus( Status.ASSIGNED )
      return True
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Assigned all jobs
  #
  def assigned_all_jobs( self, task ):

    try:
      for job in task.getAllJobs():
        if job.getStatus() != Status.DONE:
          job.setPriority(-1)
          job.setStatus( Status.ASSIGNED )
      return True
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Check if all jobs into this task are in done status
  def all_jobs_are_done( self, task ):

    try:
      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
      if len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.DONE ) ).all()) == total:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Check if all jobs into this task ran
  #
  def all_jobs_ran( self, task ):

    try:
      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
      total_done = len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.DONE ) ).all())
      total_failed = len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.FAILED ) ).all())

      if (total_done + total_failed) == total:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False


  #
  # Check if all jobs into this task ran
  #
  def check_not_allow_job_status_in_running_state( self, task ):

    try:
      exist_registered_jobs = False
      for job in task.getAllJobs():
        if job.getStatus()==Status.REGISTERED:
          job.setStatus(Status.ASSIGNED)
          exist_registered_jobs=True
      return exist_registered_jobs
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s :",task.getStatus(), task.taskName, e )
      return False



  #
  # Notify the user
  #
  def send_email_task_done( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for taskID %d")%(task.id)
      message = ("The task with name %s was assigned with DONE status.")%(task.taskName)
      self.__postman.send(task.getUser().email, subject, message)
      return True
    except Exception as e:
      MSG_ERROR(self, e)
      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True



  #
  # Notify the user
  #
  def send_email_task_broken( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for taskID %d")%(task.id)
      message = ("Your task with name %s was set to BROKEN status.")%(task.taskName)
      self.__postman.send(task.getUser().email, subject, message)
      return True
    except:

      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True


  #
  # Notify the user
  #
  def send_email_task_finalized( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for taskID %d")%(task.id)
      message = ("The task with name %s was assigned with FINALIZED status.")%(task.taskName)
      self.__postman.send(task.getUser().email, subject, message)
      return True
    except:

      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True



  #
  # Notify the user
  #
  def send_email_task_killed( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for taskID %d")%(task.id)
      message = ("The task with name %s was assigned with KILLED status.")%(task.taskName)
      self.__postman.send(task.getUser().email, subject, message)
      return True
    except:

      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True


  #
  # Set the timer
  #
  def start_timer(self, task):
    task.startTimer()
    return True




  #
  # Set delete signal
  #
  def send_delete_signal(self, task):

    task.setSignal(Signal.DELETE)
    return True


  #
  # Assigned task to removed state and remove all objects from the database and store
  #
  def remove_this_task(self, task):

    if task.getSignal() == Signal.DELETE:
      try:
        from orchestra.maestro.parsers import TaskParser
        helper = TaskParser(self.db())
        helper.delete(task.taskName,False)
        return True
      except Exception as e:
        task.setSignal(Signal.WAITING)
        task.setStatus(Status.REMOVED)
        MSG_ERROR(self, e)
        MSG_ERROR(self, "It's not possible to delete this task with name %s", task.taskName)
        return False
    else:
      return False






#
# Create the schedule
#
schedule = Schedule()

# Create the state machine
schedule.add_transiction( source=Status.REGISTERED, destination=Status.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
schedule.add_transiction( source=Status.TESTING   , destination=Status.TESTING    , trigger='test_job_still_running'                                        )
schedule.add_transiction( source=Status.TESTING   , destination=Status.BROKEN     , trigger=['test_job_fail','broken_all_jobs','send_email_task_broken']    )
schedule.add_transiction( source=Status.BROKEN    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )
schedule.add_transiction( source=Status.TESTING   , destination=Status.RUNNING    , trigger=['test_job_pass','assigned_all_jobs']                           )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.DONE       , trigger=['all_jobs_are_done', 'send_email_task_done']                   )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.FINALIZED  , trigger=['all_jobs_ran','send_email_task_finalized']                    )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.KILL       , trigger='kill_all_jobs'                                                 )
schedule.add_transiction( source=Status.RUNNING   , destination=Status.RUNNING    , trigger='check_not_allow_job_status_in_running_state'                   )
schedule.add_transiction( source=Status.FINALIZED , destination=Status.RUNNING    , trigger='retry_all_failed_jobs'                                         )
schedule.add_transiction( source=Status.KILL      , destination=Status.KILLED     , trigger=['all_jobs_were_killed','send_email_task_killed']               )
schedule.add_transiction( source=Status.KILLED    , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )
schedule.add_transiction( source=Status.DONE      , destination=Status.REGISTERED , trigger='retry_all_jobs'                                                )

