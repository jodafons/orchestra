
__all__ = ["Schedule"]


from Gaugi import Logger, NotSet, Color
from Gaugi.messenger.macros import *
from Gaugi import StatusCode
import time
from sqlalchemy import and_, or_
from sqlalchemy import desc
from orchestra.enumerations import *
from orchestra.db.models import *
from orchestra.utilities import Clock
from orchestra.constants import MAX_UPDATE_TIME, MAX_TEST_JOBS, MAX_FAILED_JOBS, MIN_SUCCESS_JOBS


class Schedule(Logger):

  def __init__(self, name, rules, cluster=Cluster.LPS):

    Logger.__init__(self, name=name)
    self.__rules = rules
    self.__cluster = cluster
    self.__states = []


  def setCluster( self, cluster ):
    self.__cluster = cluster


  def initialize(self):
    return StatusCode.SUCCESS


  def setDatabase(self, db):
    self._db = db


  def setPostman(self, postman):
    self.__postman = postman


  def db(self):
    return self._db


  def postman(self):
    return self.__postman


  #
  # Calculate the priority and the next task state
  #
  def calculate(self):

    start = time.time()

    MSG_INFO(self, "===============================================")
    for user in self.db().getAllUsers():

      MSG_INFO(self, "Calculate the job priority for %s",  user.username )

      # Get the initial priority of the user
      maxPriority = user.getMaxPriority()
      # Get the number of tasks
      tasks = user.getAllTasks( self.__cluster  )

      for task in tasks:
        MSG_INFO(self, "TaskID %d with current status: " + Color.CGREEN2 + task.getStatus() + Color.CEND, task.id)
        # Run the state transictions
        self.run(task)

      self.calculatePriorities( user )
    MSG_INFO(self, "===============================================")

    end = time.time()
    MSG_INFO( self, "Schedule loop calculated in %1.2fs", end-start )

    return StatusCode.SUCCESS


  #
  # Execute the schedule
  #
  def execute(self):
    self.calculate()
    return StatusCode.SUCCESS


  #
  # Finalize the schedule
  #
  def finalize(self):
    return StatusCode.SUCCESS


  #
  # Calculate all priorities
  #
  def calculatePriorities( self, user):
    # The rules will be an external class with Rule as inheritance.
    # These rules can be changed depends on the demand.
    return self.__rules( self.db(), user)



  #
  # Get the list of jobs ordered by the priority for CPU
  #
  def getCPUQueue( self, njobs ):
    try:
      jobs = self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
        Job.isGPU==False, Job.cluster==self.__cluster) ).order_by(desc(Job.priority)).limit(njobs).with_for_update().all()
      jobs.reverse()
      print(jobs)
      #for j in jobs:
      #  print(j.getPriority() )

      return jobs
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  #
  # Get the list of jobs ordered by the priority for GPU
  #
  def getGPUQueue( self, njobs ):
    try:
      jobs = self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
             Job.isGPU==True, Job.cluster==self.__cluster) ).order_by(desc(Job.priority)).limit(njobs).with_for_update().all()
      #jobs.reverse()
      return jobs
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  #
  # Get all running jobs into the job list
  #
  def getAllRunningJobs(self):
    try:

      MSG_INFO(self, "Getting all jobs with status: " + Color.CGREEN2+"[RUNNING]" + Color.CEND)
      return self.db().session().query(Job).filter( and_( Job.cluster==self.__cluster , Job.status==Status.RUNNING) ).with_for_update().all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []




  #
  # Transictions functions
  #



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
          MSG_INFO(self, trigger +  ' = ' + str(passed) )
          if not passed:
            break

        if passed:
          task.setStatus( destination )
          break



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
      priority = user.getMaxPriority()

      if task.getSignal() == Signal.RETRY:
        for job in task.getAllJobs():
          if job.getStatus() == Status.FAILED:
            job.setPriority(priority)
            job.setStatus( Status.ASSIGNED )
        task.setSignal( Signal.WAITING )
        return True
      else:
        return False
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
      if job.getStatus() == Status.FAILED:
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
      priority = user.getMaxPriority()
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
      message = ("The task with name %s was assigned with DONE status and will be removed in ten days. Please download all outputs using maestro.")%(task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)
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
      message = ("Your task with name %s was set to BROKEN and will be removed in ten days if you don't reach it or retry it.")%(task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)
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
      message = ("The task with name %s was assigned with FINALIZED status and will be removed in ten days if you not ping it or retry.")%(task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)
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
      message = ("The task with name %s was assigned with KILLED status and will be removed in ten days if you not ping it or retry.")%(task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)
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
  # Check if task is 5 days old when start the timer
  #
  def passed_five_days(self, task):

    import datetime
    five_days = datetime.timedelta(days=5)
    #five_days = datetime.timedelta(seconds=30)
    if task.getTimer() > five_days:
      return True
    else:
      return False



  #
  # Check if task is 10 days old when start the timer
  #
  def passed_nine_days(self, task):

    import datetime
    nine_days = datetime.timedelta(days=9)
    #nine_days = datetime.timedelta(seconds=60)
    if task.getTimer() > nine_days:
      return True
    else:
      return False



  #
  # Check if task is 10 days old when start the timer
  #
  def passed_ten_days(self, task):

    import datetime
    ten_days = datetime.timedelta(days=10)
    #ten_days = datetime.timedelta(seconds=90)
    if task.getTimer() > ten_days:
      return True
    else:
      return False


  #
  # Set delete signal
  #
  def send_delete_signal(self, task):

    task.setSignal(Signal.DELETE)
    return True


  #
  # Notify the user about the delete procedure: 5 days remain
  #
  def send_email_this_task_will_be_removed_in_five_days( self, task ):
    try:
      subject = ("[LPS Cluster] (WARNING) Notification for taskID %d")%(task.id)
      message = ("The task with name %s was assigned with %s status and will be removed in five days if you not ping it.")%(task.getStatus(),task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)

      return True
    except:

      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True


  #
  # Assigned this task to be removed state
  #
  def assigned_task_to_be_removed( self, task ):

    task.setStatus(Status.TO_BE_REMOVED)
    return True


  #
  # Notify the user about the delete procedure: 1 day remain
  #
  def send_email_this_task_will_be_removed_tomorrow( self, task ):
    try:
      subject = ("[LPS Cluster] (WARNING) Notification for taskID %d")%(task.id)
      message = ("The task with name %s was assigned with %s status and will be removed in 24 hours if you not ping it")%(task.getStatus(),task.taskName)
      self.__postman.sendNotification(task.getUser().getUserName(), subject, message)

      task.setStatus(Status.TO_BE_REMOVED_SOON)
      return True
    except:

      MSG_ERROR(self, "It's not possible to send the email to the username %s", task.getUser().getUserName())
      return True


  #
  # Assigned this task to be removed soon state
  #
  def assigned_task_to_be_removed_soon(self , task):

    task.setStatus(Status.TO_BE_REMOVED_SOON)
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












