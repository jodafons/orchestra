

__all__ = ["compile", "Schedule"]

from sqlalchemy import and_

from orchestra.enums import *
from orchestra.db import *
from orchestra.utils import *
import traceback




class Schedule:

  def __init__(self, db, postman, max_running_tasks=2):


    self.__db = db
    self.__postman = postman 
    self.__states = []
    self.__max_running_tasks = max_running_tasks


  def init(self):
    pass


  #
  # Add Transiction and state into the schedule machine
  #
  def transition( self, source, destination, trigger ):
    if type(trigger) is not list:
      trigger=[trigger]
    self.__states.append( (source, trigger, destination) )



  #
  # execute
  #
  def run(self):

    self.treat_running_jobs_not_alive()
    count = 0
    for task in self.__db.tasks():
      self.trigger(task)
      #if task.state == State.RUNNING:
      #  count+=1
      #if count>self.__max_running_tasks:
      #  break

    self.__db.commit()
    return True



  #
  # Execute the correct state machine for this task
  #
  def trigger(self, task):

    # Get the current state information
    current_state = task.state
    # Run all state triggers to find the correct transiction
    for source, triggers, destination in self.__states:
      # Check if the current state is equal than this state
      if source == current_state:
        passed = True
        # Execute all triggers into this state
        for trigger in triggers:
          passed = getattr(self, trigger)(task)
          if not passed:
            break
        if passed:
          task.state = destination
          break




  def get_jobs(self, njobs):

    try:
      jobs = self.__db.session().query(Job).filter(  Job.state==State.ASSIGNED  ).order_by(Job.id).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      traceback.print_exc()
      print(e)
      return []



  #
  # Get all running jobs into the job list
  #
  def get_all_running_jobs(self):
    try:
      return self.__db.session().query(Job).filter( and_( Job.state==State.RUNNING) ).with_for_update().all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  def treat_running_jobs_not_alive(self):
    print('treat running jobs...')
    jobs = self.get_all_running_jobs()
    for job in jobs:
      print(job.is_alive())
      if not job.is_alive():
        job.state = State.ASSIGNED


 


  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def broken_all_jobs( self, task ):

    try:
      for job in task.jobs:
        job.state = State.BROKEN
      task.signal = Signal.WAITING
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Retry all jobs after the user sent the retry signal to the task db
  #
  def retry_all_jobs( self, task ):

    try:
      if task.signal == Signal.RETRY:
        for job in task.jobs:
          job.state = State.REGISTERED
        task.signal = Signal.WAITING
        return True
      else:
        return False
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Retry all jobs with failed state after the user sent the retry signal to the task db
  #
  def retry_all_failed_jobs( self, task ):

    try:
      for job in task.jobs:
        if job.state == State.FAILED:
          if job.retry < 3:
            job.retry+=1
            job.state =  State.ASSIGNED
      task.signal = Signal.WAITING
      return True
     
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False




  #
  # Send kill state for all jobs after the user sent the kill singal to the task db
  #
  def kill_all_jobs( self, task ):

    try:
      if task.signal == Signal.KILL:
        for job in task.jobs:
          if job.state != State.RUNNING:
            job.state =  State.KILLED
          else:
            job.state = State.KILL
        task.signal = Signal.WAITING
        return True
      else:
        return False
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False




  #
  # Check if all jobs into this task were killed
  #
  def all_jobs_were_killed( self, task ):

    try:
      total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
      if ( len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.KILLED ) ).all()) == total ):
        return True
      else:
        return False
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if the test job is completed
  #
  def test_job_pass( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.DONE:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if the test job still running
  #
  def test_job_still_running( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.RUNNING:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False


  #
  # Check if the test job fail
  #
  def test_job_fail( self, task ):

    try:
      # Get the first job from the list of jobs into this task
      job = task.jobs[0]
      if job.state == State.FAILED or job.state == State.BROKEN:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if all jobs into this taks is in registered state
  #
  def all_jobs_are_registered( self, task ):

    try:
      total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
      if len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.REGISTERED ) ).all()) == total:
        return True
      else:
        return False
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Assigned the first job in the list to test
  #
  def assigned_one_job_to_test( self, task ):

    try:
      job = task.jobs[0]
      job.state =  State.ASSIGNED
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Assigned all jobs
  #
  def assigned_all_jobs( self, task ):

    try:
      for job in task.jobs:
        if job.state != State.DONE:
          job.state =  State.ASSIGNED
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if all jobs into this task are in done state
  def all_jobs_are_done( self, task ):

    try:
      total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
      if len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.DONE ) ).all()) == total:
        return True
      else:
        return False
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



  #
  # Check if all jobs into this task ran
  #
  def all_jobs_ran( self, task ):

    try:                                                                                                                                                                 
      total = len(self.__db.session().query(Job).filter( Job.taskid==task.id ).all())
      total_done = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.DONE ) ).all())
      total_failed = len(self.__db.session().query(Job).filter( and_ ( Job.taskid==task.id, Job.state==State.FAILED ) ).all())

      if (total_done + total_failed) == total:
        return True
      else:
        return False
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False


  #
  # Check if all jobs into this task ran
  #
  def check_not_allow_job_state_in_running_state( self, task ):

    try:
      exist_registered_jobs = False
      for job in task.jobs:
        if job.state==State.REGISTERED or job.state==State.PENDING: 
          job.state = State.ASSIGNED
          exist_registered_jobs=True

      return exist_registered_jobs
    except Exception as e:

      MSG_ERROR( "Exception raise in state %s for this task %s"%(task.state, task.taskname) )
      return False



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

    task.signal =Signal.DELETE
    return True


  #
  # Assigned task to removed state and remove all objects from the database and store
  #
  def remove_this_task(self, task):

    if task.signal == Signal.DELETE:
      try:
        from orchestra.parsers import TaskParser
        helper = TaskParser(self.__db)
        helper.delete(task.taskname,False)
        return True
      except Exception as e:
        task.signal =Signal.WAITING
        task.state = State.REMOVED
        MSG_ERROR(self, e)
        MSG_ERROR(self, "It's not possible to delete this task with name %s", task.taskname)
        return False
    else:
      return False




  #
  #
  # Email notification
  #
  #


  def send_email_task_done( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with DONE State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_broken( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("Your task with name %s was set to BROKEN State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_finalized( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with FINALIZED State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True


  def send_email_task_killed( self, task ):

    try:
      subject = ("[LPS Cluster] Notification for task id %d")%(task.id)
      message = ("The task with name %s was assigned with KILLED State.")%(task.taskname)
      self.__postman.send(subject, message)
      return True
    except Exception as e:
      traceback.print_exc()
      MSG_ERROR(self, "It's not possible to send the email to %s", self.__postman.email)
      return True



 
#
# Compile the state machine
#
def compile(schedule):

  # Create the state machine
  #schedule.transition( source=State.REGISTERED, destination=State.TESTING    , trigger=['all_jobs_are_registered', 'assigned_one_job_to_test']         )
  schedule.transition( source=State.REGISTERED, destination=State.TESTING    , trigger=['all_jobs_are_registered']         )
  #schedule.transition( source=State.TESTING   , destination=State.TESTING    , trigger='test_job_still_running'                                        )
  #schedule.transition( source=State.TESTING   , destination=State.BROKEN     , trigger=['test_job_fail','broken_all_jobs','send_email_task_broken']    )
  #schedule.transition( source=State.TESTING   , destination=State.RUNNING    , trigger=['test_job_pass','assigned_all_jobs']                           )
  schedule.transition( source=State.TESTING   , destination=State.RUNNING    , trigger=['assigned_all_jobs']                                           )
  schedule.transition( source=State.BROKEN    , destination=State.REGISTERED , trigger='retry_all_jobs'                                                )
  schedule.transition( source=State.RUNNING   , destination=State.DONE       , trigger=['all_jobs_are_done', 'send_email_task_done']                   )
  schedule.transition( source=State.RUNNING   , destination=State.FINALIZED  , trigger=['all_jobs_ran','send_email_task_finalized']                    )
  schedule.transition( source=State.RUNNING   , destination=State.KILL       , trigger='kill_all_jobs'                                                 )
  schedule.transition( source=State.RUNNING   , destination=State.RUNNING    , trigger='check_not_allow_job_state_in_running_state'                    )
  schedule.transition( source=State.FINALIZED , destination=State.RUNNING    , trigger='retry_all_failed_jobs'                                         )
  schedule.transition( source=State.KILL      , destination=State.KILLED     , trigger=['all_jobs_were_killed','send_email_task_killed']               )
  schedule.transition( source=State.KILLED    , destination=State.REGISTERED , trigger='retry_all_jobs'                                                )
  schedule.transition( source=State.DONE      , destination=State.REGISTERED , trigger='retry_all_jobs'                                                )

