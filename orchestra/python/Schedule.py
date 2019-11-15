
__all__ = ["Schedule"]


from Gaugi import Logger, NotSet, Color
from Gaugi.messenger.macros import *
from Gaugi import StatusCode
import time
from sqlalchemy import and_, or_
from orchestra.enumerations import *
from orchestra.db.models import *
from orchestra.utilities import Clock
from orchestra.constants import MAX_UPDATE_TIME, MAX_TEST_JOBS, MAX_FAILED_JOBS, MIN_SUCCESS_JOBS, CLUSTER_NAME



class Schedule(Logger):

  def __init__(self, name, rules):

    Logger.__init__(self, name=name)
    self.__rules = rules
    self.__clock = Clock(MAX_UPDATE_TIME)

  def initialize(self):
    return StatusCode.SUCCESS


  def setDatabase(self, db):
    self._db = db


  def db(self):
    return self._db


  def calculate(self):


    for user in self.db().getAllUsers():

      MSG_INFO(self, Color.CWHITE2 + "<<< Calculating the job priority for %s >>>" + Color.CEND, user.username )

      # Get the initial priority of the user
      maxPriority = user.getMaxPriority()
      # Get the number of tasks
      tasks = user.getAllTasks(CLUSTER_NAME)

      for task in tasks:

        MSG_INFO(self, "Looking for task name: %s", task.taskName )

        MSG_INFO(self, "The current Task has status: " + Color.CGREEN2 + "[" + task.getStatus() + "]" + Color.CEND)

        # Get the task status (REGISTED, TESTING, RUNNING, BROKEN, DONE)
        if task.getStatus() == Status.REGISTERED:

          MSG_INFO(self, "Setting some jobs to the 'testing' phase...")
          # We need to check if this is a good task to proceed.
          # To test, we will launch 10 first jobs and if 80%
          # of jobs is DONE, we will change the task status to
          # RUNNING. Until than, all not choosed jobs will have
          # priority equal zero and the 10 ones will be equal
          # USER max priority (1000,2000,..., N)
          self.setJobsToBeTested( user, task )
          # change the task status to: REGISTED to TESTING.
          task.setStatus( Status.TESTING )
          self.db().commit()

        # Check if this is a test
        elif task.getStatus() == Status.TESTING:

          # Check if we can change the task status to RUNNING or BROKEN.
          # If not, this will still TESTING until we decide each signal
          # will be assigned to this task
          MSG_INFO(self, "Waiting for the testing jobs...")
          self.checkTask( task )

        elif task.getStatus() == Status.RUNNING:

          # If this task was assigned as RUNNING, we must recalculate
          # the priority of all jobs inside of this task.
          self.checkTask( task )

        else: # HOLDED, DONE, BROKEN or FAILED Status
          continue

      self.calculatePriorities( user )

    return StatusCode.SUCCESS





  def execute(self):
    # Calculate the priority for every N minute
    if self.__clock():
      self.calculate()


  def finalize(self):
    self.getContext().finalize()


  def setJobsToBeTested( self, user, task ):

    # By default, I will send one job for each task.
    # if this job has status done, i will assigned all
    # jobs into the task, if not, all of then will be broken
    priority = user.getMaxPriority( )
    jobs = task.getAllJobs()
    njobs = len(jobs)
    max_test_jobs=njobs if njobs < MAX_TEST_JOBS else MAX_TEST_JOBS
    jobCount = 0
    while (jobCount < MAX_TEST_JOBS):
      try:
        MSG_INFO(self, "Including one job to the testing phase...")
        jobs[jobCount].setPriority(priority)
        jobs[jobCount].setStatus(Status.ASSIGNED)
        jobCount+=1
        self.db().commit()
      except Exception as e:
        MSG_ERROR(self, e)
        break


  def checkTask( self, task ):


    if task.getStatus() == Status.TESTING:
      # Maybe we need to checge these rules
      if len(self.db().session().query(Job).filter( and_( or_( Job.status==Status.FAILED,  Job.status==Status.BROKEN), Job.taskId==task.id )).all()) == MAX_FAILED_JOBS:
        task.setStatus( Status.BROKEN )
        MSG_INFO(self, "The current task will be signoff with status: " + Color.CRED2 + "[BROKEN]" + Color.CEND)
        # kill all jobs into the task assigned as broken status
        self.closeAllJobs( task )
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.DONE, Job.taskId==task.id )).all()) == MIN_SUCCESS_JOBS:
        MSG_INFO(self,"The test was completed with status: " + Color.CWHITE2 + "[RUNNING]" + Color.CEND)
        task.setStatus( Status.RUNNING )
        self.db().commit()
        self.assignedAllJobs(task)
      else:
        MSG_INFO( self, "still stesting....")
        #task.setStatus( Status.TESTING )

    elif task.getStatus() == Status.RUNNING:

      # The task is running. Here, we will check if its completed.
      if len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED, Job.taskId==task.id )).all()) > 0:
        MSG_INFO(self, "The task still with assigned jobs inside of the task list. Still with running status...")

      # Here, we have zero assigned jobs. But we can have running jobs inside of the jobs list
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING, Job.taskId==task.id )).all()) > 0:
        MSG_INFO(self, "We don't have any assigned jobs any more. But the task still with running jobs inside of the task list. Still with running status...")

      # Here, we have zero assigned/running jobs. Now, we will decide if the task is done (100% done jobs) or finalized (failed jobs > zero)
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED, Job.taskId==task.id )).all()) > 0:
        MSG_INFO( self, "The task is completed since we don't have any assigned/running jobs inside of the task list" )
        MSG_INFO( self, "The task will receive the finalized status since we have more than zero jobs with status as failed.")
        task.setStatus( Status.FINALIZED )

      else: # All jobs were completed with done status
        MSG_INFO( self, "The task is completed since we don't have any assigned/running jobs inside of the task list" )
        MSG_INFO(self,"The task was completed with status: " + Color.CBLUE2 + "[DONE]" + Color.CEND)
        task.setStatus( Status.DONE )



  def calculatePriorities( self, user):
    # The rules will be an external class with Rule as inheritance.
    # These rules can be changed depends on the demand.
    return self.__rules( self.db(), user)



  def setUpdateTime( self, t ):
    self.__maxUpdateTime = t



  def closeAllJobs(self, task):
    for job in task.getAllJobs():
      job.setStatus( Status.BROKEN )
      job.setPriority( -1 )


  def assignedAllJobs( self, task ):
    MSG_INFO(self, "Assigning all jobs into the current task...")
    for job in task.getAllJobs():
      if job.getStatus() == Status.REGISTERED:
        job.setStatus( Status.ASSIGNED )
        self.db().commit()



  def getCPUQueue( self ):
    try:
      jobs = self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED , Job.isGPU==False, Job.cluster==CLUSTER_NAME) ).order_by(Job.priority).all()
      jobs.reverse()
      return jobs
      #return []
    except Exception as e:
      MSG_ERROR(self,e)
      return []



  def getGPUQueue( self ):
    try:
      return self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED , Job.isGPU==True, Job.cluster==CLUSTER_NAME) ).order_by(Job.priority).all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  def getAllRunningJobs(self):
    try:

      MSG_INFO(self, "Getting all jobs with status: " + Color.CGREEN2+"[RUNNING]" + Color.CEND)
      return self.db().session().query(Job).filter( and_( Job.cluster==CLUSTER_NAME , Job.status==Status.RUNNING) ).all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []


