
__all__ = ["Schedule"]


from Gaugi import Logger, NotSet, Color
from Gaugi.messenger.macros import *
from Gaugi import StatusCode
import time
from sqlalchemy import and_, or_
from orchestra.enumerations import *
from orchestra.db.models import *
from orchestra.utilities import Clock
from orchestra.constants import MAX_UPDATE_TIME, MAX_TEST_JOBS, MAX_FAILED_JOBS, MIN_SUCCESS_JOBS
from orchestra import Postman

class Schedule(Logger):

  def __init__(self, name, rules, cluster=Cluster.LPS, max_update_time=MAX_UPDATE_TIME):

    Logger.__init__(self, name=name)
    self.__rules = rules
    self.__clock = Clock(max_update_time)
    self.__cluster = cluster
    try:
      self.__postman = Postman()
    except:
      pass


  def setCluster( self, cluster ):
    self.__cluster = cluster


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
      tasks = user.getAllTasks( self.__cluster  )

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


        elif task.getStatus() == Status.KILL:

          total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
          if len(self.db().session().query(Job).filter( and_( Job.status==Status.KILLED, Job.taskId==task.id )).all()) == total:
            try:
              self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.KILLED)
            except AttributeError:
              MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
            except:
              MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
            task.setStatus( Status.KILLED )



        else: # HOLDED, DONE, BROKEN, FAILED or KILLED Status
          continue

      self.calculatePriorities( user )

    return StatusCode.SUCCESS





  def execute(self):
    # Calculate the priority for every N minute
    if self.__clock():
      self.calculate()
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS


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

      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())

      # If the number of jobs is less than the MIN jobs
      if len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.FAILED ) ).all()) == total:
        MSG_INFO( self, "Job will be assigned as BROKEN status becouse we have large failed jobs." )
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.BROKEN)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
        task.setStatus( Status.BROKEN )

      elif len(self.db().session().query(Job).filter( and_ ( Job.taskId==task.id, Job.status==Status.BROKEN ) ).all()) == total:
        MSG_INFO( self, "Job will be assigned as BROKEN status" )
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.BROKEN)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
        task.setStatus( Status.BROKEN )


      # If the number of killed jobs is equal than the number of jobs, than the task will be assgined as killed
      # running to killed only if all jobs was assigned as killed
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.KILLED, Job.taskId==task.id )).all()) == total:
        MSG_INFO( self, "Job will be assigned as KILLED status" )
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.KILLED)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
        task.setStatus( Status.KILLED )
      # If there are any job with KILL status, so this task is in kill process
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.KILL, Job.taskId==task.id )).all()) > 0:
        MSG_INFO( self, "Job will be assigned as KILL status" )
        task.setStatus( Status.KILL )


      # Here, the number of jobs is higher than the minimal, apply the job pass condition (MAX_FAILED and MIN_SUCCESS)

      elif len(self.db().session().query(Job).filter( and_( or_( Job.status==Status.FAILED,  Job.status==Status.BROKEN), Job.taskId==task.id )).all()) == MAX_FAILED_JOBS:
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.BROKEN)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
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


    elif task.getStatus() == Status.RUNNING:

      total = len(self.db().session().query(Job).filter( Job.taskId==task.id ).all())
      # (Check KILL process) If there are any job with KILL status, so this task is in kill process.
      if len(self.db().session().query(Job).filter( and_( Job.status==Status.KILL, Job.taskId==task.id )).all()) > 0:
        task.setStatus( Status.KILL )


      # (BUG: Check KILLED process) If for some reason we got killed for all jobs into the running status becouse a turn-off, we
      # need to put as KILLED.
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.KILLED, Job.taskId==task.id )).all()) == total:
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.KILLED)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
        task.setStatus( Status.KILLED )



      # The task is running. Here, we will check if its completed.
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED, Job.taskId==task.id )).all()) > 0:
        MSG_INFO(self, "The task still with assigned jobs inside of the task list. Still with running status...")

      # Here, we have zero assigned jobs. But we can have running jobs inside of the jobs list
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING, Job.taskId==task.id )).all()) > 0:
        MSG_INFO(self, "We don't have any assigned jobs any more. But the task still with running jobs inside of the task list. Still with running status...")


      # Here, we have zero assigned/running jobs. Now, we will decide if the task is done (100% done jobs) or finalized (failed jobs > zero)
      elif len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED, Job.taskId==task.id )).all()) > 0:
        MSG_INFO( self, "The task is completed since we don't have any assigned/running jobs inside of the task list" )
        MSG_INFO( self, "The task will receive the finalized status since we have more than zero jobs with status as failed.")
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.FINALIZED)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
        task.setStatus( Status.FINALIZED )


      else: # All jobs were completed with done status
        MSG_INFO( self, "The task is completed since we don't have any assigned/running jobs inside of the task list" )
        MSG_INFO(self,"The task was completed with status: " + Color.CBLUE2 + "[DONE]" + Color.CEND)
        try:
          self.__postman.sendNotification(task.getUser().getUserName(), task.getTaskName(), task.getStatus(), Status.DONE)
        except AttributeError:
          MSG_ERROR(self, "Failed to send e-mail, Postman is not initialized!")
        except:
          MSG_ERROR(self, "Failed to send e-mail, unknown error with Postman!")
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


  def getCPUQueue( self, njobs ):
    try:
      jobs = self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
        Job.isGPU==False, Job.cluster==self.__cluster) ).order_by(Job.priority).limit(njobs).with_for_update().all()
      jobs.reverse()
      return jobs
    except Exception as e:
      MSG_ERROR(self,e)
      return []



  def getGPUQueue( self, njobs ):
    try:
      return self.db().session().query(Job).filter(  and_( Job.status==Status.ASSIGNED ,
        Job.isGPU==True, Job.cluster==self.__cluster) ).order_by(Job.priority).limit(njobs).with_for_update().all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []


  def getAllRunningJobs(self):
    try:

      MSG_INFO(self, "Getting all jobs with status: " + Color.CGREEN2+"[RUNNING]" + Color.CEND)
      return self.db().session().query(Job).filter( and_( Job.cluster==self.__cluster , Job.status==Status.RUNNING) ).with_for_update().all()
    except Exception as e:
      MSG_ERROR(self,e)
      return []


