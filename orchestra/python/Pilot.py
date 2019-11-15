
__all__ = ["Pilot"]


from Gaugi import Logger, NotSet, StatusCode, Color
from Gaugi.messenger.macros import *

from sqlalchemy import and_, or_
from orchestra.db.models import *
from orchestra.slots import *
from orchestra.constants import *
from orchestra.utilities import *
from orchestra.enumerations import *

class Pilot(Logger):

  def __init__(self, db, schedule, orchestrator, bypass_gpu_rule=False):
    Logger.__init__(self)
    self.__cpu_slots = Slots("CPU")
    self.__gpu_slots = Slots("GPU", gpu=True)
    self.__db = db
    self.__schedule = schedule
    self.__orchestrator = orchestrator
    self.__resouces_clock = Clock( 0.5* MINUTE )
    self.__bypass_gpu_rule = bypass_gpu_rule





  def db(self):
    return self.__db


  def schedule(self):
    return self.__schedule


  def orchestrator(self):
    return self.__orchestrator


  def cpuSlots(self):
    return self.__cpu_slots


  def gpuSlots(self):
    return self.__gpu_slots




  def initialize(self):

    # connect to the sql database (service)
    # Setup the kubernetes orchestrator (service)
    # link db to schedule
    self.schedule().setDatabase( self.db() )
    # Update the priority for each N minutes
    self.schedule().setUpdateTime( 5 )

    if self.schedule().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the Schedule tool. abort" )

    # link orchestrator/db to slots
    self.cpuSlots().setDatabase( self.db() )
    self.cpuSlots().setOrchestrator( self.orchestrator() )
    if self.cpuSlots().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the CPU slot tool. abort" )

    # link orchestrator/db to slots
    self.gpuSlots().setDatabase( self.db() )
    self.gpuSlots().setOrchestrator( self.orchestrator() )
    if self.gpuSlots().initialize().isFailure():
      MSG_FATAL( self, "Not possible to initialize the GPU slot tool. abort" )


    return StatusCode.SUCCESS



  def execute(self):

    self.treatRunningJobsBeforeStart()


    # Infinite loop
    while True:

      # Calculate all priorities for all REGISTERED jobs for each 5 minutes
      self.schedule().execute()

      ## Prepare jobs for CPU slots only
      jobs = self.schedule().getCPUQueue()


      if self.__bypass_gpu_rule:
        # Taken from CPU queue. In this case, the job can be
        # assigned to gpu or gpu.
        while (self.gpuSlots().isAvailable()) and len(jobs)>0:
          self.gpuSlots().push_back( jobs.pop() )
      else:
        jobs_gpu = self.schedule().getGPUQueue()
        while (self.gpuSlots().isAvailable()) and len(jobs_gpu)>0:
          self.gpuSlots().push_back( jobs_gpu.pop() )


      while (self.cpuSlots().isAvailable()) and len(jobs)>0:
        self.cpuSlots().push_back( jobs.pop() )



      ## Run the pilot for cpu queue
      self.cpuSlots().execute()
      ## Run the pilot for gpu queue
      self.gpuSlots().execute()

      self.updateAllBoards()


    return StatusCode.SUCCESS


  def finalize(self):

    self.db().finalize()
    self.schedule().finalize()
    self.cpuSlots().finalize()
    self.gpuSlots().finalize()
    self.orchestator().finalize()
    return StatusCode.SUCCESS



  def treatRunningJobsBeforeStart(self):

    jobs = self.schedule().getAllRunningJobs()
    if len(jobs) > 0:
      for job in jobs:
        job.setStatus( Status.ASSIGNED )
      i=0
      while (self.cpuSlots().isAvailable()) and i<len(jobs):
        self.cpuSlots().push_back( jobs[i] )
        i+=1
      self.cpuSlots().execute()



  #
  # This is for monitoring purpose. Should be used to dashboard view
  #
  def updateAllBoards( self ):

    for user in self.db().getAllUsers():
      # Get the number of tasks
      tasks = user.getAllTasks(CLUSTER_NAME)
      #MSG_INFO(self, "Updating all task parameters for user(%s)",user.username)


      for task in tasks:

        #MSG_INFO(self, "Looking into %s", task.taskName)
        try:
          board = self.db().session().query(TaskBoard).filter( TaskBoard.taskName==task.taskName ).first()
        except:
          board = None
          #MSG_INFO(self, "The task (%s) does not exist into the table monitoring. Including...",task.taskName)

        # This board is not exist into the database. This should be created first
        if board is None:
          board = TaskBoard( username=user.username, taskId=task.id, taskName=task.taskName )
          self.db().session().add(board)

        board.jobs = len(task.getAllJobs())
        # Get he number of registered jobs for this task
        board.registered    = len(self.db().session().query(Job).filter( and_( Job.status==Status.REGISTERED, Job.taskId==task.id )).all())
        board.assigned      = len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED  , Job.taskId==task.id )).all())
        board.testing       = len(self.db().session().query(Job).filter( and_( Job.status==Status.TESTING   , Job.taskId==task.id )).all())
        board.running       = len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING   , Job.taskId==task.id )).all())
        board.done          = len(self.db().session().query(Job).filter( and_( Job.status==Status.DONE      , Job.taskId==task.id )).all())
        board.failed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED    , Job.taskId==task.id )).all())
        board.status        = task.status
        self.db().commit()




