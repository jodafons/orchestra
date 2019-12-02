
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

  def __init__(self, db, schedule, orchestrator, 
               bypass_gpu_rule=False, 
               cluster=Cluster.LPS, 
               update_task_boards=True,
               timeout=None, 
               run_slots = True,
               queue_name = Queue.LPS):
    Logger.__init__(self)
    self.__cpu_slots = Slots("CPU", cluster, queue_name)
    self.__gpu_slots = Slots("GPU", cluster, queue_name, gpu=True)
    self.__db = db
    self.__schedule = schedule
    self.__orchestrator = orchestrator

    self.__bypass_gpu_rule = bypass_gpu_rule
    self.__cluster = cluster
    self.__queue_name = queue_name
    self.__update_task_boards = update_task_boards
    self.__timeout_clock = Clock( timeout )
    self.__run_slots = run_slots



  def checkTimeout(self):
    return self.__timeout_clock()


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
    self.schedule().setCluster( self.__cluster )
    self.schedule().setDatabase( self.db() )

    # Update the priority for each N minutes
    #self.schedule().setUpdateTime( 5 )

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

    if self.__run_slots:
      self.treatRunningJobsBeforeStart()



    # Infinite loop
    while True:

      if not self.checkTimeout():

        # Calculate all priorities for all REGISTERED jobs for each 5 minutes
        self.schedule().execute()

        # If in standalone mode, these slots will not in running mode. Only schedule will run.
        if self.__run_slots:
          if self.cpuSlots().isAvailable():
            ## Prepare jobs for CPU slots only
            njobs = self.cpuSlots().size() - self.cpuSlots().allocated()
            MSG_INFO(self,"There are slots available. Retrieving the first %d jobs from the CPU queue",njobs )
            jobs = self.schedule().getCPUQueue(njobs)
          
            while (self.cpuSlots().isAvailable()) and len(jobs)>0:
              self.cpuSlots().push_back( jobs.pop() )


          if self.gpuSlots().isAvailable():
            njobs = self.gpuSlots().size() - self.gpuSlots().allocated()
            if self.__bypass_gpu_rule:
              MSG_INFO(self,"There are GPU slots available. Retrieving the first %d jobs from the CPU queue since bypass gpu rule is True",njobs )
              jobs = self.schedule().getCPUQueue(njobs)
            else:
              MSG_INFO(self,"There are GPU slots available. Retrieving the first %d jobs from the GPU queue.",njobs )
              jobs = self.schedule().getGPUQueue()

            while (self.gpuSlots().isAvailable()) and len(jobs)>0:
              self.gpuSlots().push_back( jobs.pop() )

      
          ## Run the pilot for cpu queue
          self.cpuSlots().execute()
          ## Run the pilot for gpu queue
          self.gpuSlots().execute()


        # If in standalone mode, this can be calculated or note. Depend on demand.
        if self.__update_task_boards:
          MSG_INFO(self, "Calculate all task boards...")
          self.updateAllBoards()



      else:
        # Stop the main loop obly when all jobs are finished
        if self.cpuSlots().empty() and self.gpuSlots().empty():
          break



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
      tasks = user.getAllTasks( self.__cluster)
      #MSG_INFO(self, "Updating all task parameters for user(%s)",user.username)


      for task in tasks:
        #MSG_INFO(self, "Looking into %s", task.taskName)
        try:
          board = self.db().session().query(Board).filter( Board.taskName==task.taskName ).first()
        except:
          board = None
          #MSG_INFO(self, "The task (%s) does not exist into the table monitoring. Including...",task.taskName)

        # This board is not exist into the database. This should be created first
        if board is None:
          board = Board( username=user.username, taskId=task.id, taskName=task.taskName )
          self.db().session().add(board)

        board.jobs = len(task.getAllJobs())
        # Get he number of registered jobs for this task
        #board.registered    = len(self.db().session().query(Job).filter( and_( Job.status==Status.REGISTERED, Job.taskId==task.id )).all())
        board.assigned      = len(self.db().session().query(Job).filter( and_( Job.status==Status.ASSIGNED  , Job.taskId==task.id )).all())
        #board.testing       = len(self.db().session().query(Job).filter( and_( Job.status==Status.TESTING   , Job.taskId==task.id )).all())
        board.running       = len(self.db().session().query(Job).filter( and_( Job.status==Status.RUNNING   , Job.taskId==task.id )).all())
        board.done          = len(self.db().session().query(Job).filter( and_( Job.status==Status.DONE      , Job.taskId==task.id )).all())
        board.failed        = len(self.db().session().query(Job).filter( and_( Job.status==Status.FAILED    , Job.taskId==task.id )).all())
        board.status        = task.status
        self.db().commit()




