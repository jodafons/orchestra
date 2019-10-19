
__all__ = ["Pilot"]


from Gaugi import Logger, NotSet, StatusCode, Color
from Gaugi.messenger.macros import *

from orchestra.slots import *
from orchestra.constants import *
from orchestra.utilities import *
from orchestra.enumerations import *

class Pilot(Logger):

  def __init__(self, db, schedule, orchestrator, bypass_gpu_rule=False):
    Logger.__init__(self)
    self.__cpu_slot = NotSet
    self.__gpu_slot = NotSet
    self.__db = db
    self.__schedule = schedule
    self.__orchestrator = orchestrator
    self.__resouces_clock = Clock( 0.5* MINUTE )
    self.__bypass_gpu_rule = bypass_gpu_rule


  def setCPUSlots( self, slot ):
    self.__cpu_slot = slot

  def setGPUSlots( self, slot ):
    self.__gpu_slot = slot


  def treat(self):
    if not (self.cpuSlots() and self.gpuSlots()):
      MSG_FATAL( self, "cpu and gpu slots not set. You must set bouth" )


  def db(self):
    return self.__db


  def schedule(self):
    return self.__schedule


  def orchestrator(self):
    return self.__orchestrator


  def cpuSlots(self):
    return self.__cpu_slot


  def gpuSlots(self):
    return self.__gpu_slot


  def initialize(self):

    self.treat()
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



