
__all__ = ["Pilot"]


from Gaugi import Logger, StatusCode
from Gaugi.messenger.macros import *
from orchestra.enumerations import *



class Pilot(Logger):


  def __init__(self, nodename, db, schedule, backend, postman):

    Logger.__init__(self)
    self.__nodename = nodename
    self.__schedule = schedule
    self.__postman = postman
    self.__backend = backend
    self.__db = db
    self.__queue = {}



  def __add__( self, slots ):
    self.__queue[slots.getQueueName()] = slots



  def initialize(self):

    for queue , slots in self.__queue.items():
      slot.setDatabase( self.__db )
      slot.setBackend( self.__backend )
      slot.setPostman(self.__postman )
      if slots.initialize().isFailure():
        MSG_FATAL( self, "Not possible to initialize the %s slot for %s node. abort", queue, self.__nodename )

    return StatusCode.SUCCESS



  def execute(self):

    while True:

      # If in standalone mode, these slots will not in running mode. Only schedule will run.
      for queue , slots in self.__queues.items():

        self.treatRunningJobsNotAlive(slots)
          
        if slots.isAvailable():
          njobs = slot.size() - slot.allocated()

          MSG_DEBUG(self,"There are slots available. Retrieving the first %d jobs from the CPU queue",njobs )
          jobs = self.schedule().getQueue(njobs, queue)

          while (slots.isAvailable()) and len(jobs)>0:
            slots.push_back( jobs.pop() )

        slots.execute()

    return StatusCode.SUCCESS



  def finalize(self):
    self.__db.finalize()
    self.__schedule.finalize()
    for queue , slots in self.__queue.items():
      slots.finalize()
    self.__backend.finalize()
    return StatusCode.SUCCESS



  def run(self):
    self.initialize()
    self.execute()
    self.finalize()
    return StatusCode.SUCCESS



  def treatRunningJobsNotAlive(self , slots):
    jobs = self.schedule().getAllRunningJobs(slots.getQueueName())
    for job in jobs:
      if not job.isAlive():
        job.setStatus( Status.ASSIGNED )




