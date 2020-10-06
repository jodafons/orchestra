
__all__ = ["Pilot"]


from Gaugi import Logger, StatusCode
from Gaugi.messenger.macros import *
from orchestra.enums import *
from orchestra.utils import Clock

SECONDS = 1.


class Pilot(Logger):

  #
  # Constructor
  #
  def __init__(self, nodename, db, schedule,  postman):

    Logger.__init__(self)
    self.__nodename = nodename
    self.__schedule = schedule
    self.__postman = postman
    self.__db = db
    self.__queue = {}

    self.__clock = Clock( 10*SECONDS )


  def __add__( self, slots ):
    self.__queue[slots.getQueueName()] = slots
    return self



  def initialize(self):

    self.__schedule.setDatabase( self.__db )
    self.__schedule.setPostman( self.__postman )

    for queue , slots in self.__queue.items():
      slots.setDatabase( self.__db )
      slots.setPostman(self.__postman )
      if slots.initialize().isFailure():
        MSG_FATAL( self, "Not possible to initialize the %s slot for %s node. abort", queue, self.__nodename )

    return StatusCode.SUCCESS



  def execute(self):

    while True:

      if self.__clock():

        self.__schedule.execute()

        # If in standalone mode, these slots will not in running mode. Only schedule will run.
        for queue , slots in self.__queue.items():
            
          if slots.isAvailable():
            njobs = slots.size() - slots.allocated()

            MSG_DEBUG(self,"There are slots available. Retrieving the first %d jobs from the CPU queue",njobs )
            jobs = self.__schedule.getQueue(njobs, queue)

            while (slots.isAvailable()) and len(jobs)>0:
              slots.push_back( jobs.pop() )

          slots.execute()

        self.__clock.reset()

    return StatusCode.SUCCESS



  def finalize(self):

    self.__db.finalize()
    self.__schedule.finalize()
    for queue , slots in self.__queue.items():
      slots.finalize()
    return StatusCode.SUCCESS



  def run(self):

    self.initialize()
    self.execute()
    self.finalize()
    return StatusCode.SUCCESS




