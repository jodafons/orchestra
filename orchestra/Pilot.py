
__all__ = ["Pilot"]


from Gaugi import Logger, StatusCode
from Gaugi.macros import *
from orchestra.enums import *
from orchestra.utils import Clock

SECONDS = 1.


class Pilot(Logger):

  #
  # Constructor
  #
  def __init__(self, node, db, schedule,  postman, master=True):

    Logger.__init__(self)
    self.__node = node
    self.__schedule = schedule
    self.__postman = postman
    self.__db = db
    self.__queue = {}

    self.__master = master
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
        MSG_FATAL( self, "Not possible to initialize the %s slot for %s node. abort", queue, self.__node.name )

    return StatusCode.SUCCESS



  def execute(self):

    while self.alive():

      if self.__clock():

        if self.__master:
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



  def alive(self):

    if self.__node.getSignal() == 'stop':
      self.__node.setSignal('waiting')
      return False
    else:
      # tell to the database that this node is running (alive)
      self.__node.ping()
      return True





