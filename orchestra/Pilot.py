

__all__ = ['Pilot']

from orchestra.core import Devices
from orchestra.enums import *
import time


class Clock(object):

  def __init__( self , maxseconds ):
    self.__maxseconds=maxseconds
    self.__then = None


  def __call__( self ):
    if self.__maxseconds is None:
      return False
    if not self.__then:
      self.__then = time.time()
      return False
    else:
      now = time.time()
      if (now-self.__then) > self.__maxseconds:
        # reset the time
        self.__then = None
        return True
    return False

  def reset(self):
    self.__then=None





class Pilot:

  #
  # Constructor
  #
  def __init__(self, nodename, db, schedule, postman, master=True):

    #self.__nodename = nodename
    self.__postman = postman
    self.__schedule = schedule
    self.__master = master
    self.__clock = Clock( 10 )
    self.__devices = Devices(nodename, db)


  def init(self):
    self.__devices.init()
    self.__schedule.init()
    #self.__postman.init()



  def run(self):

    while True:
      if self.__clock():
        if self.__master:
          self.__schedule.run()
        if self.__devices.available():
          njobs = self.__devices.size() - self.__devices.allocated()
          jobs = self.__schedule.get_jobs(njobs)
          while (self.__devices.available() and len(jobs)>0):
            self.__devices.push_back(jobs.pop())
        self.__devices.run()
        self.__clock.reset()








