__all__ = ["Devices"]

from orchestra.enums import State
from orchestra.utils import *
from orchestra.db import Device
from orchestra.core import Slot, Consumer

import os



#
# A collection of slots for each device (CPU and GPU)
#
class Devices:

  #
  # Constructor
  #
  def __init__(self, nodename, db):
    self.__db = db
    self.__slots = []
    self.nodename = nodename



  #
  # Init 
  #
  def init(self):
    # -1 for cpu slots, 0 for gpu id 0, 1 for gpu id 1, ...
    self.__devices = {}
    self.__total = 0
    for device in self.__db.session().query(Device).filter(Device.nodename==self.nodename).all():
      self.__devices[device.gpu] = [Slot(self.nodename, device.gpu) for slot in range(device.slots)]
      for slot in range(device.enabled):
        self.__devices[device.gpu][slot].enable()
        self.__total+=1



  def run(self):

    self.sync()

    # Loop over all available consumers
    for _, consumer in enumerate(self.__slots):

      job = consumer.job
      slot = consumer.slot

      if job.state == State.KILL:
        consumer.kill()

      if consumer.state() == State.PENDING:
        if not consumer.run():
          job.state = State.BROKEN
          slot.unlock()
          self.__slots.remove(consumer)
        else: # change to running State
          job.state = State.RUNNING

      elif consumer.state() is State.FAILED:
        job.state = State.FAILED
        slot.unlock()
        self.__slots.remove(consumer)

      elif consumer.state() is State.KILLED:
        job.state = State.KILLED
        slot.unlock()
        self.__slots.remove(consumer)

      elif consumer.state() is State.RUNNING:
        consumer.ping()

      elif consumer.state() is State.DONE:
        job.state = State.DONE
        slot.unlock()
        self.__slots.remove(consumer)

      # pull states
      self.__db.commit()

    return True



  def size(self):
    return self.__total


  def available(self):
    return True if len(self.__slots) < self.size() else False


  def allocated( self ):
    return len(self.__slots)


  

  def sync(self):

    print(self.__devices)
    before = self.size()
    total = 0
    for device in self.__db.session().query(Device).filter(Device.nodename==self.nodename).all():
      device.ping()
      for idx, slot in enumerate(self.__devices[device.gpu]):
        if idx < device.enabled:
          slot.enable()
          total += 1
        else:
          slot.disable()
      
    self.__total = total

    if total!= before:
      for device in self.__devices.values():
        enabled = sum([slot.is_enabled() for slot in device])
        total = len(device)
        if device.gpu >= 0:
          MSG_INFO('Updating GPU slots(device=%d) with %d/%d'%(device.gpu, enabled, total))
        else:
          MSG_INFO('Update CPU slots with %d/%d'%(enabled,total))


  #
  # Add a job into the slot
  #
  def push_back( self, job ):

    slot = self.__get_slot()
    if slot:
      consumer = Consumer( job , slot )
      job.state = State.PENDING
      self.__slots.append(consumer)
      job.ping()
      slot.lock()
      #self.__db.commit()
      return True
    else:
      MSG_ERROR( "You asked to add one job into the stack but there is no available slots yet." )
      return False



  #
  # Private
  #


  def __get_slot(self):
    for device in self.__devices.values():
      for slot in device:
        if slot.available():
          return slot
    return None
