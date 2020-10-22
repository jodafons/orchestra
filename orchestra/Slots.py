
__all__ = ["Slots"]


from Gaugi import Logger
from Gaugi.messenger.macros import *
from Gaugi import StatusCode
from collections import deque
from orchestra import Status
from orchestra.Consumer import Consumer
from orchestra import Postman

# import tensorflow to retrieve the number of GPUs devices
import tensorflow as tf;


class SingleSlot( object ):

  def __init__(self, name, device=None):
    self.__name = name
    self.__available = True
    self.__device = device
    self.__enable = False

  def name(self):
    return self.__name

  def isAvailable( self ):
    return (self.__available and self.__enable)

  def lock( self ):
    self.__available = False

  def unlock( self ):
    self.__available = True

  # For GPU nodes
  def device(self):
    return self.__device

  def isEnable(self):
    return self.__enable

  def enable(self):
    self.__enable=True

  def disable(self):
    self.__enable=False




class GPUSlot( SingleSlot ):
  def __init__(self, name, device ):
    SingleSlot.__init__( self, name, device )




class CPUSlot( SingleSlot ):
  def __init__(self, name):
    SingleSlot.__init__(self, name)




class Slots( Logger ):

  #
  # Constructor
  #
  def __init__(self, node, queuename, db=None, gpu=False, postman=None):
    Logger.__init__(self,name=queuename)


    self.__slots = list()
    self.__available_nodes = list()
    self.__gpu = gpu
    self.__total = 0
    self.__queuename = queuename

    self.__db = db
    self.__postman = postman
    self.__node = node


  def postman (self):
    return self.__postman


  def setPostman(self, postman):
    self.__postman = postman


  def setDatabase( self, db ):
    self.__db = db


  def db(self):
    return self.__db


  def getQueueName(self):
    return self.__queuename


  def initialize(self):

    # check if db exist
    if not self.db():
      MSG_FATAL( self, "Database object not passed to slot." )



    # Check if we have GPUs in the current node
    if self.__gpu and (self.__node.getMaxNumberOfSlots( gpu=True) > 0):
      ngpus = len(tf.config.experimental.list_physical_devices('GPU'))
      MSG_INFO( self, "Number of GPUs found in %s: %d", self.__node.getName(), ngpus)
      if ngpus==0:
        return StatusCode.FATAL




    MSG_INFO(self,"Setup all slots into the queue with name: %s", self.__queuename)

    if self.__gpu:
      # The node start enable flag as False. You must enable this in the first interation
      self.__available_slots = [ GPUSlot(self.__node.getName(),idx) for idx in range(self.__node.getMaxNumberOfSlots( gpu=True )) ]
    else:
      # The node start enable flag as False. You must enable this in the first interation
      self.__available_slots = [ CPUSlot(self.__node.getName()) for _ in range(self.__node.getMaxNumberOfSlots()) ]


    # enable each machine node
    for idx in range( self.__node.getNumberOfEnabledSlots( gpu=self.__gpu ) ):
      try:
        self.__available_slots[idx].enable()
      except:
        MSG_ERROR(self, "Failed to enable {}'s node {}".format(self.__node.getName(), idx))


    if self.__gpu:
      for slot in self.__available_slots:
        MSG_INFO( self, "Creating a GPU Slot(%s) with device %d. This slot is enable? %s", self.__node.getName(), slot.device(), slot.isEnable() )
    else:
      MSG_INFO( self, "Creating a CPU Slot(%s) with %d/%d", self.__node.getName(), self.__node.getNumberOfEnabledSlots(), self.__node.getMaxNumberOfSlots() )


    # Count the number of enable slots
    self.__total = 0
    for slot in self.__available_slots:
      if slot.isEnable(): self.__total+=1

    MSG_INFO( self, "Creating cluster stack with %d slots", self.size() )

    return StatusCode.SUCCESS





  def execute(self):

    self.update()


    for idx, consumer in enumerate(self.__slots):


      if consumer.job().getStatus() == Status.KILL:
        consumer.kill()


      if consumer.status() is Status.PENDING:

        if consumer.execute().isFailure():

          consumer.job().setStatus( Status.BROKEN )
          consumer.finalize()
          consumer.slot().unlock()
          self.__slots.remove(consumer)

        else: # change to running status
          consumer.job().setStatus( Status.RUNNING )

      elif consumer.status() is Status.FAILED:

        consumer.job().setStatus( Status.FAILED )
        consumer.finalize()
        consumer.slot().unlock()
        self.__slots.remove(consumer)

      elif consumer.status() is Status.KILL:

        consumer.job().setStatus( Status.KILLED )
        consumer.finalize()
        consumer.slot().unlock()
        self.__slots.remove(consumer)

      elif consumer.status() is Status.RUNNING:
        consumer.ping()


      elif consumer.status() is Status.DONE:
        consumer.job().setStatus( Status.DONE )
        consumer.finalize()
        consumer.slot().unlock()
        self.__slots.remove(consumer)

      # update the current consumer
      self.db().commit()

    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS


  def size(self):
    return self.__total


  def isAvailable(self):
    return True if len(self.__slots) < self.size() else False


  def allocated( self ):
    return len(self.__slots)


  def empty(self):
    return False if len(self.__slots)>0 else True


  def getAvailableSlot(self):
    for slot in self.__available_slots:
      if slot.isAvailable():
        return slot
    return None



  #
  # Update the number of slots from the database
  #
  def update(self):

    # get the number of activated slots
    before = self.size()

    total = 0

    # enable each machine node
    for idx, slot in enumerate(self.__available_slots):
      if idx < self.__node.getNumberOfEnabledSlots( gpu=self.__gpu ):
        slot.enable(); total+=1
      else:
        slot.disable()


    # Update the total number of enable slots in the slots list
    self.__total=total

    if self.size()!=before:
      if self.__gpu:
        for slot in self.__available_slots:
          MSG_INFO( self, "Updating a GPU Slot(%s) with device %d. This slot is enable? %s", self.__node.getName(), slot.device(), slot.isEnable() )
      else:
        MSG_INFO( self, "Updating a CPU Slots(%s) with %d/%d", self.__node.getName(), self.__node.getNumberOfEnabledSlots(), self.__node.getMaxNumberOfSlots() )

      MSG_INFO( self, "Creating cluster stack with %d slots", self.size() )




  #
  # Add a job into the slot
  #
  def push_back( self, job ):

    # Check if we have available slots
    if self.isAvailable():
      slot = self.getAvailableSlot()
      consumer = Consumer( job, slot, self.db() )
      consumer.job().setStatus( Status.PENDING )
      job.ping()
      consumer.initialize()
      self.__slots.append( consumer )
      slot.lock()
    else:
      MSG_WARNING( self, "You asked to add one job into the stack but there is no available slots yet." )












