__all__=  ["GPUSlots"]

from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from Gaugi import retrieve_kw
from orchestra import Slots
from collections import deque



class GPUSlots( Slots ):

  def __init__( self, name,nodes ):
    Slots.__init__( self, name,len(nodes) )
    self.__available_nodes = [(node,False) for node in nodes]


  def initialize(self):
    if(Slots.initialize(self).isFailure()):
      return StatusCode.FAILURE
    MSG_INFO( self, "This slots will be dedicated for GPUs" )
    return StatusCode.SUCCESS




  def unlock(self,node):
    for n in self.__available_nodes:
      if node==n[0]:
        n[1]=False; break


  def lock(self,node):
    for n in self.__available_nodes:
      if node==n[0]:
        n[1]=True; break


  def unlockAll(self):
    for n in self.__available_nodes:
      n[1]=False


  def getAvailableNode(self):
    for n in self.__available_nodes:
      if not n[1]:  return n[0]
    return None



  def update(self):
    for idx, consumer in enumerate(self.__slots):

      # consumer.status is not DB like, this is internal of kubernetes
      # In DB, the job was activated but here, we put as pending to wait the
      # kubernetes. If everything its ok, the internal status will be change
      # to (running,failed or done).
      if consumer.status() is Status.PENDING:
        # TODO: Change the internal state to RUNNING
        # If, we have an error during the message,
        # we will change to BROKEN status
        if consumer.execute().isFailure():
          # Tell to DB that this job is in broken status
          consumer.job().setStatus( Status.BROKEN )
          consumer.finalize()
          self.__slots.remove(consumer)
          self.unlock( consume.node() )
        else: # change to running status
          # Tell to DB that this job is running
          consumer.job().setStatus( Status.RUNNING )

      elif consumer.status() is Status.FAILED:
        # Tell to db that this job was failed
        consumer.job().setStatus( Status.FAILED )
        consumer.finalize()
        # Remove this job into the stack
        self.__slots.remove(consumer)
        self.unlock( consume.node() )
      # Kubernetes job is running. Go to the next slot...
      elif consumer.status() is Status.RUNNING:
        continue

      elif consumer.status() is Status.DONE:
        consumer.job().setStatus( Status.DONE )
        consumer.finalize()
        self.__slots.remove(consumer)
        self.unlock( consume.node() )

    self.db().commit()

  #
  # Add an job into the stack
  # Job is an db object
  #
  def push_back( self, job ):
    if self.isAvailable():
      node = self.getAvailableNode()
      # Create the job object
      obj = Consumer( job, node )
      # Tell to database that this job will be activated
      obj.setOrchestrator( self.orchestrator() )
      # TODO: the job must set the internal status to ACTIVATED mode
      obj.initialize()
      obj.job().setStatus( Status.ACTIVATED )
      self.__slots.append( obj )
      self.lock(obj.node())
    else:
      MSG_WARNING( self, "You asked to add one job into the stack but there is no available slots yet." )

















