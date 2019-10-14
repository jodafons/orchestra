__all__=  ["GPUSlots"]

from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from Gaugi import retrieve_kw
from lpsgrid.engine.slots import Slots
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

  def update(self):

    to_be_removed = []

    for idx, job in enumerate(self._slots):

      if job.status() is StatusJob.ACTIVATED:
        # TODO: Change the internal state to RUNNING
        # If, we have an error during the message,
        # we will change to BROKEN status
        if job.execute().isFailure():
          # tell to db that we have a broken
          job.job().setStatus( StatusJob.BROKEN )
          to_be_removed.append(idx)
        else: # change to running status
          job.job().setStatus( StatusJob.RUNNING )

      elif job.status() is StatusJob.FAILED:

        # Tell to db that this job was failed
        job.job().setStatus( StatusJob.FAILED )
        # Remove this job into the stack
        to_be_removed.append(idx)

      elif job.status() is StatusJob.RUNNING:
        continue

      elif job.status() is StatusJon.DONE:
        job.job().setStatus( StatusJob.DONE )
        to_be_removed.append(idx)


    # remove all failed/broken/done jobs of the stack
    for r in to_be_removed:
      node = self.__slots[r].node()
      del self.__slots[r]
      self.unlockNode(node)
      MSG_DEBUG(self, "Removing this %d in the stack. We have $d/%d slots availabel", r, len(self._stack),selt.size())



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
      # Tell to db that this job was activated by the slot
      obj.job().setStatus( StatusJob.ACTIVATED )
      self._slots.append( obj )
      # lock this node since we can only run one process per node
      self.lockNode(node)
    else:
      MSG_WARNING( self, "You asked to add one job into the stack but there is no available slots yet." )





  def unlockNode(self,node):
    for n in self.__available_nodes:
      if node==n[0]:
        n[1]=False; break


  def lockNode(self,node):
    for n in self.__available_nodes:
      if node==n[0]:
        n[1]=True; break


  def unlockAllNodes(self):
    for n in self.__available_nodes:
      n[1]=False


  def getAvailableNode(self):
    for n in self.__available_nodes:
      if not n[1]:  return n[0]
    return None


