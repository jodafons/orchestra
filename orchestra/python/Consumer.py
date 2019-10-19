
__all__ = ["Consumer"]


from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from Gaugi import retrieve_kw
from orchestra import Status
import hashlib

class Consumer( Logger ):

  #
  # Args: The database manager, the kubernetes API (Kubernetes class) and
  # the database job object), node (use this to choose a specific node for
  # gpu staff
  #
  def __init__(self, job,  node ):
    Logger.__init__(self)
    self.__job = job
    self.__orchestrator = NotSet
    self.__node = node
    self.__pending=True
    self.__broken=False
    hash_object = hashlib.md5(str.encode(job.execArgs))
    self.__hash = hash_object.hexdigest()
    # the namespace is the username
    self.__namespace = job.getTask().getUser().getUserName()
    self.__name = 'user.' + self.__namespace + '.' + self.__hash[:10]
    MSG_INFO(self, "Create consumer with name: %s for namespace: %s", self.__name, self.__namespace)


  def name(self):
    return self.__name


  def namespace(self):
    return self.__namespace


  def job(self):
    return self.__job


  def node(self):
    return self.__node


  def setOrchestrator(self, orchestrator):
    self.__orchestrator=orchestrator


  def orchestrator(self):
    return self.__orchestrator


  def initialize(self):
    if self.orchestrator() is NotSet:
      MSG_FATAL(self, "you must pass the orchestrator to consumer.")
    return StatusCode.SUCCESS


  def execute(self):
    try:
      self.orchestrator().create( self.name(), self.namespace(), self.__job.containerImage, self.__job.execArgs, self.node() )
      self.__pending=False
    except Exception as e:
      MSG_ERROR(self, e)
      self.__broken=True
      return StatusCode.FAILURE

    return StatusCode.SUCCESS


  def finalize(self):
    if self.broken():
      MSG_DEBUG(self, "this consumer has no container into the rancher server. There is no thing to do...")
    else:
      self.orchestrator().delete( self.name(), self.namespace() )
    return StatusCode.SUCCESS


  def broken(self):
    return self.__broken


  def pending(self):
    return self.__pending


  def status(self):
    if self.pending():
      return Status.PENDING
    elif self.broken():
      return Status.BROKEN
    else: # Return the kubernetes status
      return self.orchestrator().status(self.name(), self.namespace())


