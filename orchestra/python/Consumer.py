
__all__ = ["Consumer"]


from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from Gaugi import retrieve_kw
from orchestra.db.models import *
from orchestra import Status
from orchestra.constants import MAX_FAIL
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
    self.__killed=False
    self.__logs=""
    hash_object = hashlib.md5(str.encode(job.execArgs))
    self.__hash = hash_object.hexdigest()
    # the namespace is the username
    self.__namespace = job.getTask().getUser().getUserName()
    queuename = job.getQueueName()
    self.__name = queuename+ '.user.' + self.__namespace + '.' + self.__hash
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
      # We must remove the job if DONE, FAILED or KILLED status
      self.orchestrator().delete( self.name(), self.namespace() )

    return StatusCode.SUCCESS

  def kill(self):
    self.__killed=True


  def killed(self):
    return self.__killed


  def broken(self):
    return self.__broken


  def pending(self):
    return self.__pending


  def status(self):
    if self.pending():
      return Status.PENDING
    elif self.killed():
      return Status.KILL
    elif self.broken():
      return Status.BROKEN
    else: # Return the kubernetes status
      # If this consumer is running in GPU mode, we dont need
      # to give the chance to kubernetes to retry in another node
      # Maybe, this job can be assigned in a node without GPU in case
      # of fail. Herem we will tell to orchestrator to check the status
      # with max fail equal one. Otherwise will be three.
      if self.node().device() is not None:
        # GPU case with max fail equal one
        return self.orchestrator().status(self.name(), self.namespace(), 1)
      else:
        # default case with max fail equal three
        return self.orchestrator().status(self.name(), self.namespace(), 1)

  @property
  def logs(self):
    self.updateLogs()
    return self.__logs

  def updateLogs(self):
    if self.__logs == []:
      self.__logs = self.orchestrator().logs(self.name(), self.namespace())
    else:
      logs = self.orchestrator().logs(self.name(), self.namespace())
      if logs != []:
        self.__logs = logs

