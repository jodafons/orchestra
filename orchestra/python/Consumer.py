
__all__ = ["Consumer"]


from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from Gaugi import retrieve_kw
from orchestra.db.models import *
from orchestra import Status
from orchestra.constants import MAX_FAIL
import hashlib
import os, glob


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
    self.__name = (queuename+ '.user.' + self.__namespace + '.' + self.__hash).replace('_','-') # add protection name
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
      dirname = self.job().getTheOutputStoragePath()
      try:
        # check if directory exist
        if os.path.exists(dirname):
          os.system('rm -rf %s'%dirname)
      except Exception as e:
        MSG_ERROR(self,e)
        MSG_ERROR(self, "It's not possible to remove the job config directory.")

      try: # create the empty directory
        os.system('mkdir %s'%dirname)
      except Exception as e:
        MSG_ERROR(self,e)
        MSG_ERROR(self,"It's not possible to create the output directory into the storage.")

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
    else:
      # Return the kubernetes status
      answer = self.orchestrator().status(self.name(), self.namespace(), 1)
      # If kubernetes tell that this job is done, we need to check first
      # if there are any file (created by the job script) into the job directory.
      # If any, we assume that the job fail in somepoint and kube dont catch.
      if answer == Status.DONE:
        # Check for any output file into the job directory
        output = self.job().getTheOutputStoragePath()
        flist = glob.glob(output+"/*")
        print(flist)
        MSG_INFO(self, "The job with name (%s) finished with %d files into the output directory: %s", self.name(), len(flist), output)
        return Status.FAILED if len(flist)==0 else Status.DONE
      else:
        return answer



  def saveLogs(self):
    try:
      from orchestra.constants import LOGFILE_NAME
      output = self.job().getTheOutputStoragePath() + "/" + LOGFILE_NAME%self.job().configId
      logs = self.orchestrator().logs(self.name(), self.namespace())
      flog = open(output,'w')
      try:
        flog.write(logs[0])
      except Exception as e:
        flog.write(e)
        flog.write("\nIt's not possible to retrive the log.")
        MSG_WARNING(self, "It's not possible to retrive and save the job log")
      flog.close()
    except Exception as e:
      MSG_ERROR(self,e)






