
__all__ = ["Consumer"]


from Gaugi import Logger, StatusCode
from Gaugi.messenger.macros import *
from orchestra import Status
import os, glob, hashlib


class Consumer( Logger ):

  
  #
  # Constructor
  #
  def __init__(self, job, slot, db, backend ):
    Logger.__init__(self)
    self.__job = job
    self.__db = db
    self.__slot = slot
    self.__backend = backend
    
    self.__pending=True
    self.__broken=False
    self.__killed=False
    
    # compose the job name
    hash_object = hashlib.md5(str.encode(job.execArgs))
    self.__hash = hash_object.hexdigest()
    self.__namespace = job.getTask().getUser().getUserName()
    queuename = job.getQueueName()
    self.__jobname = (queuename+ '.user.' + self.__namespace + '.' + self.__hash).replace('_','-') # add protection name
    
    MSG_INFO(self, "Create consumer with name: %s for namespace: %s", self.__jobname, self.__namespace)



  def job(self):
    return self.__job


  def slot(self):
    return self.__slot


  def db(self):
    return self.__db


  def backend(self):
    return self.__backend



  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    try:
      dirname = self.__volume + '/' + self.job().getTheOutputStoragePath()
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

      self.backend().create( self.__jobname, self.__job, self.__slot )

      self.__pending=False

    except Exception as e:
      MSG_ERROR(self, e)
      self.__broken=True
      return StatusCode.FAILURE

    return StatusCode.SUCCESS




  def finalize(self):
    if self.broken():
      MSG_DEBUG(self, "this consumer is in broken status. There is no thing to do...")
    else:
      self.delete()
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
      answer = self.backend().status(self.__jobname)
      # If kubernetes tell that this job is done, we need to check first
      # if there are any file (created by the job script) into the job directory.
      # If any, we assume that the job fail in somepoint and kube dont catch.
      if answer == Status.DONE:
        # Check for any output file into the job directory
        output = self.job().getTheOutputStoragePath()
        flist = glob.glob(output+"/*")
        MSG_INFO(self, "The job with name (%s) finished with %d files into the output directory: %s", self.__jobname, len(flist), output)
        return Status.FAILED if len(flist)==0 else Status.DONE
      else:
        return answer





