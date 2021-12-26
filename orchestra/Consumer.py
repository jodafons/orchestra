
__all__ = ["Consumer"]


from Gaugi import Logger, StatusCode
from Gaugi.macros import *
from orchestra import Status, getEnv
import os, glob, hashlib, sys

from subprocess import Popen, PIPE, STDOUT

class Consumer( Logger ):


  #
  # Constructor
  #
  def __init__(self, job, slot, db, extra_envs={} ):
    Logger.__init__(self)
    self.__job = job
    self.__db = db
    self.__slot = slot

    self.__pending=True
    self.__broken=False
    self.__killed=False

    # compose the job name
    hash_object = hashlib.md5(str.encode(job.execArgs))
    self.__hash = hash_object.hexdigest()
    self.__namespace = job.getTask().getUser().getUserName()
    queuename = job.getQueueName()
    self.__jobname = (queuename+ '.user.' + self.__namespace + '.' + self.__hash).replace('_','-') # add protection name


    # process
    self.__proc = None
    self.__extra_envs = extra_envs
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
      dirname = self.job().getTheOutputStoragePath()

      try:
        if os.path.exists(dirname):
          os.system('rm -rf %s'%dirname)
      except Exception as e:
        MSG_ERROR(self,e)
        MSG_ERROR(self, "It's not possible to remove the job config directory.")

      try: # create the empty directory
        os.system('mkdir -p %s'%dirname)
      except Exception as e:
        MSG_ERROR(self,e)
        MSG_ERROR(self,"It's not possible to create the output directory into the storage.")

      self.start()
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
      self.close()
    return StatusCode.SUCCESS


  def kill(self):
    self.__killed=True


  def killed(self):
    return self.__killed


  def broken(self):
    return self.__broken


  def pending(self):
    return self.__pending


  def ping(self):
    self.__job.ping()


  #
  # Get the consumer status
  #
  def status(self):

    if self.pending():
      return Status.PENDING
    elif self.killed():
      return Status.KILL
    elif self.broken():
      return Status.BROKEN
    else:

      answer = Status.DONE

      if self.__proc is not None:
        if not  self.__proc.poll() is None:
          if self.__proc.returncode != 0:
            answer =  Status.FAILED
        else:
          answer =  Status.RUNNING

      if answer == Status.DONE:
        # Check for any output file into the job directory
        output = self.job().getTheOutputStoragePath()

        from orchestra import getConfig
        config = getConfig()

        flist = glob.glob(output+"/"+config["job_complete_file_name"])
        MSG_INFO(self, "The job with name (%s) finished with %d files into the output directory: %s", self.__jobname, len(flist), output)
        return Status.FAILED if len(flist)==0 else Status.DONE
      else:
        return answer



  #
  # Create the job using a template
  #
  def start( self ):

    # copy the current env workspace
    env = os.environ.copy()
    env["export CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"

    if self.__slot.device() is not None:
      # Append the device arg in execArgs to use a specifically GPU device
      env["CUDA_VISIBLE_DEVICES"]=("%d")%( self.__slot.device() )
    else:
      # Force the job to not see and GPU device in case of the node has GPU installed or
      # the job is in GPU node but the device is not requested
      env["CUDA_VISIBLE_DEVICES"]=("-1")

    command = 'cd %s' % self.__job.getTheOutputStoragePath()
    #command+= ' && git clone https://github.com/jodafons/ringer.git'
    command+= ' && '+self.__job.execArgs

    #command+= ' &> %s/mylog.log'% self.__job.getTheOutputStoragePath()
    # Send the job configuration to cluster kube server
    MSG_INFO( self, "Launching job %s ...", self.__jobname)
    env.update( self.__extra_envs )

    print(command)
    self.__proc = Popen( command , env=env , shell=True)
    #self.__proc = Popen( command ,shell=True, stdout=PIPE, stderr=STDOUT, env=env )



  #
  # close process and save the log
  #
  def close( self ):

    if self.__proc is not None:

      #try:
      #  # compose the log file name
      #  logfile = self.__job.getTheOutputStoragePath()+'/mylog.log'
      #  with open(logfile, 'ab') as file:
      #    for line in self.__proc.stdout: # b'\n'-separated lines
      #      sys.stdout.buffer.write(line) # pass bytes as is
      #      file.write(line)

      #except Exception as e:
      #  MSG_ERROR( self, "It's not possible to save the log file. %s", e )

      # delete the process
      del self.__proc


