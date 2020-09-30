__all__ = ["Subprocess"]

from Gaugi import Logger, StatusCode
from Gaugi.messenger.macros import *

from pprint import pprint
from orchestra import Status

import time
import re
import json
import ast


from subprocess import Popen, PIPE
import os



class Subprocess(Logger):

  def __init__(self):
    Logger.__init__(self)
    self.__process = {}


  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS



  #
  # Get the current status of the job.
  # Should be: FAILED, RUNNING or DONE
  #
  def status( self, name, namespace, trials ):

    if self.exist( jobname ):
      proc = self.__process[ jobname ]
      if not  proc.poll() is None:
        if proc.returncode != 0:
          return Status.FAILED
        else:
          return Status.DONE
      else:
        return Status.RUNNING
    else:
      return Status.DONE


  #
  # Create the job using a template
  #
  def create( self, jobname, job, slot ):

    # Check if the job exist.
    if self.exist( jobname ):
      self.delete( jobname )


    # copy the current env workspace
    env = os.environ.copy()
    env["export CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"


    if slot.device() is not None:
      # Append the device arg in execArgs to use a specifically GPU device
      env["CUDA_VISIBLE_DEVICES"]=("%d")%( slot.device() )
    else:
      # Force the job to not see and GPU device in case of the node has GPU installed or
      # the job is in GPU node but the device is not requested
      env["CUDA_VISIBLE_DEVICES"]=("-1")

    command = execArgs

    # Send the job configuration to cluster kube server
    MSG_INFO( self, "Launching job using subprocess...")

    command=command.split(' ')
    pprint(command)
    #proc = Popen( command ,stdout=PIPE, stderr=PIPE, env=env )
    proc = Popen( command , env=env )
    self.__process[ jobname ] = proc

    return jobname


  #
  # Delete an single job by name
  #
  def delete( self, jobname ):
    del self.__process[jobname]


  #
  # Check if a job exist into the kubernetes
  #
  def exist( self, jobname):
    if jobname in self.__process.keys():
      return True
    return False



