
__all__ = ["Orchestrator"]

from Gaugi import Logger, NotSet, Color
from Gaugi.messenger.macros import *

from pprint import pprint
from orchestra import Status
from orchestra.constants import MAX_FAIL

import time
import re
import json
import ast


from subprocess import Popen, PIPE
import os



class Orchestrator(Logger):

  def __init__(self):
    Logger.__init__(self)
    # Hold all process points
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

    if self.exist( name, namespace ):
      proc = self.__process[ self.getProcName( name, namespace ) ]
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
  def create( self, name, namespace, containerImage, execArgs, node ):

    # Check if the job exist.
    if self.exist( name, namespace ):
      self.delete( name, namespace )

    # copy the current env workspace
    env = os.environ.copy()
    env["export CUDA_DEVICE_ORDER"]= "PCI_BUS_ID"

    if node.device() is not None:
      # Append the device arg in execArgs to use a specifically GPU device
      env["CUDA_VISIBLE_DEVICES"]=("%d")%( node.device() )
    else:
      # Force the job to not see and GPU device in case of the node has GPU installed or
      # the job is in GPU node but the device is not requested
      env["CUDA_VISIBLE_DEVICES"]=("-1")

    command = execArgs

    # Send the job configuration to cluster kube server
    MSG_INFO( self, Color.CVIOLET2+"Launching job using subprocess..."+Color.CEND)

    command=command.split(' ')
    #from pprint import pprint
    #pprint(command)
    proc = Popen( command ,stdout=PIPE, stderr=PIPE, env=env )
    #proc = Popen( command , env=env )
    self.__process[ self.getProcName(name,namespace) ] = proc

    return name


  #
  # Delete an single job by name
  #
  def delete( self, name, namespace ):
    self.__process[self.getProcName(name,namespace)]


  #
  # Check if a job exist into the kubernetes
  #
  def exist( self, name , namespace):
    if self.getProcName(name,namespace) in self.__process.keys():
      return True
    return False



  def getProcName( self, name, namespace):
    return namespace+"__"+name


