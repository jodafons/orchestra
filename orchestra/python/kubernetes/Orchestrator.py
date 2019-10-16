
__all__ = ["Orchestrator"]

from Gaugi import Logger, NotSet, Color
from Gaugi.messenger.macros import *
from kubernetes import *
from pprint import pprint
from orchestra import Status
from orchestra.constants import MAX_FAIL
#from orchestra.rules import Policy
import time



class Orchestrator(Logger):

  def __init__(self, job_template, credentials):
    Logger.__init__(self)
    import os
    # this is the current config LPS cluster yaml file
    config.load_kube_config(config_file=credentials)
    # Get the job batch api
    self._api = client.BatchV1Api()
    self._template_job_path = job_template
    self.__db = NotSet
    #self.__policy = Policy()


  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS




  def setDatabase(self,db):
    self.__db = db

  def db(self):
    return self.__db


  def api(self):
    return self._api

  #
  # The template job
  #
  def getTemplate( self ):
    #from benedict import benedict as b

    try:
      import benedict
      d = dict(benedict.load_yaml_file(self._template_job_path))
    except:
      from benedict import benedict
      d = dict(benedict.from_yaml(self._template_job_path))
    return d

  #
  # Get the current status of the job.
  # Should be: FAILED, RUNNING or DONE
  #
  def status( self, name ):
    resp = self.api().read_namespaced_job_status( name=name, namespace='default' )
    #pprint(resp)
    if resp.status.failed and resp.status.failed > MAX_FAIL:
      return Status.FAILED
    elif resp.status.succeeded:
      return Status.DONE
    return Status.RUNNING


  #
  # Create the job using a template
  #
  def create( self, name, containerImage, execArgs, gpu_node=None ):

    # Check if the job exist.
    if self.exist( name ):
      self.delete( name )
    # Generate the template
    template = self.getTemplate()
    template['metadata']['name'] = name
    template['spec']['template']['spec']['containers'][0]['image']=containerImage
    
    #execArgs = self.__policy.check( execArgs )
    preExecArgs = "export CUDA_DEVICE_ORDER='PCI_BUS_ID'"
    #posExecArgs = "if ! (($?)); then exit 1; fi"
    posExecArgs = "exit $?"

    if gpu_node:
      template['spec']['template']['spec']['nodeName']= gpu_node.name()
      template['spec']['template']['spec']['containers'][0]['resources']=\
      {
          'limits':{'nvidia.com/gpu':1},
          'requests':{'nvidia.com/gpu': 1}
      }
      # Append the device arg in execArgs to use a specifically GPU device
      preExecArgs += " && export CUDA_VISIBLE_DEVICES=%d"%( gpu_node.device() )
    else:
      # Force the job to not see and GPU device in case of the node has GPU installed or
      # the job is in GPU node but the device is not requested
      preExecArgs += " && export CUDA_VISIBLE_DEVICES="

    command = preExecArgs + " && ("+execArgs+") && " + posExecArgs

    template['spec']['template']['spec']['containers'][0]['args']=[command]

    # Send the job configuration to cluster kube server
    MSG_INFO( self, Color.CVIOLET2+"Launching job using kubernetes..."+Color.CEND)
    resp = self.api().create_namespaced_job(body=template, namespace='default')
    name = resp.metadata.name
    return name


  #
  # Delete an single job by name
  #
  def delete( self, name ):
    body = client.V1DeleteOptions(propagation_policy='Background')
    resp = self.api().delete_namespaced_job(name=name, namespace='default',body=body)
    time.sleep(5)
    return True

  #
  # Delete all jobs
  #
  def delete_all( self ):
    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      self.delete(item.metadata.name)


  #
  # Check if a job exist into the kubernetes
  #
  def exist( self, name ):
    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      if name == item.metadata.name:
        return True
    return False


  #
  # List all jobs
  #
  def list( self ):
    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      MSG_INFO(self, 
            "%s\t%s\t%s" %
            (
             item.metadata.namespace,
             item.metadata.name,
             status_job_toString(self.status(item.metadata.name))
             ))



