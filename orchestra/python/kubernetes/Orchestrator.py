
__all__ = ["Orchestrator"]

from Gaugi import Logger, NotSet
from kubernetes import *
from pprint import pprint
from orchestra import Status
from orchestra.constants import MAX_FAIL
import time



class Orchestrator(Logger):

  def __init__(self, job_template, credentials):

    import os
    # this is the current config LPS cluster yaml file
    config.load_kube_config(config_file=credentials)
    # Get the job batch api
    self._api = client.BatchV1Api()
    self._template_job_path = job_template
    self.__db = NotSet

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
    from benedict import benedict as b
    return dict(b.from_yaml(self._template_job_path))



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
  def create( self, name, containerImage, execArgs, node=None, gpu=False ):

    # Check if the job exist.
    if self.exist( name ):
      self.delete( name )
    # Generate the template
    template = self.getTemplate()
    template['metadata']['name'] = name
    template['spec']['template']['spec']['containers'][0]['args']=[execArgs]
    template['spec']['template']['spec']['containers'][0]['image']=containerImage

    if node:
      template['spec']['template']['spec']['nodeName']=node
    if gpu:
      template['spec']['template']['spec']['containers'][0]['runtime']='nvidia'
      #template['spec']['template']['spec']['containers'][0]['resources']=\
      #{
      #    'limits':{'nvidia.com/gpu':1},
      #    #'requests':{'nvidia.com/gpu': 1}
      #}


    # Send the job configuration to cluster kube server
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



