
__all__ = ["Orchestrator"]

from Gaugi import Logger, NotSet
from kubernetes import *
from pprint import pprint
from lpsgrid.engine.enumerations import Status
import time

MAX_FAIL=1
class Orchestrator(Logger):

  def __init__(self,path=None):

    import os
    # this is the current config LPS cluster yaml file
    config.load_kube_config(config_file=os.environ['SAPHYRA_PATH']+'/Tools/lpsgrid/data/lps_cluster.yaml')
    # Get the job batch api
    self._api = client.BatchV1Api()
    self._template_job_path = os.environ['SAPHYRA_PATH']+'/Tools/lpsgrid/data/job_template.yaml'
    #self._template_job_path = os.environ['SAPHYRA_PATH']+'/Tools/lpsgrid/data/pi.yaml'
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


  def getTemplate( self ):
    from benedict import benedict as b
    return dict(b.from_yaml(self._template_job_path))



  def status( self, name ):
    resp = self.api().read_namespaced_job_status( name=name, namespace='default' )
    #pprint(resp)
    if resp.status.failed and resp.status.failed > MAX_FAIL:
      return Status.FAILED
    elif resp.status.succeeded:
      return Status.DONE
    return Status.RUNNING



  def create( self, name, containerImage, execArgs, node=None ):

    if self.exist( name ):
      self.delete( name )
      

    template = self.getTemplate()
    template['metadata']['name'] = name
    template['spec']['template']['spec']['containers'][0]['args']=[execArgs]
    template['spec']['template']['spec']['containers'][0]['image']=containerImage
    template['spec']['template']['spec']['nodeName']='node07'
    pprint(template)
    # Send the job configuration to cluster kube server
    resp = self.api().create_namespaced_job(body=template, namespace='default')
    name = resp.metadata.name
    return name



  def delete( self, name ):
    body = client.V1DeleteOptions(propagation_policy='Background')
    resp = self.api().delete_namespaced_job(name=name, namespace='default',body=body)
    time.sleep(5)
    return True


  def delete_all( self ):
    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      self.delete(item.metadata.name)


  def exist( self, name ):

    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      if name == item.metadata.name:
        return True
    return False



  def list( self ):
    resp = self.api().list_namespaced_job(namespace='default')
    for item in resp.items:
      print(
            "%s\t%s\t%s" %
            (
             item.metadata.namespace,
             item.metadata.name,
             status_job_toString(self.status(item.metadata.name))
             ))



