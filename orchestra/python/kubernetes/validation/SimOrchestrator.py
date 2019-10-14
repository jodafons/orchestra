
__all__ = ["Orchestrator"]

from Gaugi import Logger, NotSet
from kubernetes import *
from pprint import pprint
from lpsgrid.engine.enumerations import Status


class Orchestrator(Logger):

  def __init__(self, path):

    import os
    # this is the current config LPS cluster yaml file
    self._template_job_path = os.environ['SAPHYRA_PATH']+'/Tools/lpsgrid/data/job_template.yaml'
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




  def getTemplate( self ):
    from benedict import benedict
    return dict(b.from_yaml(self._template_job_path))



  def status( self, name ):
    resp = self.api().read_namespaced_job_status( name=name, namespace='default' )
    if not resp.status.active is None:
      return Status.RUNNING
    elif not resp.status.failed is None:
      return Status.FAILED
    return Status.DONE




  def create( self, path ):

    pprint(d)
    # Send the job configuration to cluster kube server
    resp = self.api().create_namespaced_job(body=d, namespace='default')
    name = resp.metadata.name
    return name








