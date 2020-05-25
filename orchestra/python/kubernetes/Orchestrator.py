
__all__ = ["Orchestrator"]

from kubernetes.client.rest import ApiException
from Gaugi import StatusCode, Logger, NotSet, Color
from Gaugi.messenger.macros import *
from kubernetes import *
from pprint import pprint
from orchestra import Status
from orchestra.constants import MAX_FAIL
from orchestra.utilities import Convert
import datetime
import time
import re
import json
import ast
import pprint


class Orchestrator(Logger):

  def __init__(self, job_template, credentials):
    Logger.__init__(self)
    import os
    # this is the current config LPS cluster yaml file
    config.load_kube_config(config_file=credentials)
    # Get the job batch api
    self.__batch_api = client.BatchV1Api()
    self.__client_api = client.ApiClient()
    self.__core_api = client.CoreV1Api()
    self._template_job_path = job_template
    #self.__policy = Policy()


  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS


  def batch(self):
    return self.__batch_api


  def client(self):
    return self.__client_api


  def core(self):
    return self.__core_api



  #
  # The template job
  #
  def getTemplate( self ):
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
  def status( self, name, namespace, max_fail=MAX_FAIL ):
    if self.exist( name, namespace ):
      resp = {}
      try:
        resp = self.batch().read_namespaced_job_status( name=name, namespace=namespace )
      except Exception as e:
        MSG_ERROR(self, e)
        return Status.FAILED
      timeout = datetime.timedelta(days=5)
      if (datetime.datetime.now()-resp.status.start_time.replace(tzinfo=None))>timeout:
        return Status.FAILED
      elif (resp.status.failed is not None) and resp.status.failed > 0:
        return Status.FAILED
      elif resp.status.succeeded:
        return Status.DONE
      else:
        return Status.RUNNING
    else:
      return Status.DONE


  #
  # Get LOGS for a given job
  #
  def logs (self, name, namespace):
    log = []
    try:
      ret = self.core().list_pod_for_all_namespaces(watch=False)
      for i in ret.items:
        if name in i.metadata.name:
          try:
            log.append(self.core().read_namespaced_pod_log(name=i.metadata.name, namespace=namespace))
          except:
            pass
      return log
    except ApiException:
      MSG_ERROR(self, "[ApiException] Failed to get logs for pod {}".format(name))
    except:
      MSG_ERROR(self, "[Unknown Error] Failed to get logs for pod {}".format(name))
    return []


  #
  # Create the job using a template
  #
  def create( self, name, namespace, containerImage, execArgs, node ):

    # Check if the job exist.
    if self.exist( name, namespace ):
      self.delete( name, namespace )
    # Generate the template
    template = self.getTemplate()
    template['metadata']['name'] = name
    template['spec']['template']['spec']['containers'][0]['image']=containerImage
    template['spec']['template']['spec']['nodeName']= node.name()

    #execArgs = self.__policy.check( execArgs )
    preExecArgs = "export CUDA_DEVICE_ORDER='PCI_BUS_ID'"
    #posExecArgs = "if ! (($?)); then exit 1; fi"
    posExecArgs = "exit $?"
    #posExecArgs = 'export OUTPUT_SIG=$? && echo "job output with signal $OUTPUT_SIG" && exit $OUTPUT_SIG'

    if node.device() is not None:
      MSG_INFO( self, "Setting this (%s) with GPU device (%d)", name, node.device() )
      template['spec']['template']['spec']['containers'][0]['resources']=\
      {
          'limits':{'nvidia.com/gpu':1},
          'requests':{'nvidia.com/gpu': 1}
      }
      # Append the device arg in execArgs to use a specifically GPU device
      preExecArgs += " && export CUDA_VISIBLE_DEVICES=%d"%( node.device() )
    else:
      # Force the job to not see and GPU device in case of the node has GPU installed or
      # the job is in GPU node but the device is not requested
      preExecArgs += " && export CUDA_VISIBLE_DEVICES='-1'"

    command = preExecArgs + " && ("+execArgs+") && " + posExecArgs

    template['spec']['template']['spec']['containers'][0]['args']=[command]

    # Send the job configuration to cluster kube server
    MSG_INFO( self, Color.CVIOLET2+"Launching job using kubernetes..."+Color.CEND)

    resp = self.batch().create_namespaced_job(body=template, namespace=namespace)
    name = resp.metadata.name
    return name


  #
  # Delete an single job by name
  #
  def delete( self, name, namespace ):
    try:
      body = client.V1DeleteOptions(propagation_policy='Background')
      resp = self.batch().delete_namespaced_job(name=name, namespace=namespace,body=body)
      time.sleep(5)
      return True
    except Exception as e:
      MSG_ERROR(self, e)
    return False

  #
  # Delete all jobs
  #
  def delete_all( self, namespace ):
    resp = self.batch().list_namespaced_job(namespace=namespace)
    for item in resp.items:
      self.delete(item.metadata.name)


  #
  # Check if a job exist into the kubernetes
  #
  def exist( self, name , namespace):
    try:
      resp = self.batch().list_namespaced_job(namespace=namespace)
      for item in resp.items:
        if name == item.metadata.name:
          return True
    except Exception as e:
      MSG_ERROR( self, e )
    return False


  #
  # List all jobs
  #
  def list( self, namespace ):
    resp = self.batch().list_namespaced_job(namespace=namespace)
    for item in resp.items:
      MSG_INFO(self,
            "%s\t%s\t%s" %
            (
             item.metadata.namespace,
             item.metadata.name,
             status_job_toString(self.status(item.metadata.name))
             ))





  def getNodeStatus(self):

    node_list = []
    try:
      for node in self.core().list_node().items:
        d = {'name' : node.metadata.name}
        for status in node.status.conditions:
          if status.type == "Ready":
            d["Ready"] = eval(status.status) if status.status != "Unknown" else False
          elif status.type == "MemoryPressure":
            d["MemoryPressure"] = eval(status.status) if status.status != "Unknown" else True
          elif status.type == "DiskPressure":
            d["DiskPressure"] = eval(status.status) if status.status != "Unknown" else True
          #elif status.type == "NetworkUnavailable":
          #  d["NetworkUnavailable"] = status.status

        node_list.append(d)
    except ApiException:
      MSG_ERROR (self, "Failed to get node status. Kubernetes API Exception")
    except:
      MSG_ERROR (self, "Failed to get node status. Unknown Exception")
    return node_list



  #
  # Get the reosurces (cpu/mem) for each node.
  # Taken from: https://github.com/amelbakry/kube-node-utilization/blob/master/nodeutilization.py
  #
  def utilization(self):
    ready_nodes = []
    usage = {}
    allocatable = {}
    allocatable_space = {}
    available_space = {}

    for n in self.core().list_node().items:
      #print(n.metadata.labels)
      #role = n.metadata.labels["kubernetes.io/role"]
      for status in n.status.conditions:
        if status.status == 'True' and status.type == 'Ready':
          ready_nodes.append(n.metadata.name)

    for node in ready_nodes:
      node_metrics = "/apis/metrics.k8s.io/v1beta1/nodes/" + node
      response = self.client().call_api(node_metrics,
                                       'GET', auth_settings=['BearerToken'],
                                       response_type='json', _preload_content=False)
      response = json.loads(response[0].data.decode('utf-8'))
      used = response.get("usage")
      values = {}
      values["memory"] = Convert.memory(used.get("memory"))
      values["cpu"] = Convert.cpu(used.get("cpu"))
      usage[node] = values

    for n in self.core().list_node().items:
      allocation = n.status.allocatable
      values = {}
      values["memory"] = Convert.memory(allocation.get("memory"))
      values["cpu"] = Convert.cpu(allocation.get("cpu"))
      allocatable_space[n.metadata.name] = values



    consume = []

    for node in ready_nodes:
      try:
        describe = {}
        usedmem = usage[node].get("memory")
        allmem = allocatable_space[node].get("memory")
        avamem = (usedmem / allmem) * 100
        avamem = "%.2f" % avamem


        usedcpu = usage[node].get("cpu")
        allcpu = allocatable_space[node].get("cpu")
        avacpu = (int(usedcpu) / int(allcpu)) * 100
        avacpu = "%.2f" % avacpu
        available_space[node] = {"memory": avamem, "cpu": avacpu}

        describe = {'usedmem':usedmem,'allmem':allmem,'avamem':avamem,
                    'usedcpu':usedcpu,'allcpu':allcpu,'avacpu':avacpu,
                    'node':node}
        consume.append( describe )
      except:
        pass
    return consume




  def getCPUConsume(self):
    allcpu = 0; usedcpu=0
    nodes = self.utilization()
    for n in nodes:
      usedcpu += n['usedcpu']
      allcpu += n['allcpu']
    return (int(usedcpu)/int(allcpu)) * 100


  def getMemoryConsume(self):
    allmem = 0; usedmem=0
    nodes = self.utilization()
    for n in nodes:
      usedmem += n['usedmem']
      allmem += n['allmem']
    return (int(usedmem)/int(allmem)) * 100







