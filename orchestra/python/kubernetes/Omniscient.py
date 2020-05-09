__all__ = ["Omniscient"]

from orchestra.db import *
from orchestra import Cluster, Queue

from Gaugi import Logger
from Gaugi.messenger.macros import *

from prometheus_client.parser import text_string_to_metric_families
import requests
from copy import deepcopy

class Omniscient (Logger):
  """
  This is the Omniscient module. It's called like
  that because its purpose is to know everything
  about every node on the cluster. It shall provide
  information for load management and monitoring.
  """

  def __init__ (self, cluster=Cluster.LPS, queue=Queue.LPS):
    Logger.__init__(self)
    self.__cluster = cluster
    self.__queue   = queue
    self.__db = OrchestraDB(cluster = self.__cluster)

  def __str__ (self):
    return "<The Omniscient>"
  
  def __repr__ (self):
    return self.__str__()

  def getRawNodeMetrics (self, node):
    try:
      node = self.db.getMachine(self.cluster, self.queue, node)
    except:
      MSG_ERROR (self, "Failed to get node details from the database")
      return {}
    try:
      metrics = requests.get("http://" + node.ip + ":9796/metrics").text
    except:
      MSG_ERROR (self, "Failed to fetch data from node prometheus instance")
      return {}
    try:
      metrics_dict = {}
      for family in text_string_to_metric_families(metrics):
        for sample in family.samples:
          # Sample is a named tuple w/ the following format:
          # sample[0] = "name"        <str>
          # sample[1] = "labels"      <dict>
          # sample[2] = "value"       <float>
          # sample[3] = "timestamp"   <timestamp>
          # sample[4] = "exemplar"    <???>
          if sample[0] in metrics_dict.keys():
            if type(metrics_dict[sample[0]]) != list:
              old = deepcopy(metrics_dict[sample[0]])
              metrics_dict[sample[0]] = []
              metrics_dict[sample[0]].append(old)
            new = {}
            new['labels']     = sample[1]
            new['value']      = sample[2]
            new['timestamp']  = sample[3]
            new['exemplar']   = sample[4]
            metrics_dict[sample[0]].append(new)
          else:
            metrics_dict[sample[0]] = {}
            metrics_dict[sample[0]]['labels']     = sample[1]
            metrics_dict[sample[0]]['value']      = sample[2]
            metrics_dict[sample[0]]['timestamp']  = sample[3]
            metrics_dict[sample[0]]['exemplar']   = sample[4]
      return metrics_dict
    except:
      MSG_ERROR (self, "Failed to parse data from node")
      return {}

  def getNodeMetrics (self, node):
    raw_metrics = self.getRawNodeMetrics(node)
    if raw_metrics == {}:
      return False
    metrics = {}
    # Getting CPU count
    try:
      cpu_count = 0
      for entry in raw_metrics['node_cpu_seconds_total']:
        if entry['labels']['mode'] == 'system':
          cpu_count += 1
      metrics['cpu_count'] = cpu_count
    except:
      MSG_ERROR (self, "Failed to parse CPU count data")
    # Getting total RAM
    try:
      total_ram = raw_metrics['node_memory_MemTotal_bytes']['value']
      metrics['mem_total'] = total_ram
    except:
      MSG_ERROR (self, "Failed to parse total RAM data")
    # Getting average CPU load (1m average)
    try:
      cpu_load = raw_metrics['node_load1']['value'] / metrics['cpu_count']
      metrics['cpu_load'] = cpu_load
      metrics['cpu_load_percent'] = cpu_load * 100
    except:
      MSG_ERROR (self, "Failed to parse CPU Load (1m) data")
    # Getting average CPU load (5m average)
    try:
      cpu_load = raw_metrics['node_load1']['value'] / metrics['cpu_count']
      metrics['cpu_load_5m'] = cpu_load
      metrics['cpu_load_5m_percent'] = cpu_load * 100
    except:
      MSG_ERROR (self, "Failed to parse CPU Load (5m) data")
    # Getting RAM usage
    try:
      metrics['mem_free'] = raw_metrics['node_memory_MemFree_bytes']['value']
      metrics['mem_used'] = metrics['mem_total'] - metrics['mem_free']
      metrics['mem_free_percent'] = metrics['mem_free'] / metrics['mem_total'] * 100
      metrics['mem_used_percent'] = metrics['mem_used'] / metrics['mem_total'] * 100
    except:
      MSG_ERROR (self, "Failed to parse Memory Usage info")
    # Getting FS usage
    try:
      disk_total = 0
      disk_free  = 0
      for entry in raw_metrics['node_filesystem_size_bytes']:
        if not ('roots' in entry['labels']['device'] or 'HarddiskVolume' in entry['labels']['device']):
          disk_total += entry['value']
      for entry in raw_metrics['node_filesystem_free_bytes']:
        if not ('roots' in entry['labels']['device'] or 'HarddiskVolume' in entry['labels']['device']):
          disk_free += entry['value']
      metrics['disk_total'] = disk_total
      metrics['disk_free']  = disk_free
      metrics['disk_used']  = disk_total - disk_free
      metrics['disk_free_percent'] = disk_free / disk_total * 100
      metrics['disk_used_percent'] = metrics['disk_used'] / disk_total * 100
    except:
      MSG_ERROR (self, "Failed to parse Disk Usage info")
    return metrics

  @property
  def db (self):
    return self.__db

  @property
  def cluster (self):
    return self.__cluster

  @property
  def queue (self):
    return self.__queue