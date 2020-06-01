
from orchestra import Schedule, Pilot, LCGRule, Status
from orchestra.db import OrchestraDB
from orchestra import Cluster, Queue
from orchestra.kubernetes import Orchestrator
from orchestra.constants import *
from pprint import pprint

#orchestrator  = Orchestrator( "../data/job_template.yaml",  "../data/lps_cluster.yaml" )
orchestrator  = Orchestrator( "/home/rancher/.cluster/orchestra/external/partitura/data/job_template.yaml",
                              "/home/rancher/.cluster/orchestra/external/partitura/data/lps_cluster.yaml" )



#nodes = orchestrator.getNodeStatus()

#pprint(nodes)

logs = orchestrator.logs("nvidia.user.mverissi.3764e201d86245fa0574644def5c20fa-pt9x4", 'mverissi')
print(len(logs))

f = open('test.log','w')
f.write(logs[0])
f.close()


