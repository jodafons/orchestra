

from orchestra import Monitoring
from pprint import pprint


kube = Monitoring('../../data/lps_cluster.yaml')



d = kube.utilization()
pprint(d)


print( kube.getCPUConsume() )
print( kube.getMemoryConsume() )


