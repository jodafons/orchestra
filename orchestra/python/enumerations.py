
__all__ = [ "Status" , "Cluster", "Queue"]

from Gaugi import EnumStringification


class Status ( EnumStringification ):

  HOLDED     = "holded"
  BROKEN     = "broken"
  FAILED     = "failed"
  KILL       = "kill"
  KILLED     = "killed"
  DONE       = "done"
  REGISTERED = "registered"
  TESTING    = "testing"
  ASSIGNED   = "assigned"
  ACTIVATED  = "activated"
  PENDING    = "pending"
  STARTING   = "starting"
  RUNNING    = "running"
  FINALIZED  = "finalized"




class Cluster( EnumStringification ):
  LPS = 'LPS'
  SDUMONT = 'SDUMONT'



class Queue( EnumStringification ):

  # For LPS rancher cluster
  LPS = 'lps'
  # For SDumont cluster
  CPU = 'cpu'
  CPU_SMALL = 'cpu_small'
  NVIDIA  = 'nvidia'
  BULL_SEQUANA = 'gdl'


