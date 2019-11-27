
__all__ = [ "Status" , "Cluster"]

from Gaugi import EnumStringification


class Status ( EnumStringification ):

  HOLDED = "holded"
  BROKEN = "broken"
  FAILED = "failed"
  KILLED = "killed"
  DONE = "done"
  REGISTERED = "registered"
  TESTING = "testing"
  ASSIGNED = "assigned"
  ACTIVATED = "activated"
  PENDING = "pending"
  STARTING = "starting"
  RUNNING  = "running"
  FINALIZED = "finalized"




class Cluster( EnumStringification ):
  LPS = 'LPS'
  SDUMONT = 'SDUMONT'
  LOBOC = 'LOBOC'
  LCG = 'LCG'


