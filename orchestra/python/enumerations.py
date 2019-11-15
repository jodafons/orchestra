
__all__ = [ "Status" ]

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





