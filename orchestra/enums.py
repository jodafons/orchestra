
__all__ = [ "Status" , "Signal" ]

from Gaugi import EnumStringification


#
# job status
#
class Status ( EnumStringification ):
  HOLD       = "hold"
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
  REMOVED    = "removed"


#
# signal sent to the user
#
class Signal ( EnumStringification ):
  RETRY      = "retry"
  KILL       = "kill"
  WAITING    = "waiting"
  DELETE     = "delete"


