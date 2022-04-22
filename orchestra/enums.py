
__all__ = [ "State" , "Signal" ]





#
# job status
#
class State:
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
class Signal:
  RETRY      = "retry"
  KILL       = "kill"
  WAITING    = "waiting"
  DELETE     = "delete"


