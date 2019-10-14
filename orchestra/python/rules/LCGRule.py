

__all__ = ["LCGRule"]

from lpsgrid.engine.rules import Rule
from Gaugi.messenger.macros import *
from lpsgrid.engine.enumerations import *

class LCGRule(Rule):

  def __init__(self):
    Rule.__init__(self)



  # rules( user, task, status = [StatusJob.REGISTED] )
  def __call__(self, db, user):

    # LCG rule taken from: https://twiki.cern.ch/twiki/bin/view/PanDA/PandaAthena#Job_priority
    # this must be ordered by creation (date). First must be the older one
    # The total number of the user's subJobs existing in the whole queue. (existing = job status is one of 
    # defined,assigned,activated,sent,starting,running)
    T = self.getTotalJobsExisting(user)
    n = 0
    for task in user.getAllTasks():
      for job in task.getAllJobs():
        if job.getStatus()==Status.ASSIGNED:
          # This is the LCG rule
          priority = user.getMaxPriority() - (T+n)/5.
          job.setPriority(priority)
          n+=1
        elif job.getStatus()==Status.RUNNING:
          MSG_INFO(self, "We dont need to caculate running jobs since this still into the slot.")
        else: # REGISTERED, DONE, BROKEN, KILLED
          job.setPriority(-1)


    db.commit()




  # Calculate the number of total jobs running by user
  def getTotalJobsExisting( self, user ):
    total=0
    for task in user.getAllTasks():
      for job in task.getAllJobs():
        if( (job.getStatus() == Status.RUNNING) or
           (job.getStatus() == Status.REGISTERED) or
           (job.getStatus() == Status.ASSIGNED) ):
          total+=1
    return total

