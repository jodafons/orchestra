

__all__ = ["LCGRule"]

from orchestra.rules import Rule
from orchestra.enumerations import *
from orchestra.db.models import Job
from Gaugi.messenger.macros import *
from sqlalchemy import and_, or_

class LCGRule(Rule):

  def __init__(self):
    Rule.__init__(self)



  # rules( user, task, status = [StatusJob.REGISTED] )
  def __call__(self, db, user):

    # LCG rule taken from: https://twiki.cern.ch/twiki/bin/view/PanDA/PandaAthena#Job_priority
    # this must be ordered by creation (date). First must be the older one
    # The total number of the user's subJobs existing in the whole queue. (existing = job status is one of
    # defined,assigned,activated,sent,starting,running)

    # The total number of jobs for this user in running or assigned status
    T = len(db.session().query(Job).filter(Job.userId==user.id).filter(or_(Job.status==Status.ASSIGNED , Job.status==Status.RUNNING ) ).all() )
    tasks = user.getAllTasks()
    jobs = db.session().query(Job).filter(Job.userId==user.id).filter( Job.status==Status.ASSIGNED ).all()
    for n, job in enumerate(jobs):
      # This is the LCG rule
      priority = user.getMaxPriority() - (T+n)/5.
      job.setPriority(priority)
      n+=1


    db.commit()
    MSG_INFO(self, "Update all priorities.")




