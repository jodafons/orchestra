

__all__ = ["NoRule"]

from orchestra.rules import Rule
from orchestra.enumerations import *
from Gaugi.messenger.macros import *


class NoRule(Rule):

  def __init__(self):
    Rule.__init__(self)



  # rules( user, task, status = [StatusJob.REGISTED] )
  def __call__(self, db, user):
    pass
