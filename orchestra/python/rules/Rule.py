

__all__ = ["Rule"]

from Gaugi import Logger
from Gaugi.messenger.macros import *


class Rule(Logger):

  def __init__(self):

    Logger.__init__(self)


  def setDatabase(self,db):
    self._db = db

  def db(self):
    return self.db


  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS


  # rules( user, task, status = [StatusJob.REGISTED] )
  def __call__(self, user, task, status=None ):
    pass


