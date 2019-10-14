

__all__ = ["Rule"]

from Gaugi import Logger, NotSet
from Gaugi.messenger.macros import *


class Rule(Logger):

  def __init__(self):

    Logger.__init__(self)
    self.__db = NotSet


  def setDatabase(self,db):
    self.__db = db


  def db(self):
    return self.__db


  def initialize(self):
    return StatusCode.SUCCESS


  def execute(self):
    return StatusCode.SUCCESS


  def finalize(self):
    return StatusCode.SUCCESS




