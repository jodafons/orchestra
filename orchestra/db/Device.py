__all__=['Device']


from sqlalchemy import Column, Integer, String, DateTime
from orchestra.db import Base
import datetime

class Device (Base):

  __tablename__ = 'device'

  id          = Column(Integer, primary_key = True)
  nodename    = Column(String)
  slots       = Column( Integer )
  enabled     = Column( Integer )
  gpu         = Column( Integer , default=-1)

  timer   = Column(DateTime)

  def ping(self):
    self.timer = datetime.datetime.now()

  def is_alive(self):
    return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False







