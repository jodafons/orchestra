
__all__ = ["Job"]

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from orchestra.db import Base
import datetime

#
#   Jobs Table
#
class Job (Base):

    __tablename__ = 'job'

    # Local
    id      = Column(Integer, primary_key = True)
    command = Column(String , default="")
    state   = Column(String , default="registered")
    retry   = Column(Integer, default=0)
    jobid   = Column(Integer)


    # Foreign
    task    = relationship("Task", back_populates="jobs")
    taskid  = Column(Integer, ForeignKey('task.id'))
    timer   = Column(DateTime)



    def volume(self):
      return self.task.volume + "/" + "job." + str(self.id).zfill(6)


    def ping(self):
      self.timer = datetime.datetime.now()


    def is_alive(self):
      return True  if (self.timer and ((datetime.datetime.now() - self.timer).total_seconds() < 30)) else False


