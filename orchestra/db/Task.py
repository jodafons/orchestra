__all__=['Task']


from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from orchestra.db import Base

#
#   Tasks Table
#
class Task (Base):

    __tablename__ = 'task'
  
    # Local
    id       = Column(Integer, primary_key = True)
    taskname = Column(String, unique=True)
    volume   = Column(String)
    # For task status
    state    = Column(String, default="registered")
    # Foreign
    jobs     = relationship("Job", order_by="Job.id", back_populates="task")
    # Signal column to be user to retry, delete or kill functions
    signal = Column( String, default='waiting' )
  
  
    #
    # Method that adds jobs into task
    #
    def __add__ (self, job):
      self.jobs.append(job)
  
  
