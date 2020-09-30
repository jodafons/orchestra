__all__=['Node']



from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base

#
#   Node Table
#
class Node (Base):

  __tablename__ = 'node'

  id        = Column(Integer, primary_key = True)
  
  queueName = Column( String )
  name      = Column(String)
  jobs      = Column( Integer )
  maxJobs   = Column( Integer )


  def getName(self):
    return self.name


  def getMaxJobs(self):
    return self.maxJobs


  def getJobs(self):
    return self.jobs


  def getQueueName(self):
    return self.queueName





