__all__=['Node']



from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
from orchestra.db.models.Worker import db

#
#   Node Table
#
class Node (Base, db.Model):

  __tablename__ = 'node'

  id        = Column(Integer, primary_key = True)
  
  queueName = Column( String )
  name      = Column(String)
  jobs = Column( Integer )
  maxJobs = Column( Integer )
  cluster = Column( String )
  completedJobs = Column( Integer, default=0 )
  failedJobs = Column( Integer, default=0 )
  
  ip = Column( String )


  def getName(self):
    return self.name


  def getMaxJobs(self):
    return self.maxJobs


  def getJobs(self):
    return self.jobs


  def getQueueName(self):
    return self.queueName


  def completed( self ):
    self.completedJobs+=1


  def failed( self ):
    self.failedJobs+=1


  def getCluster( self ):
    return self.cluster




