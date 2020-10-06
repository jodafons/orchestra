__all__=['Node']



from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base

#
#   Node Table
#
class Node (Base):

  __tablename__ = 'node'

  id                = Column(Integer, primary_key = True)
  queueName         = Column( String )
  name              = Column(String)
  enabledSlots      = Column( Integer )
  maxNumberOfSlots  = Column( Integer )
  isGPU             = Column( Boolean, default=False )


  def getName(self):
    return self.name


  def getMaxNumberOfSlots(self):
    return self.maxNumberOfSlots


  def getNumberOfEnabledSlots(self):
    return self.enabledSlots


  def getQueueName(self):
    return self.queueName





