__all__=['Node']



from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
import datetime


#
#   Node Table
#
class Node (Base):

  __tablename__ = 'node'

  id                = Column(Integer, primary_key = True)
  name              = Column(String)

  enabledCPUSlots      = Column( Integer )
  enabledGPUSlots      = Column( Integer )

  maxNumberOfCPUSlots  = Column( Integer )
  maxNumberOfGPUSlots  = Column( Integer )
  
  master               = Column( Boolean, default=False )


  timer = Column(DateTime)

  # Signal column to be user to retry, delete or kill functions
  signal = Column( String, default='waiting' )


  def getName(self):
    return self.name


  def getMaxNumberOfSlots(self, gpu=False):
    return self.maxNumberOfGPUSlots if gpu else self.maxNumberOfCPUSlots


  def getNumberOfEnabledSlots(self, gpu=False):
    return self.enabledGPUSlots if gpu else self.enabledCPUSlots


  def ping(self):
    self.timer = datetime.datetime.now()


  def isAlive(self):
    if self.timer is None:
      return False
    else:
      return True  if (datetime.datetime.now() - self.timer).total_seconds() < 60 else False


  def getSignal(self):
    return self.signal

  def setSignal(self, value):
    self.signal = value



  def setThisNodeAsMaster( self ):
    self.master=True

  def setThisNodeAsSlave( self ):
    self.master=False

  def isMaster(self):
    return self.master


