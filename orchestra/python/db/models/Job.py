
__all__ = ["Job"]

from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from orchestra.db.models import Base


#
#   Jobs Table
#
class Job (Base):
    __tablename__ = 'job'

    # Local
    id = Column(Integer, primary_key = True)

    containerImage = Column(String)

    # Job configuration for this job
    configFilePath = Column(String)
    # For CERN grid cluster
    configId = Column(Integer)


    # For LPS grid cluster
    execArgs = Column(String,default="")


    status = Column(String, default="registered")
    cluster  = Column( String )
    isGPU = Column(Boolean, default=False)
    priority = Column(Integer)
    retry = Column(Integer, default=0)


    # Foreign
    task = relationship("Task", back_populates="jobs")
    taskId = Column(Integer, ForeignKey('task.id'))




    def __repr__ (self):
        return "<Job (configFilePath='{}', status='{}, taskId = {}, configId = {}')>".format(
            self.configFilePath, self.status, self.taskId, self.configId
        )

    def getStatus(self):
        return self.status


    def setStatus(self, status):
        self.status = status


    def getConfigPath (self):
        return self.configFilePath


    def getTask(self):
      return self.task


    def setPriority(self, priority):
      self.priority = priority


    def getPriority(sefl):
      return self.priority


    def getTask(self):
      return self.task


