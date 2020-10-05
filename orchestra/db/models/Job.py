
__all__ = ["Job"]

from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
import datetime

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
    queueName = Column( String )

    priority = Column(Integer)
    retry = Column(Integer, default=0)

    # Foreign
    task = relationship("Task", back_populates="jobs")
    taskId = Column(Integer, ForeignKey('task.id'))
    userId = Column(Integer)


    timer = Column(DateTime)


    def __repr__ (self):
        return "<Job (configFilePath='{}', status='{}, taskId = {}, configId = {}', Priority = {})>".format(
            self.configFilePath, self.status, self.taskId, self.configId, self.priority
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


    def getPriority(self):
      return self.priority


    def getTask(self):
      return self.task


    def getQueueName(self):
      return self.queueName


    def getTaskName(self):
      return self.getTask().getTaskName()


    def getUserName(self):
      return self.getTask().getUser().getUserName()


    def ping(self):
      self.timer = datetime.datetime.now()


    def isAlive(self):
      if self.timer is None:
        return False
      else:
        return True  if (datetime.datetime.now() - self.timer).total_seconds() < 30 else False

    #
    # Get the output file path for this job into the storage
    #
    def getTheOutputStoragePath(self):
      return self.getTask().getTheOutputStoragePath() + "/" + "job_configId_%d"%self.configId


