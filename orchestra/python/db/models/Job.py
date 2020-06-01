
__all__ = ["Job"]

from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from orchestra.db.models import Base
from orchestra.db.models.Worker import db

#
#   Jobs Table
#
class Job (Base, db.Model):
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



    def setTimer(self, value):
      self.timer = value

    def getTimer(self):
      return timer


    def getQueueName(self):
      return self.queueName


    def getTaskName(self):
      return self.getTask().getTaskName()


    def getUserName(self):
      return self.getTask().getUser().getUserName()


    def getTheOutputStoragePath(self):
      from orchestra.constants import OUTPUT_DIR
      return self.getUserName() + "/" + self.getTaskName() + "/" + OUTPUT_DIR%self.configId



