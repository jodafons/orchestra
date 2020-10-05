__all__=['Task']


from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey, JSON, DateTime
from sqlalchemy.orm import relationship
from orchestra.db.models import Base, Job
import datetime

#
#   Tasks Table
#
class Task (Base):

    __tablename__ = 'task'
  
    # Local
    id = Column(Integer, primary_key = True)
    taskName = Column(String, unique=True)
  
    inputFilePath = Column(String)
    outputFilePath = Column(String)
    configFilePath = Column(String)
    containerImage = Column(String)
  
    # For LPS grid
    templateExecArgs   = Column( String, default="" )
  
    # Useful for extra data paths
    secondaryDataPath = Column( JSON, default="{}" )
  
    # For task status
    status = Column(String, default="registered")
  
    queueName = Column( String )
  
    # Foreign
    jobs = relationship("Job", order_by="Job.id", back_populates="task")
    userId = Column(Integer, ForeignKey('worker.id'))
    user = relationship("Worker", back_populates="tasks")
  
  
    # Signal column to be user to retry, delete or kill functions
    signal = Column( String, default='waiting' )
  
  
  
  
  
    #
    # Method that adds jobs into task
    #
    def addJob (self, job):
      self.jobs.append(job)
  
  
    #
    # Method that gets all jobs from task
    #
    def getAllJobs (self):
      return self.jobs
  
  
    #
    # Method that gets single task from user
    #
    def getJob (self, configId):
      try:
        for job in self.jobs:
          if job.configId == configId:
            return job
        return None
      except:
        return None
  
  
  
    def getStatus(self):
      return self.status
  
    def setStatus(self,status):
      self.status = status
  
  
    def setTaskName(self, value):
      self.taskName = value
  
    def getTaskName(self):
      return self.taskName
  
  
    def setTemplateExecArgs(self, value):
      self.templateExecArgs = value
  
    def getTemplateExecArgs(self):
      return self.templateExecArgs
  
  
  
    def getSignal(self):
      return self.signal
  
    def setSignal(self, value):
      self.signal = value
  
  
    def getUser(self):
      return self.user
  
  
  
    def getQueueName(self):
      return self.queueName
  
  
    def getContainerImage(self):
      return self.containerImage
  


    def getTheOutputStoragePath(self):
      return self.outputFilePath



