
__all__=['Worker']


from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey, Table, Boolean
from sqlalchemy.orm import relationship, backref
from orchestra.db.models import Base


#
#   Users Table
#
class Worker (Base):

    __tablename__ = 'worker'
  
    # Local
    id = Column(Integer, primary_key = True)
    username = Column(String, unique = True)
    email = Column (String)
    volume = Column (String)
  
    # Foreign
    tasks = relationship("Task", order_by="Task.id", back_populates="user")
  
  
  
    #
    # add task
    #
    def addTask (self, task):
      self.tasks.append(task)
  
  
    def getAllTasks (self, cluster=None):
      return self.tasks
  
  
    #
    # get the task from the name
    #
    def getTask (self, taskName):
      for task in self.getAllTasks():
        if taskName == task.getTaskName():
          return task
      return None
  
  
    #
    # setter and getter username
    #
    def getUserName(self):
      return self.username
  
    def setUserName(self, name ):
      self.username = name
  
  
    #
    # setter and getter volume
    #
    def setVolume(self, volume):
      self.volume = volume
  
    def getVolume(self):
      return self.volume
  
