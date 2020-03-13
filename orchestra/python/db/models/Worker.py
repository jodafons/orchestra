
__all__=[
  'Worker',
  'roles_workers'
]

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey, Table
from sqlalchemy.orm import relationship
from orchestra.db.models import Base, Task
from flask_login import UserMixin

db = SQLAlchemy()

roles_workers = Table(
  'roles_workers',
  Base.metadata,
  Column('worker_id', Integer(), ForeignKey('worker.id')),
  Column('role_id', Integer(), ForeignKey('role.id'))
)

#
#   Users Table
#
class Worker (object, db.Model, UserMixin, Base):

  __tablename__ = 'worker'

  # Local
  id = Column(Integer, primary_key = True)
  username = Column(String, unique = True)
  maxPriority = Column( Integer )
  passwordHash = Column(String)
  email = Column(String, unique = True)

  # Foreign
  tasks = relationship("Task", order_by="task.id", back_populates="user")
  roles = relationship('Role', secondary=roles_workers,
              backref=db.backref('workers', lazy='dynamic'))


  def __repr__ (self):
    return "<User {}, priority {}>".format(self.username, self.maxPriority)

  # Method that adds tasks into user
  def addTask (self, task):
    self.tasks.append(task)

  # Method that gets all tasks from user
  def getAllTasks (self, cluster=None):
    if cluster:
      cluster_tasks=[]
      for task in self.tasks:
        if task.cluster==cluster:
          cluster_tasks.append(task)
      return cluster_tasks
    else:
      return self.tasks


  def getTask (self, taskName):
    for task in self.getAllTasks():
      if taskName == task.getTaskName():
        return task
    return None


  def getUserName(self):
    return self.username

  def setUserName(self, name ):
    self.username = name

  def getPasswordHash (self):
    return self.passwordHash

  def getMaxPriority(self):
    return self.maxPriority

  def setMaxPriority(self, value):
    self.maxPriority = value



