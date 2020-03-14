
__all__=['Worker']


from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey, Table, Boolean
from sqlalchemy.orm import relationship, backref
from orchestra.db.models import Base, Role
from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

#roles_workers = Table(
#  'roles_workers',
#  Base.metadata,
#  Column('worker_id', Integer(), ForeignKey('Worker.id')),
#  Column('role_id', Integer(), ForeignKey('Role.id'))
#)

#
#   Roles_Workers Table
#
class RolesWorkers (Base, db.Model):

  __tablename__ = 'roles_workers'

  # Local
  id = Column(Integer, primary_key = True)

  # Foreign
  worker_id = Column(Integer, ForeignKey('worker.id'))
  role_id = Column(Integer, ForeignKey('role.id'))


#
#   Users Table
#
class Worker (Base, db.Model, UserMixin):

  __tablename__ = 'worker'

  # Local
  id = Column(Integer, primary_key = True)
  username = Column(String, unique = True)
  maxPriority = Column( Integer )
  password = Column(String)
  email = Column (String)
  active = Column(Boolean)

  # Foreign
  tasks = relationship("Task", order_by="Task.id", back_populates="user")
  roles = relationship("Role", secondary='roles_workers',
              backref=backref('workers', lazy='dynamic'))

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


