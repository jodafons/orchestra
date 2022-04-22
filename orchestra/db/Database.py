
__all__ = ["Database"]


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db import Task, Job, Device
from orchestra.utils import *
import traceback




class Database:

  def __init__( self, url):

    self.url=url
    try:
      MSG_INFO(url)
      self.__engine = create_engine(url)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      traceback.print_exc()
      print(e)

  def __del__(self):
    self.commit()
    self.close()


  def session(self):
    return self.__session

  def commit(self):
    self.session().commit()


  def close(self):
    self.session().close()


  def reconnect(self):
    self.close()
    try:
      self.__engine = create_engine(self.url)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      traceback.print_exc()
      print(e)


  

  def create_task( self, taskname, volume ):
                         
    try:
      task = Task(
        id=self.generateId(Task),
        taskname=taskname,
        volume=volume,
        state='registered',
      )
      #self.session().add(task)
      return task
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None


  def create_job( self, task, jobname, inputfile, command, state='registered', id=None):

    try:
      job = Job(
        id=self.generateId(Job) if id is None else id,
        command=command,
        jobname = jobname,
        inputfile=inputfile,
        state=state
      )
      task+=job
      return job
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None


  def create_device( self, nodename, enabled, slots, device=-1 ):

    try:
      device = Device(nodename=nodename, enabled=enabled, slots=slots, gpu=device)
      self.session().add(device)
      self.commit()
      return device
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None


  def task( self, taskname ):
    try:
      return self.session().query(Task).filter(Task.taskname==taskname).first()
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None


  def tasks(self):
    try:
      return self.session().query(Task).all()
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None


  def devices(self):
    try:
      return self.session().query(Device).all()
    except Exception as e:
      traceback.print_exc()
      print(e)
      return None



  def generateId( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0











