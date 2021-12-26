
__all__ = ["OrchestraDB"]

from Gaugi import Logger, StatusCode
from Gaugi.macros import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db.models import *
#from orchestra.constants import *
from sqlalchemy import and_, or_
import time






class OrchestraDB(Logger):

  def __init__( self, url):

    Logger.__init__(self)
    try:
      self.__engine = create_engine(url)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      MSG_FATAL( self, e )




  def createTask( self , user, 
                         taskName, 
                         configFilePath, 
                         inputFilePath, 
                         outputFilePath, 
                         containerImage, 
                         templateExecArgs="{}",
                         secondaryDataPath="{}",
                         queueName='cpu_small'
                         ):

    try:

      task = Task(
        id=self.generateId(Task),
        taskName=taskName,
        inputFilePath=inputFilePath,
        outputFilePath=outputFilePath,
        configFilePath=configFilePath,
        containerImage=containerImage,
        # The task always start as registered status
        status='registered',
        # Extra args
        templateExecArgs=templateExecArgs,
        secondaryDataPath=secondaryDataPath,
        queueName=queueName,
      )
      user.addTask(task)
      return task
    except Exception as e:
      MSG_ERROR(self, e)
      return None






  def createJob( self, task, configFilePath, configId, priority=1000, execArgs="{}", id=None ):

    try:
      job = Job(
        id=self.generateId(Job) if id is None else id,
        configFilePath=configFilePath,
        containerImage=task.containerImage,
        configId=configId,
        execArgs=execArgs,
        retry=0,
        status="registered",
        priority=priority,
        userId = task.userId,
        queueName = task.getQueueName(),
      )
      task.addJob(job)
      return job
    except Exception as e:
      MSG_ERROR( self, e)
      return None


  def getUser( self, username ):
    try:
      return self.session().query(Worker).filter(Worker.username==username).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None



  def getTask( self, taskName ):
    try:
      return self.session().query(Task).filter(Task.taskName==taskName).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None



  def getAllUsers( self ):
    try:
      return self.session().query(Worker).all()
    except Exception as e:
      MSG_ERROR( self, e)
      return None



  def getAllTasks( self, user ):
    try:
      return user.getAllTasks()
    except Exception as e:
      return None



  def getAllJobs( self, task ):
    try:
      return job.getAllJobs()
    except Exception as e:
      return None



  def session(self):
    return self.__session



  def commit(self):
    self.session().commit()


  def close(self):
    self.session().close()


  def initialize( self ):
    return StatusCode.SUCCESS


  def execute( self ):
    return StatusCode.SUCCESS


  def finalize( self ):
    self.commit()
    self.close()
    return StatusCode.SUCCESS


  def retryTask( self, taskname ):
    try:
      task = self.getTask( args.taskname  )
    except:
      MSG_ERROR(self, "The task name (%s) does not exist into the data base", taskname)
      return False
    try:
      for job in task.getAllJobs():
        if job.getStatus() == 'failed':
          job.setStatus('assigned')
    except Exception as e:
      MSG_ERROR(self, "Impossible to assigned all failed jobs. error: %s",e)
      return False

    return True


  def deleteTask( self, taskname ):

    try:
      task = self.getTask( args.taskname  )
    except:
      MSG_ERROR(self, "The task name (%s) does not exist into the data base", taskname)
      return False

    id = task.id
    
    # delete all jobs into this task
    try:
      self.session().query(Job).filter(Job.taskId==id).delete()
    except Exception as e:
      MSG_ERROR(self, "Impossible to remove Job lines from (%d) task", id)
      return False

    # delete the task it self
    try:
      self.session().query(Task).filter(Task.id==id).delete()
    except Exception as e:
      MSG_ERROR(self, "Impossible to remove Task lines from (%d) task", id)
      return False

    return True



  def getAllNodes(self):
    try:
      return self.session().query(Node).all()
    except Exception as e:
      MSG_ERROR(self, e)
      return []



  def getNode( self, nodeName ):
    try:
      return self.session().query(Node).filter(Node.name==nodeName).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None



  def generateId( self, model  ):
    if self.session().query(model).all():
      return self.session().query(model).order_by(model.id.desc()).first().id + 1
    else:
      return 0




  def createDataset( self, dataset ):
    try:
      self.session().add(dataset)
      return True
    except Exception as e:
      MSG_ERROR(self, e)
      return False



  def getAllDatasets( self, username):
    try:
      return self.session().query(Dataset).filter(Dataset.username==username).all()
    except Exception as e:
      MSG_ERROR(self, e)
      return None




  def getDataset( self, username, dataset ):
    try:
      return self.session().query(Dataset).filter(and_( Dataset.username==username, Dataset.dataset==dataset) ).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None




  def createUser( self, username, email ):

    try:
      if self.getUser(username) is None:
        worker = Worker(username=username,email=email,volume='')
        self.session().add(worker)
        self.commit()
        return True
      else:
        MSG_ERROR( self, "The user with name (%s) exist into the database.", username )
        return False
    except Exception as e:
      MSG_ERROR(self, e)
      return False



  def createNode( self, nodename, enabledCPUSlots, maxNumberOfCPUSlots, enabledGPUSlots, maxNumberOfGPUSlots ):

    try:
      if self.getNode(nodename) is None:
        node = Node(name=nodename, 
                    enabledCPUSlots=enabledCPUSlots, 
                    maxNumberOfCPUSlots=maxNumberOfCPUSlots,
                    enabledGPUSlots=enabledGPUSlots, 
                    maxNumberOfGPUSlots=maxNumberOfGPUSlots,
                    )
        self.session().add(node)
        self.commit()
        return True
      else:
        MSG_ERROR( self, "The node (%s) with queue (%s) exist into the database", nodename, queuename )
        return False
    except Exception as e:
      MSG_ERROR(self, e)
      return False




