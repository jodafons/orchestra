
__all__ = ["OrchestraDB"]

from Gaugi import Logger, NotSet, StatusCode
from Gaugi.messenger.macros import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orchestra.db.models import *
from orchestra.constants import *
from sqlalchemy import and_, or_
import time

from orchestra.constants import *
from orchestra import Cluster

class OrchestraDB(Logger):

  def __init__( self, cluster=Cluster.LPS, url=CLUSTER_POSTGRES_URL ):

    Logger.__init__(self)
    self.__cluster = cluster

    try: # Get the connection and create an session
      MSG_INFO( self, "Connect to %s.", url )
      self.__engine = create_engine(url)
      Session= sessionmaker(bind=self.__engine)
      self.__session = Session()
    except Exception as e:
      MSG_FATAL( self, e )


  def getCluster(self):
    return self.__cluster


  def getStoragePath(self):

    if self.__cluster is Cluster.LPS:
      return CLUSTER_VOLUME
    elif self.__cluster is Cluster.SDUMONT:
      return CLUSTER_VOLUME_SDUMONT
    else:
      MSG_WARNING( self, "Cluster path not defined.")



  def createTask( self , user, taskName, configFilePath, inputFilePath, outputFilePath, containerImage, cluster,
                  templateExecArgs="{}",
                  secondaryDataPath="{}",
                  etBinIdx=None,
                  etaBinIdx=None,
                  queue='cpu_small'):

    try:
      # Create the task and append into the user area
      desired_id = self.__session.query(Task).order_by(Task.id.desc()).first().id + 1
      task = Task(
        id=desired_id,
        taskName=taskName,
        inputFilePath=inputFilePath,
        outputFilePath=outputFilePath,
        configFilePath=configFilePath,
        containerImage=containerImage,
        # The task always start as registered status
        status='registered',
        cluster=cluster,
        # Extra args
        templateExecArgs=templateExecArgs,
        secondaryDataPath=secondaryDataPath,
        etBinIdx=etBinIdx,
        etaBinIdx=etaBinIdx,
        queueName=queue,
      )
      user.addTask(task)
      return task
    except Exception as e:
      MSG_ERROR(self, e)
      return None



  def createJob( self, task, configFilePath, configId, priority=1000, execArgs="{}" ):

    try:
      desired_id = self.__session.query(Job).order_by(Job.id.desc()).first().id + 1
      job = Job(
        id=desired_id,
        configFilePath=configFilePath,
        containerImage=task.containerImage,
        configId=configId,
        execArgs=execArgs,
        cluster=task.getCluster(),
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

    if not self.isConnected():
      return None

    try:
      return self.session().query(Worker).filter(Worker.username==username).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None


  def getTask( self, taskName ):

    if not self.isConnected():
      return None

    try: # Get the task object using the task name as filter
      return self.session().query(Task).filter(Task.taskName==taskName).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None


  def getAllUsers( self ):
    if not self.isConnected():
      return None

    try:
      return self.session().query(Worker).all()
    except Exception as e:
      MSG_ERROR( self, e)
      return None


  def getAllTasks( self, user ):
    if not self.isConnected():
      return None
    try:
      return user.getAllTasks()
    except Exception as e:
      return None


  def getAllJobs( self, task ):
    if not self.isConnected():
      return None
    try:
      return job.getAllJobs()
    except Exception as e:
      return None




  def isConnected( self ):
    # for _ in range(NUMBER_OF_TRIALS):
    #   try:
    #     self.session().query(Worker).all()
    #     return True
    #   except:
    #     MSG_WARNING(self, "Data base connection is failed... wainting 5 minutes")
    #     time.sleep( 5*MINUTE )
    #     continue
    # return False
    return True


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

    if not self.isConnected():
      return False

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




  def deleteTask( self, taskname ):

    if not self.isConnected():
      return False

    try:
      task = self.getTask( args.taskname  )
    except:
      MSG_ERROR(self, "The task name (%s) does not exist into the data base", taskname)
      return False
    id = task.id
    try:
      self.session().query(Job).filter(Job.taskId==id).delete()
    except Exception as e:
      MSG_ERROR(self, "Impossible to remove Job lines from (%d) task", id)
      return False

    try:
      self.session().query(Task).filter(Task.id==id).delete()
    except Exception as e:
      MSG_ERROR(self, "Impossible to remove Task lines from (%d) task", id)
      return False

    return True



  def getAllMachines(self, cluster , queue_name):
    if not self.isConnected():
      return False

    try:
      return self.session().query(Node).filter(and_( Node.queueName==queue_name, Node.cluster==cluster)).all()
    except Exception as e:
      MSG_ERROR(self, "Impossible to retrieve nodes from database.")
      return []



  def getMachine( self, cluster, queue_name, name ):
    if not self.isConnected():
      return None
    try:
      return self.session().query(Node).filter(Node.cluster==cluster).filter(and_( Node.queueName==queue_name, Node.name==name)).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None



  def createDataset( self, dataset ):

    if not self.isConnected():
      return None

    try:

      desired_id = self.__session.query(Dataset).order_by(Dataset.id.desc()).first().id + 1
      dataset.id = desired_id
      self.session().add(dataset)
      return True
    except Exception as e:
      MSG_ERROR(self, e)
      return False



  def getAllDatasets( self, username):

    if not self.isConnected():
      return None

    try:
      return self.session().query(Dataset).filter(Dataset.username==username).all()
    except Exception as e:
      MSG_ERROR(self, e)
      return None




  def getDataset( self, username, dataset ):

    if not self.isConnected():
      return None

    try:
      return self.session().query(Dataset).filter(and_( Dataset.username==username, Dataset.dataset==dataset) ).first()
    except Exception as e:
      MSG_ERROR(self, e)
      return None






