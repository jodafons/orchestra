# Exports
__all__ = [
  'MaestroAPI'
]

# Imports
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from flask_login import LoginManager, current_user, login_user
from sqlalchemy import create_engine
from orchestra.constants import *
from orchestra import OrchestraDB, Board
from hashlib import sha256
from werkzeug.utils import secure_filename
import subprocess
from Gaugi import Logger, StringLogger
from http import HTTPStatus

class MaestroAPI (Logger):

  def __init__ (self):

    self.__app = Flask (__name__)
    self.__app.secret_key = "pxgTWHQEaA28qz95"
    self.__db = OrchestraDB()
    self.__api = Api(self.__app)
    self.__login = LoginManager(self.__app)

    db = self.__db

    ###
    class Authenticate (Resource):
      def post(self):
        if current_user.is_authenticated:
          return jsonify(
            error_code=v,
            message="User is already authenticated!"
          )
        else:
          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.UNAUTHORIZED,
              message="Authentication failed!"
            )
          password = request.form['password']

          if (user.getUserName() == request.form['username']) and (password == user.getPasswordHash()):
            try:
              login_user(user, remember=False)
            except:
              return jsonify(
                error_code=HTTPStatus.UNAUTHORIZED,
                message="Failed to login."
              )
            return jsonify(
              error_code=HTTPStatus.OK,
              message="Authentication successful!"
            )
          return jsonify(
            error_code=HTTPStatus.UNAUTHORIZED,
            message="Authentication failed!"
          )
    ###

    ###
    class ListDatasets (Resource):
      def post (self):
        # TODO:
        # if current_user.is_authenticated:
        if True:
          from Gaugi import Color
          from prettytable import PrettyTable

          username = request.form['username']

          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="User not found."
            )

          t = PrettyTable([ Color.CGREEN2 + 'Username' + Color.CEND,
                            Color.CGREEN2 + 'Dataset'  + Color.CEND,
                            Color.CGREEN2 + 'Files' + Color.CEND])
          for ds in db.getAllDatasets( username ):
            t.add_row(  [username, ds.dataset, len(ds.files)] )
          return jsonify(
            error_code=HTTPStatus.OK,
            message=t.get_string()
          )
    ###

    ###
    class ListTasks (Resource):
      def post (self):
        # TODO:
        # if current_user.is_authenticated:
        if True:
          from Gaugi import Color
          from prettytable import PrettyTable

          username = request.form['username']

          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="User not found."
            )

          def getStatus(status):
            if status == 'registered':
              return Color.CWHITE2+"REGISTERED"+Color.CEND
            elif status == 'assigned':
              return Color.CWHITE2+"ASSIGNED"+Color.CEND
            elif status == 'testing':
              return Color.CGREEN2+"TESTING"+Color.CEND
            elif status == 'running':
              return Color.CGREEN2+"RUNNING"+Color.CEND
            elif status == 'done':
              return Color.CGREEN2+"DONE"+Color.CEND
            elif status == 'failed':
              return Color.CGREEN2+"DONE"+Color.CEND
            elif status == 'killed':
              return Color.CRED2+"KILLED"+Color.CEND
            elif status == 'finalized':
              return Color.CRED2+"FINALIZED"+Color.CEND
            elif status == 'broken':
              return Color.CRED2+"BROKEN"+Color.CEND
            elif status == 'hold':
              return Color.CRED2+"HOLD"+Color.CEND

          t = PrettyTable([ Color.CGREEN2 + 'Username'    + Color.CEND,
                            Color.CGREEN2 + 'Taskname'    + Color.CEND,
                            Color.CGREEN2 + 'Registered'  + Color.CEND,
                            Color.CGREEN2 + 'Assigned'    + Color.CEND,
                            Color.CGREEN2 + 'Testing'     + Color.CEND,
                            Color.CGREEN2 + 'Running'     + Color.CEND,
                            Color.CRED2   + 'Failed'      + Color.CEND,
                            Color.CGREEN2 + 'Done'        + Color.CEND,
                            Color.CRED2   + 'kill'        + Color.CEND,
                            Color.CRED2   + 'killed'      + Color.CEND,
                            Color.CGREEN2 + 'Status'      + Color.CEND,
                            ])

          tasks = self.__db.session().query(Board).filter( Board.username==username ).all()
          for b in tasks:
            if len(b.taskName)>80:
              taskname = b.taskName[0:55]+' ... '+ b.taskName[-20:]
            else:
              taskname = b.taskName
            t.add_row(  [username, taskname, b.registered,  b.assigned, b.testing, b.running, b.failed,  b.done, b.kill, b.killed, getStatus(b.status)] )
          return jsonify(
            error_code=HTTPStatus.OK,
            message=t.get_string()
          )
    ###

    self.__api.add_resource(Authenticate, '/authenticate')
    self.__api.add_resource(ListDatasets, '/list-datasets')
    self.__api.add_resource(ListTasks, '/list-tasks')

  def run (self):
    self.__app.run (host = '0.0.0.0', port = API_PORT)



  # class CopyFile (Resource):
  #   def post(self):
  #     if current_user.is_authenticated:
  #       # Get file and assure file name is OK
  #       receivedFile = request.files['file']
  #       filename = secure_filename(receivedFile.filename)
  #       destination_dir = CLUSTER_VOLUME + current_user.getUserName() + '/files'

  #       # If dir doesn't exist, creates it
  #       if not os.path.exists(destination_dir):
  #         os.makedirs(destination_dir)
        
  #       # Save received file
  #       receivedFile.save(os.path.join(destination_dir, filename))

  #       return jsonify(
  #         error_code=ERR_OK,
  #         message="File saved successfully."
  #       )
  #     else:
  #       return jsonify(
  #         error_code=ERR_UNAUTHORIZED,
  #         message="Please authenticate."
  #       )


#
# TODO>
#
# * Build endpoint for authentication
#
# * Build endpoint for the "list" command:
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')

    # from Gaugi import Color
    # from prettytable import PrettyTable
    # t = PrettyTable([ Color.CGREEN2 + 'Username' + Color.CEND,
    #                   Color.CGREEN2 + 'Dataset'  + Color.CEND,
    #                   Color.CGREEN2 + 'Files' + Color.CEND])

    # # Loop over all datasets inside of the username
    # for ds in self.__db.getAllDatasets( username ):
    #   t.add_row(  [username, ds.dataset, len(ds.files)] )
    # print(t)
#
# * Build endpoint for deleting datasets, checking if both dataset and users exist:
    # ds = self.__db.getDataset( username, datasetname )
    # if ds.task_usage:
    #   MSG_FATAL( self, "This is a task dataset and can not be removed. Please use task delete." )
    # for file in ds.getAllFiles():
    #   self.__db.session().query(File).filter( File.id==file.id ).delete()
    # self.__db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()
    # file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
    # file_dir = file_dir.replace('//','/')
    # if os.path.exists(file_dir):
    #   command = 'rm -rf {FILE}'.format(FILE=file_dir)
    #   os.system(command)
    # else:
    #   MSG_WARNING(self, "This dataset does not exist into the database (%s)", file_dir)
    # self.__db.commit()
#
# * Build endpoint for downloading datasets, checking if both dataset and users exist:
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # if not self.__db.getDataset( username, datasetname ):
    #   MSG_FATAL( self, "The dataset exist into the database")
    # file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
    # if not os.path.exists(file_dir):
    #   MSG_FATAL(self, "This dataset does not exist into the database (%s)", file_dir)
    # os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=file_dir,DESTINATION=datasetname) )
#
# * Build endpoint for uploading datasets, checking if both dataset and users exist:
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # if self.__db.getDataset( username, datasetname ):
    #   MSG_FATAL( self, "The dataset exist into the database")
    # try:
    #   ds  = Dataset( username=username, dataset=datasetname, cluster=self.__db.getCluster())
    #   filename = path
    #   destination_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
    #   destination_dir = destination_dir.replace('//','/')
    #   if not os.path.exists(destination_dir):
    #     os.makedirs(destination_dir)
    #   os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=filename, DESTINATION=destination_dir) )
    #   for path in expandFolders(destination_dir):
    #     MSG_INFO( self, "Registry %s into %s", path,datasetname)
    #     hash_object = hashlib.md5(str.encode(path))
    #     ds.addFile( File(path=path, hash=hash_object.hexdigest()) )
    #   self.__db.createDataset(ds)
    #   self.__db.commit()
    # except:
    #     MSG_FATAL( self, "Impossible to registry the dataset(%s)", datasetname)
#
# * Build endpoint for creating new tasks:
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # # Check if the task exist into the databse
    # if self.__db.getUser(username).getTask(taskname) is not None:
    #   MSG_FATAL( self, "The task exist into the database. Abort.")
    # # check data (file) is in database
    # if self.__db.getDataset(username, dataFile) is None:
    #   MSG_FATAL( self, "The file (%s) does not exist into the database. Should be registry first.", dataFile)
    # # check configFile (file) is in database
    # if self.__db.getDataset(username, configFile) is None:
    #   MSG_FATAL( self, "The config file (%s) does not exist into the database. Should be registry first.", configFile)
    # # Get the secondary data as dict
    # secondaryDS = eval(secondaryDS)
    # # check secondary data paths exist is in database
    # for key in secondaryDS.keys():
    #   if self.__db.getDataset(username, secondaryDS[key]) is None:
    #     MSG_FATAL( self, "The secondary data file (%s) does not exist into the database. Should be registry first.", secondaryDS[key])
    # # check exec command policy
    # if not '%DATA' in execCommand:
    #   MSG_FATAL( self,  "The exec command must include '%DATA' into the string. This will substitute to the dataFile when start.")
    # if not '%IN' in execCommand:
    #   MSG_FATAL( self, "The exec command must include '%IN' into the string. This will substitute to the configFile when start.")
    # if not '%OUT' in execCommand:
    #   MSG_FATAL( self, "The exec command must include '%OUT' into the string. This will substitute to the outputFile when start.")
    # # parser the secondary data in the exec command
    # for key in secondaryDS.keys():
    #   if not key in execCommand:
    #     MSG_FATAL( selrf, "The exec command must include %s into the string. This will substitute to %s when start",key, secondaryDS[key])
    # # check if the output exist into the dataset base
    # if self.__db.getDataset(username, taskname ):
    #   MSG_FATAL( self, "The output dataset exist. Please, remove than or choose another name for this task", taskname )
    # # Check if the board monitoring for this task exist into the database
    # if self.__db.session().query(Board).filter( Board.taskName==taskname ).first():
    #   MSG_FATAL( self, "There is a board monitoring with this taskname. Contact the administrator." )
    # # check if task exist into the storage
    # outputFile = CLUSTER_VOLUME +'/'+username+'/'+taskname
    # if os.path.exists(outputFile):
    #   MSG_WARNING(self, "The task dir exist into the storage. Beware!")
    # else:
    #   # create the task dir
    #   MSG_INFO(self, "Creating the task dir in %s", outputFile)
    #   os.system( 'mkdir %s '%(outputFile) )
    # # create the task into the database
    # if not dry_run:
    #   try:
    #     user = self.__db.getUser( username )
    #     task = self.__db.createTask( user, taskname, configFile, dataFile, taskname,
    #                         containerImage, self.__db.getCluster(),
    #                         secondaryDataPath=secondaryDS,
    #                         templateExecArgs=execCommand,
    #                         etBinIdx=et,
    #                         etaBinIdx=eta,
    #                         isGPU=gpu,
    #                         )
    #     task.setStatus('hold')
    #     configFiles = self.__db.getDataset(username, configFile).getAllFiles()
    #     _dataFile = self.__db.getDataset(username, dataFile).getAllFiles()[0].getPath()
    #     _dataFile = _dataFile.replace( CLUSTER_VOLUME, '/volume' ) # to docker path
    #     _outputFile = '/volume/'+username+'/'+taskname # to docker path
    #     _secondaryDS = {}
    #     for key in secondaryDS.keys():
    #       _secondaryDS[key] = self.__db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()
    #       _secondaryDS[key] = _secondaryDS[key].replace(CLUSTER_VOLUME, '/volume') # to docker path
    #     for idx, file in enumerate(configFiles):
    #       _configFile = file.getPath()
    #       _configFile = _configFile.replace(CLUSTER_VOLUME, '/volume') # to docker path
    #       command = execCommand
    #       command = command.replace( '%DATA' , _dataFile  )
    #       command = command.replace( '%IN'   , _configFile)
    #       command = command.replace( '%OUT'  , _outputFile)
    #       for key in _secondaryDS:
    #         command = command.replace( key  , _secondaryDS[key])
    #       job = self.__db.createJob( task, _configFile, idx, execArgs=command, isGPU=gpu, priority=-1 )
    #     ds  = Dataset( username=username, dataset=taskname, cluster=self.__db.getCluster(), task_usage=True)
    #     ds.addFile( File(path=outputFile, hash='' ) ) # the real path
    #     self.__db.createDataset(ds)
    #     self.createBoard( user, task )
    #     task.setStatus('registered')
    #     self.__db.commit()
    #   except Exception as e:
    #     MSG_FATAL(self, e)
#
# * Build an endpoint for deleting tasks
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # try:
    #   user = self.__db.getUser( username )
    # except:
    #   MSG_FATAL( self , "The user name (%s) does not exist into the data base", username)
    # try:
    #   task = self.__db.getTask( taskname )
    # except:
    #   MSG_FATAL( self, "The task name (%s) does not exist into the data base", args.taskname)
    # id = task.id
    # try:
    #   self.__db.session().query(Job).filter(Job.taskId==id).delete()
    # except Exception as e:
    #   MSG_FATAL( self, "Impossible to remove Job lines from (%d) task", id)
    # try:
    #   self.__db.session().query(Task).filter(Task.id==id).delete()
    # except Exception as e:
    #   MSG_FATAL( self, "Impossible to remove Task lines from (%d) task", id)
    # try:
    #   self.__db.session().query(Board).filter(Board.taskId==id).delete()
    # except Exception as e:
    #   MSG_WARNING( self, "Impossible to remove Task board lines from (%d) task", id)
    # ds = self.__db.getDataset( username, taskname )
    # if not ds.task_usage:
    #   MSG_FATAL( self, "This dataset is not task usage. There is something strange..." )
    # for file in ds.getAllFiles():
    #   self.__db.session().query(File).filter( File.id==file.id ).delete()
    # self.__db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()
    # file_dir = CLUSTER_VOLUME + '/' + username + '/' + taskname
    # file_dir = file_dir.replace('//','/')
    # if os.path.exists(file_dir):
    #   command = 'rm -rf {FILE}'.format(FILE=file_dir)
    #   print(command)
    # else:
    #   MSG_WARNING(self, "This dataset does not exist into the database (%s)", file_dir)
    # self.__db.commit()
#
# * Build an endpoint for retrying tasks
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # try:
    #   user = self.__db.getUser( username )
    # except:
    #   MSG_FATAL( self , "The user name (%s) does not exist into the data base", username)
    # try:
    #   task = self.__db.getTask( taskname )
    #   if task.getStatus() == Status.BROKEN:
    #     for job in task.getAllJobs():
    #       job.setStatus(Status.REGISTERED)
    #     task.setStatus(Status.REGISTERED)
    #   else:
    #     for job in task.getAllJobs():
    #       if ( (job.getStatus() == Status.FAILED) or (job.getStatus() == Status.KILL) or (job.getStatus() == Status.KILLED) ):
    #         job.setStatus(Status.ASSIGNED)
    #     task.setStatus(Status.RUNNING)
    # except:
    #   MSG_FATAL( self, "The task name (%s) does not exist into the data base", args.taskname)
    # self.__db.commit()
#
# * Build an endpoint for listing all tasks related to an username
    # if username in self.__db.getAllUsers():
    #   MSG_FATAL( self, 'The username does not exist into the database. Please, report this to the db manager...')
    # from Gaugi import Color
    # def getStatus(status):
    #   if status == 'registered':
    #     return Color.CWHITE2+"REGISTERED"+Color.CEND
    #   elif status == 'assigned':
    #     return Color.CWHITE2+"ASSIGNED"+Color.CEND
    #   elif status == 'testing':
    #     return Color.CGREEN2+"TESTING"+Color.CEND
    #   elif status == 'running':
    #     return Color.CGREEN2+"RUNNING"+Color.CEND
    #   elif status == 'done':
    #     return Color.CGREEN2+"DONE"+Color.CEND
    #   elif status == 'failed':
    #     return Color.CGREEN2+"DONE"+Color.CEND
    #   elif status == 'killed':
    #     return Color.CRED2+"KILLED"+Color.CEND
    #   elif status == 'finalized':
    #     return Color.CRED2+"FINALIZED"+Color.CEND
    #   elif status == 'broken':
    #     return Color.CRED2+"BROKEN"+Color.CEND
    #   elif status == 'hold':
    #     return Color.CRED2+"HOLD"+Color.CEND
    # from prettytable import PrettyTable
    # t = PrettyTable([ Color.CGREEN2 + 'Username'    + Color.CEND,
    #                   Color.CGREEN2 + 'Taskname'    + Color.CEND,
    #                   Color.CGREEN2 + 'Registered'  + Color.CEND,
    #                   Color.CGREEN2 + 'Assigned'    + Color.CEND,
    #                   Color.CGREEN2 + 'Testing'     + Color.CEND,
    #                   Color.CGREEN2 + 'Running'     + Color.CEND,
    #                   Color.CRED2   + 'Failed'      + Color.CEND,
    #                   Color.CGREEN2 + 'Done'        + Color.CEND,
    #                   Color.CRED2   + 'kill'        + Color.CEND,
    #                   Color.CRED2   + 'killed'      + Color.CEND,
    #                   Color.CGREEN2 + 'Status'      + Color.CEND,
    #                   ])
    # tasks = self.__db.session().query(Board).filter( Board.username==username ).all()
    # for b in tasks:
    #   if len(b.taskName)>80:
    #     taskname = b.taskName[0:55]+' ... '+ b.taskName[-20:]
    #   else:
    #     taskname = b.taskName
    #   t.add_row(  [username, taskname, b.registered,  b.assigned, b.testing, b.running, b.failed,  b.done, b.kill, b.killed, getStatus(b.status)] )
    # print(t)
#
# * Build an endpoint for deleting tasks
    # try:
    #   user = self.__db.getUser( username )
    # except:
    #   MSG_FATAL( self , "The user name (%s) does not exist into the data base", username)
    # if taskname=='all':
    #   MSG_INFO( self, "Remove all tasks inside of %s username", username )
    #   for task in user.getAllTasks():
    #     for job in task.getAllJobs():
    #       if job.getStatus()==Status.RUNNING or job.getStatus()==Status.TESTING:
    #         job.setStatus(Status.KILL)
    #       else:
    #         job.setStatus(Status.KILLED)
    # else:
    #   if taskname.split('.')[0] != 'user':
    #     MSG_FATAL( self, 'The task name must starts with: user.%USER.taskname.')
    #     task = self.__db.getTask( taskname )
    #     if not task:
    #       MSG_FATAL( self, "The task name (%s) does not exist into the data base", args.taskname)
    #     for job in task.getAllJobs():
    #       if job.getStatus()==Status.RUNNING or job.getStatus()==Status.TESTING:
    #         job.setStatus(Status.KILL)
    #       else:
    #         job.setStatus(Status.KILLED)
    # self.__db.commit()