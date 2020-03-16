# Exports
__all__ = [
  'MaestroAPI'
]

# Imports
from flask import Flask, request, jsonify, send_file, Response
from flask_restful import Resource, Api
from flask_login import LoginManager, current_user, login_user
from sqlalchemy import create_engine
from orchestra.constants import *
from orchestra import OrchestraDB
from orchestra.db import *
from orchestra import Status
from hashlib import sha256, md5
from werkzeug.utils import secure_filename
import subprocess
from Gaugi import Logger, StringLogger, expandFolders
from http import HTTPStatus
import pickle
import base64
from pathlib import Path
import os
import hmac
import hashlib
import base64
from passlib.context import CryptContext

home = str(Path.home())
text_type = str
salt = "Krj6cd2mW63pER7Hjy8bsUbXYLY6t"

def get_pw_context ():
  pw_hash = 'pbkdf2_sha512'
  schemes = ['bcrypt', 'des_crypt', 'pbkdf2_sha256', 'pbkdf2_sha512', 'sha256_crypt', 'sha512_crypt', 'plaintext']
  deprecated = ['auto']
  return CryptContext (schemes=schemes, default=pw_hash, deprecated=deprecated)

def encode_string(string):
  if isinstance(string, text_type):
    string = string.encode('utf-8')
  return string

def get_hmac (password):
  h = hmac.new(encode_string(salt), encode_string(password), hashlib.sha512)
  return base64.b64encode(h.digest())

def verify_password (password, hash):
  return get_pw_context().verify(get_hmac(password), hash)

def pickledAuth (data, db):
  try:
    pickled = data.decode('utf-8')
  except:
    pickled = data
  auth_data = pickle.loads(base64.b64decode(pickled.encode()))
  user = db.getUser(auth_data['username'])
  if user is None:
    return jsonify(
      error_code=HTTPStatus.UNAUTHORIZED,
      message="Authentication failed!"
    )
  password = auth_data['password']

  if (user.getUserName() == auth_data['username']) and (password == user.getPasswordHash()):
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
            error_code=HTTPStatus.UNAUTHORIZED,
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

          if (verify_password(password, user.getPasswordHash())):
            return jsonify(
              error_code=HTTPStatus.OK,
              message="Success!"
            )

          return jsonify(
            error_code=HTTPStatus.UNAUTHORIZED,
            message="Auth: {}".format(verify_password(password, user.getPasswordHash()))
          )

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
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

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
    class ListDatasetsPy (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        username = request.form['username']

        user = db.getUser(request.form['username'])
        if user is None:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="User not found."
          )

        datasets = db.getAllDatasets(username)
        datasets = [ds.dataset for ds in datasets]

        import pickle
        import base64
        pickled_datasets = pickle.dumps(datasets)
        b64_pickled_datasets = base64.b64encode(pickled_datasets)

        return Response(b64_pickled_datasets)
    ###

    ###
    class ListTasks (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

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

        tasks = db.session().query(Board).filter( Board.username==username ).all()
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

    ###
    class ListTasksPy (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth


        username = request.form['username']

        user = db.getUser(request.form['username'])
        if user is None:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="User not found."
          )

        tasks = db.session().query(Board).filter( Board.username==username ).all()
        tasks = [task.taskName for task in tasks]
        
        import pickle
        import base64
        pickled_tasks = pickle.dumps(tasks)
        b64_pickled_tasks = base64.b64encode(pickled_tasks)

        return Response(b64_pickled_tasks)
    ###

    ###
    class DeleteDataset (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        import os

        username = request.form['username']
        datasetname = request.form['datasetname']

        ds = db.getDataset( username, datasetname )

        if ds is None:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="Dataset not found on the database."
          )

        if ds.task_usage:
          return jsonify(
            error_code=HTTPStatus.CONFLICT,
            message="This dataset is in use right now, please delete the task related to it."
          )
        for file in ds.getAllFiles():
          db.session().query(File).filter( File.id==file.id ).delete()
        db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()
        file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
        file_dir = file_dir.replace('//','/')
        if os.path.exists(file_dir):
          command = 'rm -rf {FILE}'.format(FILE=file_dir)
          os.system(command)
        else:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="This dataset does not exist in the database ({})".format(file_dir)
          )
        db.commit()
        return jsonify(
          error_code=HTTPStatus.OK,
          message="Successfully deleted."
        )
    ###

    ###
    class DownloadDataset (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        import os

        username = request.form['username']
        datasetname = request.form['datasetname']

        if not db.getDataset( username, datasetname ):
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="This dataset doesn't exist on the database"
          )
        file_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
        file_dir = file_dir.replace('//','/')

        if not os.path.exists(file_dir):
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="Dataset files not found!"
          )

        import zipfile
        from os.path import basename

        def zipdir(path, ziph):
          for root, dirs, files in os.walk(path):
            for file in files:
              full_path = os.path.join(root, file)
              ziph.write(full_path, basename(full_path))

        zipfilename = '{}.zip'.format(datasetname)
        zipfilepath = os.path.join(home, zipfilename)

        zipf = zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED)
        zipdir(file_dir, zipf)
        zipf.close()

        return send_file(zipfilepath, attachment_filename=zipfilename)
    ###

    ###
    class UploadDataset (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        import os

        username = request.form['username']
        datasetname = request.form['datasetname']

        try:
          ds_exists = True
          ds = db.getDataset(username, datasetname)
          if ds is None:
            ds  = Dataset( username=username, dataset=datasetname, cluster=db.getCluster())
            ds_exists = False
          receivedFile = request.files['file']
          filename = secure_filename(receivedFile.filename)
          destination_dir = CLUSTER_VOLUME + '/' + username + '/' + datasetname
          destination_dir = destination_dir.replace('//','/')
          if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
          receivedFile.save(os.path.join(destination_dir, filename))
          path = os.path.join(destination_dir, filename)
          hash_object = md5(str.encode(path))
          file_obj = File(path=path, hash=hash_object.hexdigest())
          for ds_file in ds.getAllFiles():
            if ds_file.hash == file_obj.hash:
              return jsonify (
                error_code=HTTPStatus.CONFLICT,
                message="A file with same name as {} is already on this dataset!".format(filename)
              )
          ds.addFile( file_obj )
          if not ds_exists:
            db.createDataset(ds)
          db.commit()
          return jsonify (
            error_code=HTTPStatus.OK,
            message="File {} successfully uploaded!".format(filename)
          )
        except:
          return jsonify (
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Unknown error when uploading this dataset. Please report."
          )
    ###

    ###
    class CreateTask (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        username = request.form['username']
        taskname = request.form['taskname']
        configFile = request.form['configFile']
        dataFile = request.form['dataFile']
        containerImage = request.form['containerImage']
        secondaryDS = request.form['secondaryDS']
        execCommand = request.form['execCommand']
        et = request.form['et']
        eta = request.form['eta']
        gpu = bool(request.form['gpu'])

        import os

        if db.getUser(username).getTask(taskname) is not None:
          return jsonify(
            error_code = HTTPStatus.CONFLICT,
            message = "The task already exists."
          )

        if db.getDataset(username, dataFile) is None:
          return jsonify(
            error_code = HTTPStatus.NOT_FOUND,
            message = "This dataset doesn't exist in the database, should be registered first."
          )

        if db.getDataset(username, configFile) is None:
          return jsonify(
            error_code = HTTPStatus.NOT_FOUND,
            message = "This config doesn't exist in the database, should be registered first."
          )

        secondaryDS = eval(secondaryDS)

        for key in secondaryDS.keys():
          if db.getDataset(username, secondaryDS[key]) is None:
            return jsonify(
              error_code = HTTPStatus.NOT_FOUND,
              message = "The secondary data file {} is not on the database.".format(secondaryDS[key])
            )

        if not '%DATA' in execCommand:
          return jsonify(
            error_code = HTTPStatus.BAD_REQUEST,
            message = "The exec command must include '%DATA' on it, this shall be replaced by the dataFile when the jobs starts."
          )
        if not '%IN' in execCommand:
          return jsonify(
            error_code = HTTPStatus.BAD_REQUEST,
            message = "The exec command must include '%IN' on it, this shall be replaced by the configFile when the jobs starts."
          )
        if not '%OUT' in execCommand:
          return jsonify(
            error_code = HTTPStatus.BAD_REQUEST,
            message = "The exec command must include '%OUT' on it, this shall be replaced by the outputFile when the jobs starts."
          )

        for key in secondaryDS.keys():
          if not key in execCommand:
            return jsonify(
              error_code = HTTPStatus.BAD_REQUEST,
              message = "The exec command must include '{}' on it, this shall be replaced by {} when the job starts.".format(key, secondaryDS[key])
            )

        if db.getDataset(username, taskname):
          return jsonify(
            error_code = HTTPStatus.BAD_REQUEST,
            message = "The output data file already exists. Please remove it or choose another name."
          )

        if db.session().query(Board).filter( Board.taskName==taskname ).first():
          return jsonify(
            error_code = HTTPStatus.BAD_REQUEST,
            message = "There's already a board monitoring with this taskname, please contact the administrators."
          )

        outputFile = CLUSTER_VOLUME +'/'+username+'/'+taskname
        if not os.path.exists(outputFile):
          os.makedirs(outputFile)

        try:
          user = db.getUser( username )
          task = db.createTask( user, taskname, configFile, dataFile, taskname,
                              containerImage, db.getCluster(),
                              secondaryDataPath=secondaryDS,
                              templateExecArgs=execCommand,
                              etBinIdx=et,
                              etaBinIdx=eta,
                              isGPU=gpu,
                              )
          task.setStatus('hold')
          configFiles = db.getDataset(username, configFile).getAllFiles()
          _dataFile = db.getDataset(username, dataFile).getAllFiles()[0].getPath()
          _dataFile = _dataFile.replace( CLUSTER_VOLUME, '/volume' )
          _outputFile = '/volume/'+username+'/'+taskname
          _secondaryDS = {}
          for key in secondaryDS.keys():
            _secondaryDS[key] = db.getDataset(username, secondaryDS[key]).getAllFiles()[0].getPath()
            _secondaryDS[key] = _secondaryDS[key].replace(CLUSTER_VOLUME, '/volume')
          for idx, file in enumerate(configFiles):
            _configFile = file.getPath()
            _configFile = _configFile.replace(CLUSTER_VOLUME, '/volume')
            command = execCommand
            command = command.replace( '%DATA' , _dataFile  )
            command = command.replace( '%IN'   , _configFile)
            command = command.replace( '%OUT'  , _outputFile)
            for key in _secondaryDS:
              command = command.replace( key  , _secondaryDS[key])
            job = db.createJob( task, _configFile, idx, execArgs=command, isGPU=gpu, priority=-1 )
          ds  = Dataset( username=username, dataset=taskname, cluster=db.getCluster(), task_usage=True)
          ds.addFile( File(path=outputFile, hash='' ) )
          db.createDataset(ds)
          self.createBoard( user, task, db )
          task.setStatus('registered')
          db.commit()
          return jsonify(
            error_code=HTTPStatus.OK,
            message="Success!"
          )
        except Exception as e:
          print(e)
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Unknown error"
          )

      #
      # This is for monitoring purpose. Should be used to dashboard view
      #
      def createBoard( self , user, task, db ):

        board = Board( username=user.username, taskId=task.id, taskName=task.taskName )
        board.jobs = len(task.getAllJobs())
        board.registered = board.jobs
        board.assigned=board.testing=board.running=board.failed=board.done=board.killed=0
        board.status = task.status
        db.session().add(board)

    ###

    ###
    class DeleteTask (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        username = request.form['username']
        taskname = request.form['taskname']

        try:
          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="User not found."
            )
        except:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="User not found."
          )

        try:
          task = db.getTask( taskname )
          if task is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="Task not found."
            )
        except:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="Task not found."
          )

        id = task.id

        try:
          db.session().query(Job).filter(Job.taskId==id).delete()
        except Exception as e:
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Impossible to remove jobs from task #{}.".format(id)
          )

        try:
          db.session().query(Task).filter(Task.id==id).delete()
        except Exception as e:
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="Impossible to remove task #{}.".format(id)
          )

        try:
          db.session().query(Board).filter(Board.taskId==id).delete()
        except Exception as e:
          print( "Impossible to remove Task board lines from ({}) task".format(id))

        ds = db.getDataset( username, taskname )
        if not ds.task_usage:
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="The dataset related to this task is not in use. This wasn't supposed to happen. Please report."
          )

        for file in ds.getAllFiles():
          db.session().query(File).filter( File.id==file.id ).delete()

        db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()

        file_dir = CLUSTER_VOLUME + '/' + username + '/' + taskname
        file_dir = file_dir.replace('//','/')
        if os.path.exists(file_dir):
          command = 'rm -rf {FILE}'.format(FILE=file_dir)
        else:
          print (self, "This dataset is not in the database: {}.".format(file_dir))
        db.commit()

        return jsonify(
          error_code = HTTPStatus.OK,
          message = "Success!"
        )
    ###

    ###
    class RetryTask (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        username = request.form['username']
        taskname = request.form['taskname']

        try:
          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="User not found."
            )
        except:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="User not found."
          )

        try:
          task = db.getTask( taskname )
          if task.getStatus() == Status.BROKEN:
            for job in task.getAllJobs():
              job.setStatus(Status.REGISTERED)
            task.setStatus(Status.REGISTERED)
          else:
            for job in task.getAllJobs():
              if ( (job.getStatus() == Status.FAILED) or (job.getStatus() == Status.KILL) or (job.getStatus() == Status.KILLED) ):
                job.setStatus(Status.ASSIGNED)
            task.setStatus(Status.RUNNING)
        except:
          return jsonify(
            error_code = HTTPStatus.NOT_FOUND,
            message = "Task is not in the database."
          )
        db.commit()
        return jsonify(
          error_code = HTTPStatus.OK,
          message = "Success!"
        )
    ###

    ###
    class KillTask (Resource):
      def post (self):
        if not current_user.is_authenticated:
          auth = pickledAuth(request.form['credentials'], db)
          if auth.json['error_code'] != 200:
            return auth

        username = request.form['username']
        taskname = request.form['taskname']

        try:
          user = db.getUser(request.form['username'])
          if user is None:
            return jsonify(
              error_code=HTTPStatus.NOT_FOUND,
              message="User not found."
            )
        except:
          return jsonify(
            error_code=HTTPStatus.NOT_FOUND,
            message="User not found."
          )

        if taskname == 'all':
          for task in user.getAllTasks():
            for job in task.getAllJobs():
              if job.getStatus()==Status.RUNNING or job.getStatus()==Status.TESTING:
                job.setStatus(Status.KILL)
              else:
                job.setStatus(Status.KILLED)
        else:
          try:
            task = db.getTask( taskname )
            if task.getStatus() == Status.BROKEN:
              for job in task.getAllJobs():
                job.setStatus(Status.REGISTERED)
              task.setStatus(Status.REGISTERED)
            else:
              for job in task.getAllJobs():
                if ( (job.getStatus() == Status.FAILED) or (job.getStatus() == Status.KILL) or (job.getStatus() == Status.KILLED) ):
                  job.setStatus(Status.ASSIGNED)
              task.setStatus(Status.RUNNING)
          except:
            return jsonify(
              error_code = HTTPStatus.NOT_FOUND,
              message = "Task is not in the database."
            )
          for job in task.getAllJobs():
            if job.getStatus()==Status.RUNNING or job.getStatus()==Status.TESTING:
              job.setStatus(Status.KILL)
            else:
              job.setStatus(Status.KILLED)
        db.commit()
        return jsonify(
          error_code = HTTPStatus.OK,
          message = "Success!"
        )
    ###

    self.__api.add_resource(Authenticate, '/authenticate')
    self.__api.add_resource(ListDatasets, '/list-datasets')
    self.__api.add_resource(ListDatasetsPy, '/list-datasets-py')
    self.__api.add_resource(DeleteDataset, '/delete-dataset')
    self.__api.add_resource(DownloadDataset, '/download-dataset')
    self.__api.add_resource(UploadDataset, '/upload-dataset')
    self.__api.add_resource(CreateTask, '/create-task')
    self.__api.add_resource(DeleteTask, '/delete-task')
    self.__api.add_resource(RetryTask, '/retry-task')
    self.__api.add_resource(ListTasks, '/list-tasks')
    self.__api.add_resource(ListTasksPy, '/list-tasks-py')
    self.__api.add_resource(KillTask, '/kill-task')

  def run (self):
    self.__app.run (host = '0.0.0.0', port = API_PORT)