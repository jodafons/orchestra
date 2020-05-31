# Exports
__all__ = [
  'MaestroAPI'
]

# Imports
from flask import Flask, request, jsonify, send_file, Response
from flask_restful import Resource, Api
from flask_login import LoginManager, login_user
from sqlalchemy import create_engine
from orchestra.constants import *
from orchestra import OrchestraDB
from orchestra.db import *
from orchestra import Status, Signal
from hashlib import sha256, md5
from werkzeug.utils import secure_filename
import subprocess
from Gaugi import Logger, StringLogger, expandFolders
from Gaugi.messenger.macros import *
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
    data = data.decode('utf-8')
  except:
    pass
  finally:
    data = data.split("$")
  username = data[1]
  token = data[2]

  user = db.getUser(username)
  if user is None:
    return jsonify(
      error_code=HTTPStatus.UNAUTHORIZED,
      message="Authentication failed!"
    )

  if not user.has_role('user'):
    return jsonify(
      error_code=HTTPStatus.UNAUTHORIZED,
      message="Your account has not been activated yet."
    )

  cmp_token = "{}-{}".format(user.getUserName(), user.getPasswordHash())
  cmp_token = md5(encode_string(cmp_token)).hexdigest()

  if (user.getUserName() == username) and (token == cmp_token):
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



def get_username_by_name(name):
  return name.split('.')[1]



class MaestroAPI (Logger):

  def __init__ (self):
    Logger.__init__(self)
    self.__app = Flask (__name__)
    self.__app.secret_key = "pxgTWHQEaA28qz95"
    self.__db = OrchestraDB()
    self.__api = Api(self.__app)
    self.__login = LoginManager(self.__app)
    db = self.__db



    ###
    class Authenticate (Resource):
      def post(self):
        user = db.getUser(request.form['username'])

        if user is None:
          return jsonify(
            error_code=HTTPStatus.UNAUTHORIZED,
            message="Authentication failed!"
          )
        password = request.form['password']

        if (user.getUserName() == request.form['username']) and (verify_password(password, user.getPasswordHash())):
          try:
            login_user(user, remember=False)
          except:
            return jsonify(
              error_code=HTTPStatus.UNAUTHORIZED,
              message="Failed to login."
            )
          token = "{}-{}".format(user.getUserName(), user.getPasswordHash())
          token = md5(encode_string(token)).hexdigest()
          return jsonify(
            error_code=HTTPStatus.OK,
            message="Authentication successful!",
            token=token
          )
        return jsonify(
          error_code=HTTPStatus.UNAUTHORIZED,
          message="Authentication failed!"
        )
    ###

    ###
    class ListDatasets (Resource):
      def post (self):
        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        username = request.form['username']
        status, answer = DatasetParser(db).list(username)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message=answer
          )
        else:
          return jsonify(
            error_code=HTTPStatus.OK,
            message=answer.get_string()
          )
    ###


    ###
    class ListDatasetsPy (Resource):
      def post (self):
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

        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        username = request.form['username']
        status, answer = TaskParser(db).list(username)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message=answer
          )
        else:
          return jsonify(
            error_code=HTTPStatus.OK,
            message=answer.get_string()
          )
    ###


    ###
    class ListTasksPy (Resource):
      def post (self):
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
        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        username = request.form['username']
        datasetname = request.form['datasetname']
        status, answer = DatasetParser(db).delete(datasetname)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message = answer
          )
        else:
          return jsonify(
            error_code=HTTPStatus.OK,
            message="Successfully deleted."
          )
    ###


    ###
    class DownloadDataset (Resource):
      def post (self):
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
            desired_id = db.session().query(Dataset).order_by(Dataset.id.desc()).first().id + 1
            ds  = Dataset( id=desired_id, username=username, dataset=datasetname, cluster=db.getCluster())
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
          desired_id = db.session().query(File).order_by(File.id.desc()).first().id + 1
          file_obj = File(id=desired_id, path=path, hash=hash_object.hexdigest())
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
        queue = request.form['queue']
        is_dummy_data = request.form['is_dummy_data']
        is_dummy_config = request.form['is_dummy_config']

        status , answer = TaskParser(db).create( taskname, dataFile, configFile, secondaryDS,
                                                 execCommand,containerImage,et,eta,
                                                 False, False, queue,is_dummy_data,
                                                 is_dummy_config)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message=answer
          )
        else:
          MSG_INFO(self, answer)
          return jsonify(
            error_code = HTTPStatus.OK,
            message = "Success!"
          )



    ###
    class DeleteTask (Resource):
      def post (self):
        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        taskname = request.form['taskname']

        status, answer = TaskParser(db).delete(taskname, remove=False)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message = answer
          )
        else:
          MSG_INFO(self, answer)
          return jsonify(
            error_code = HTTPStatus.OK,
            message = "Success!"
          )

    ###

    ###
    class RetryTask (Resource):
      def post (self):
        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        taskname = request.form['taskname']

        status, answer = TaskParser(db).retry(taskname)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message = answer
          )
        else:
          MSG_INFO(self, answer)
          return jsonify(
            error_code = HTTPStatus.OK,
            message = "Success!"
          )

    ###

    ###
    class KillTask (Resource):

      def post (self):

        auth = pickledAuth(request.form['credentials'], db)
        if auth.json['error_code'] != 200:
          return auth

        taskname = request.form['taskname']

        # kill this task
        status, answer = TaskParser(db).kill(taskname)

        if status.isFailure():
          return jsonify(
            error_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message = answer
          )
        else:
          MSG_INFO(self, answer)
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
