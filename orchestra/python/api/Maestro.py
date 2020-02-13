# Imports
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from flask_login import LoginManager, current_user, login_user
from sqlalchemy import create_engine
from orchestra.constants import *
#from orchestra import OrchestraDB, TaskBoard
from orchestra import OrchestraDB
from hashlib import sha256
from werkzeug.utils import secure_filename
import os
import subprocess

# Appeding path
import sys
print(sys.path)
sys.path.append('.')

# Creating Flask app
app = Flask (__name__)

# Initializing DB
db = OrchestraDB()

# Making it an API and adding login manager
api = Api(app)
login = LoginManager(app)

#
# Auxiliar functions
#
def hashPw (password):
  m = sha256()
  m.update(password.encode('utf-8'))
  return m.hexdigest()

#
# API Resources
#

# Authenticate
class Authenticate (Resource):
  def post(self):
    if current_user.is_authenticated:
      return jsonify(
        error_code=ERR_UNAUTHORIZED,
        message="User is already authenticated!"
      )
    else:
      user = db.getUser(request.form['username'])
      password = request.form['password']

      if password == user.getPassword():
        try:
          login_user(user, remember=False)
        except:
          return jsonify(
            error_code=ERR_UNAUTHORIZED,
            message="Failed to login."
          )
        return jsonify(
          error_code=ERR_OK,
          message="Authentication successful!"
        )
      return jsonify(
        error_code=ERR_UNAUTHORIZED,
        message="Authentication failed!"
      )

# Copy file
class CopyFile (Resource):
  def post(self):
    if current_user.is_authenticated:
      # Get file and assure file name is OK
      receivedFile = request.files['file']
      filename = secure_filename(receivedFile.filename)
      destination_dir = CLUSTER_VOLUME + current_user.getUserName() + '/files'

      # If dir doesn't exist, creates it
      if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

      # Save received file
      receivedFile.save(os.path.join(destination_dir, filename))

      return jsonify(
        error_code=ERR_OK,
        message="File saved successfully."
      )
    else:
      return jsonify(
        error_code=ERR_UNAUTHORIZED,
        message="Please authenticate."
      )

# Getting tasks for printing
class Tasks (Resource):
  def get(self):
    # if current_user.is_authenticated:
    if True:
      # try:
      tasks = db.session().query(TaskBoard).all()
      output = {}
      for task in tasks:
        output[task.taskName] = {''}
      return tasks
      # except:
      #   return jsonify(
      #     error_code=ERR_NOT_FOUND,
      #     message="Failed to query for tasks."
      #   )

# Orchestra create
class Create (Resource):
  def post(self):
    # if current_user.is_authenticated:
    if True:
      args_dict = request.form

      # check task policy (user.username)
      taskname = args_dict['task']
      taskname = taskname.split('.')
      if taskname[0] != 'user':
        return jsonify(
          error_code=400,
          message='The task name must start with: user.%USER.taskname.'
        )

      # check task policy (username must exist into the database)
      username = taskname[1]
      if not username in db.getAllUsers():
        return jsonify(
          error_code=400,
          message='The username DOES NOT exist in the database. Please, report this to the cluster staff.'
        )

      # Check if the task exist into the databse
      if db.getUser(username).getTask(args_dict['task']) is not None:
        return jsonify(
          error_code=400,
          message="This task already exists. Abort."
        )

      # check data (file) is in database
      if db.getDataset(username, args_dict['dataFile']) is None:
        return jsonify(
          error_code=400,
          message="The file ({}) does not exist in the database.".format(args_dict['dataFile'])
        )

      # check configFile (file) is in database
      if db.getDataset(username, args_dict['configFile']) is None:
        return jsonify(
          error_code=400,
          message="The config file ({}) does not exist into the database.".format(args_dict['configFile'])
        )

      # Get the secondary data as dict
      secondaryData = eval(args_dict['secondaryData'])

      # check secondary data paths exist is in database
      for key in secondaryData.keys():
        if db.getDataset(username, secondaryData[key]) is None:
          return jsonify(
            error_code=400,
            message="The secondary data file ({}) does not exist into the database.".format(secondaryData[key])
          )

      # check if task exist into the storage
      storagePath = args_dict['storagePath']+'/'+username+'/'+args_dict['task']
      if os.path.exists(storagePath):
        return jsonify(
          error_code=400,
          message="The task dir already exists in the storage. Please contact the cluster staff."
        )
      else:
        # create the task dir
        os.makedirs(storagePath)

      # create the data (file) link in the storage path
      dataLink = db.getDataset(username, args_dict['dataFile']).getAllFiles()[0].getPath()
      os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/', args_dict['dataFile']) )

      # create the secondary data (file) link in the storage path
      for key in secondaryData.keys():
        dataLink = db.getDataset(username, secondaryData[key]).getAllFiles()[0].getPath()
        os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/',secondaryData[key]) )

      # create the output file
      os.system('mkdir %s/%s'%(storagePath,args_dict['outputFile']))

      # create the config file link
      dataLink = db.getDataset(username, args_dict['configFile']).getAllFiles()[0].getPath()
      dataLink = str('/').join(dataLink.split('/')[0:-1]) # get only the dir
      os.system( 'ln -s %s %s/%s'%(dataLink, storagePath+'/',args_dict['configFile']) )

      # Create the base path (docker volume)
      basepath = '/volume/'+args_dict['task']

      # Create the pseudo data path (docker volume)
      dataFile=basepath+'/'+args_dict['dataFile']

      # Create the pseudo output file path (docker volume)
      outputFile = basepath+'/'+args_dict['outputFile']

      # Check the exec command (docker volume)
      execCommand = args_dict['execCommand']

      # check exec command policy
      if not '%DATA' in execCommand:
        return jsonify(
          error_code=400,
          message="The exec command must include '%DATA' into the string. This will substitute to the dataFile when start."
        )
      if not '%IN' in execCommand:
        return jsonify(
          error_code=400,
          message="The exec command must include '%IN' into the string. This will substitute to the configFile when start."
        )
      if not '%OUT' in execCommand:
        return jsonify(
          error_code=400,
          message="The exec command must include '%OUT' into the string. This will substitute to the outputFile when start."
        )

      # parser the secondary data in the exec command
      for key in secondaryData.keys():
        if not key in execCommand:
          return jsonify(
            error_code=400,
            message="The exec command must include {} into the string. This will substitute to {} when start".format(key, secondaryData[key])
          )
        secondaryData[key] = basepath+'/'+ secondaryData[key]

      # create the task into the database
      if not args_dict['dry_run']:
        try:
          user = db.getUser( username )
          task = db.createTask( user, args_dict['task'], args_dict['configFile'], args_dict['dataFile'], args_dict['outputFile'],
                              args_dict['containerImage'], args_dict['cluster'],
                              secondaryDataPath=args_dict['secondaryData'],
                              templateExecArgs=args_dict['execCommand'],
                              etBinIdx=args_dict['et'],
                              etaBinIdx=args_dict['eta'],
                              isGPU=args_dict['gpu'],
                              )
          task.setStatus('hold')
        except:
          return jsonify(
            error_code=400,
            message="Failed to create task."
          )

      # create all jobs. Must be loop over all config files
      configFiles = db.getDataset(username, args_dict['configFile']).getAllFiles()


      for idx, file in enumerate(configFiles):

        # create the pseudo path (docker volume)
        configFile = basepath+'/'+args_dict['configFile']+'/'+file.getPath().split('/')[-1]

        command = execCommand
        command = command.replace( '%DATA' , dataFile  )
        command = command.replace( '%IN'   , configFile)
        command = command.replace( '%OUT'  , outputFile)

        for key in secondaryData:
          command = command.replace( key  , secondaryData[key])
        print("\n"+command+"\n")

        if not args_dict['dry_run']:
          job = db.createJob( task, configFile, idx, execArgs=command, isGPU=args_dict['gpu'], priority=-1 )
          job.setStatus('assigned' if args_dict['bypass_test_job'] else 'registered')


      if not args_dict['dry_run']:
        task.setStatus( 'running' if args_dict['bypass_test_job'] else 'registered'  )
        db.commit()
        db.close()


    else:
      return jsonify(
        error_code=ERR_UNAUTHORIZED,
        message="Please authenticate."
      )

# Adding resources
api.add_resource(Authenticate, '/login')
api.add_resource(CopyFile, '/upload')
api.add_resource(Create, '/create')
#api.add_resource(Tasks, '/tasks')

# Main
if __name__ == '__main__':
  app.run(port=api_port)
