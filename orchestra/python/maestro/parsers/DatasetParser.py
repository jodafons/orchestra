
__all__ = ["DatasetParser"]

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import csvStr2List, expandFolders
from Gaugi import load
from Gaugi import StatusCode

# Connect to DB
from orchestra.db import OrchestraDB
from orchestra.db import Dataset,File
from orchestra import Status, Cluster


# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib
import argparse



class DatasetParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:
      # Upload dataset using the dataset CLI
      upload_parser = argparse.ArgumentParser(description = 'Dataset upload command lines.' , add_help = False)
      upload_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                  help = "The dataset name used to registry into the database. (e.g: user.jodafons...)")
      upload_parser.add_argument('-p','--path', action='store', dest='path', required=True,
                                  help = "The path to the dataset")
      # Download dataset using the dataset CLI
      download_parser = argparse.ArgumentParser(description = 'Dataset donwload command lines.', add_help = False)
      download_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                   help = "The dataset name to be downloaded")
      # Delete dataset using the dataset CLI
      delete_parser = argparse.ArgumentParser(description = 'Dataset Delete command lines.', add_help = False)
      delete_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                   help = "The dataset name to be removed")
      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'Dataset List command lines.', add_help = False)
      list_parser.add_argument('-u', '--user', action='store', dest='username', required=True,
                                   help = "List all datasets for a selected user.")



      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('upload', parents=[upload_parser])
      subparser.add_parser('download', parents=[download_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      args.add_parser( 'castor', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'castor':
      if args.option == 'upload':
        status, answer = self.upload(args.datasetname, args.path)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'download':
        status, answer = self.download(args.datasetname)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'delete':
        status, answer = self.delete(args.datasetname)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'list':
        status, answer = self.list(args.username)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def list( self, username ):

    if not username in [user.getUserName() for user in self.__db.getAllUsers()]:
      return (StatusCode.FATAL, 'The username does not exist into the database. Please, report this to the db manager...')

    from Gaugi import Color
    from prettytable import PrettyTable
    t = PrettyTable([ Color.CGREEN2 + 'Username' + Color.CEND,
                      Color.CGREEN2 + 'Dataset'  + Color.CEND,
                      Color.CGREEN2 + 'Files' + Color.CEND])

    # Loop over all datasets inside of the username
    for ds in self.__db.getAllDatasets( username ):
      t.add_row(  [username, ds.dataset, len(ds.files)] )

    return (StatusCode.SUCCESS, t)





  def delete( self, datasetname ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The dataset name must starts with user.$USER.taskname.')

    username = datasetname.split('.')[1]
    if not username in [user.getUserName() for user in self.__db.getAllUsers()]:
      return (StatusCode.FATAL, 'The username does not exist into the database. Please, report this to the db manager...')

    if not self.__db.getDataset( username, datasetname ):
      return (StatusCode.FATAL, "The dataset exist into the database")

    # prepare to remove from database
    ds = self.__db.getDataset( username, datasetname )

    if ds.task_usage:
      return (StatusCode.FATAL, "This is a task dataset and can not be removed. Please use task delete." )


    for file in ds.getAllFiles():
      try:
        # Delete the file inside of the dataset
        self.__db.session().query(File).filter( File.id==file.id ).delete()
      except Exception as e:
        MSG_ERROR(self, e)
        return (StatusCode.FATAL, "It's not possible to remove the file. %s"%e )


    try:
      # Delete the dataset
      self.__db.session().query(Dataset).filter( Dataset.id==ds.id ).delete()
    except Exception as e:
      MSG_ERROR(self, e)
      return (StatusCode.FATAL, "It's not possible to remove the dataset. %s"%e )


    # The path to the dataset in the cluster
    file_dir = self.__db.volume() + '/' + username + '/' + datasetname
    file_dir = file_dir.replace('//','/')


    # Delete the file from the storage
    # check if this path exist
    try:
      if os.path.exists(file_dir):
        command = 'rm -rf {FILE}'.format(FILE=file_dir)
        os.system(command)
      else:
        MSG_WARNING(self, "This dataset does not exist into the database (%s)", file_dir)
    except Exception as e:
      MSG_ERROR(self,  e)
      return (StatusCode.FATAL, "Unknown error. %s")

    self.__db.commit()
    return (StatusCode.SUCCESS, "Successfully deleted." )



  #
  # Download the dataset in the cluster storage to the current directory
  #
  def download( self, datasetname ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The dataset name must starts with: user.%USER.taskname.')

    username = datasetname.split('.')[1]
    if not username in [ user.getUserName() for user in self.__db.getAllUsers()]:
      return (StatusCode.FATAL, 'The username does not exist into the database. Please, report this to the db manager...')

    if not self.__db.getDataset( username, datasetname ):
      return (StatusCode.FATAL, "The dataset exist into the database")


    # The path to the dataset in the cluster
    file_dir = self.__db.volume() + '/' + username + '/' + datasetname

    # check if this path exist
    if not os.path.exists(file_dir):
      MSG_FATAL(self, "This dataset does not exist into the database (%s)"%file_dir )

    # copy to the current directory
    os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=file_dir,DESTINATION=datasetname) )

    return (StatusCode.SUCCESS, "Successfully downloaded." )




  #
  # Upload and create the dataset into the cluster storage/database
  #
  def upload( self , datasetname, path ):

    # check task policy
    if datasetname.split('.')[0] != 'user':
      return (StatusCode.FATAL, 'The dataset name must starts with: user.%USER.taskname.')

    username = datasetname.split('.')[1]

    if not username in [ user.getUserName() for user in self.__db.getAllUsers()]:
      return (StatusCode.FATAL, 'The username does not exist into the database. Please, report this to the db manager...')

    if self.__db.getDataset( username, datasetname ):
      return (StatusCode.FATAL, "The dataset exist into the database")

    # Let's registry and upload into the database
    try:
      # Create the new dataset
      ds  = Dataset( id=self.__db.generateId(Dataset),username=username, dataset=datasetname, cluster=self.__db.getCluster())

      # check if file exist into the storage
      # Get file and assure file name is OK
      filename = path
      destination_dir = self.__db.volume() + '/' + username + '/' + datasetname

      # treat path string with duplicate /
      destination_dir = destination_dir.replace('//','/')

      # If dir doesn't exist, creates it
      if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

      os.system( 'cp -r {FILE} {DESTINATION}'.format(FILE=filename, DESTINATION=destination_dir) )
      # Loop over files
      desired_id = self.__db.generateId(File)+1
      for idx, path in enumerate(expandFolders(destination_dir)):
        MSG_INFO( self, "Registry %s into %s", path,datasetname)
        hash_object = hashlib.md5(str.encode(path))
        file= File(path=path, hash=hash_object.hexdigest(),id=desired_id+idx)
        ds.addFile(file)

      self.__db.session().add(ds)
      #self.__db.createDataset(ds)
      self.__db.commit()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Impossible to registry the dataset(%s)."%datasetname)

    return (StatusCode.SUCCESS, "Successfully uploaded." )


