
__all__ = ["DatasetParser"]

from Gaugi import LoggingLevel, Logger
from Gaugi.macros import *
from Gaugi import StatusCode, Color, expand_folders

# Connect to DB
from orchestra.db import Task,Dataset,File,Job
from orchestra import Status, Signal, getStatus
from sqlalchemy import and_, or_
from prettytable import PrettyTable

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib

from orchestra.utils import getConfig
config = getConfig()




class DatasetParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:
      # Upload dataset using the dataset CLI
      registry_parser = argparse.ArgumentParser(description = 'Dataset registry command lines.' , add_help = False)
      registry_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                  help = "The dataset name used to registry into the database. (e.g: user.jodafons...)")
      registry_parser.add_argument('-p','--path', action='store', dest='path', required=True,
                                  help = "The path to the dataset")

      # Delete dataset using the dataset CLI
      unregistry_parser = argparse.ArgumentParser(description = 'Dataset unregistry command lines.', add_help = False)
      unregistry_parser.add_argument('-d', '--dataset', action='store', dest='datasetname', required=True,
                                   help = "The dataset name to be removed")

      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'Dataset List command lines.', add_help = False)
      list_parser.add_argument('-u', '--user', action='store', dest='username', required=False, default=config['username'],
                                   help = "List all datasets for a selected user.")



      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('registry', parents=[registry_parser])
      subparser.add_parser('unregistry', parents=[unregistry_parser])
      subparser.add_parser('list', parents=[list_parser])
      args.add_parser( 'castor', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'castor':
      if args.option == 'registry':
        status, answer = self.registry(args.datasetname, args.path)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)


      elif args.option == 'unregistry':
        status, answer = self.unregistry(args.datasetname)

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

    t = PrettyTable([ Color.CGREEN2 + 'Username' + Color.CEND,
                      Color.CGREEN2 + 'Dataset'  + Color.CEND,
                      Color.CGREEN2 + 'Files' + Color.CEND])


    # Loop over all datasets inside of the username
    for ds in self.__db.getAllDatasets( username ):
      t.add_row(  [username, ds.dataset, len(ds.files)] )

    return (StatusCode.SUCCESS, t)


  #
  # Unregistry a dataset
  #
  def unregistry( self, datasetname ):

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


    for file in ds.getAllFiles():
      try:
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


    self.__db.commit()
    return (StatusCode.SUCCESS, "Successfully deleted." )



  #
  # registry a dataset
  #
  def registry( self , datasetname, path ):

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
      ds  = Dataset( id=self.__db.generateId(Dataset),username=username, dataset=datasetname )

      # If dir doesn't exist, creates it
      if not os.path.exists(path):
        return (StatusCode.FATAL, "The path (%s) does not exist."%path )

      # Loop over files
      desired_id = self.__db.generateId(File)+1
      for idx, subpath in enumerate(expand_folders(path)):
        MSG_INFO( self, "Registry %s into %s", subpath,datasetname)
        file= File(path=subpath,id=desired_id+idx)
        ds.addFile(file)

      self.__db.session().add(ds)
      self.__db.commit()
    except Exception as e:
      MSG_ERROR(self,e)
      return (StatusCode.FATAL, "Impossible to registry the dataset(%s)."%datasetname)

    return (StatusCode.SUCCESS, "Successfully uploaded." )


