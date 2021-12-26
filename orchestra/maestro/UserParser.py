
__all__ = ["UserParser"]

from Gaugi import LoggingLevel, Logger
from Gaugi.macros import *
from Gaugi import StatusCode, Color, expand_folders

# Connect to DB
from orchestra.db import Task,Dataset,File,Job
from orchestra import Status, Signal, getStatus
from orchestra.db.models import *
from sqlalchemy import and_, or_
from prettytable import PrettyTable

# common imports
import glob
import numpy as np
import argparse
import sys,os
import hashlib




class UserParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:
      
      create_parser = argparse.ArgumentParser(description = 'User create command lines.' , add_help = False)
      create_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                  help = "The name of the user.")
      create_parser.add_argument('-e','--email', action='store', dest='email', required=True,
                                  help = "The user email.")


      delete_parser = argparse.ArgumentParser(description = 'User remove command lines.', add_help = False)
      delete_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                   help = "The dataset name to be removed")


      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'List all users command lines.', add_help = False)
      list_parser.add_argument('-u', '--user', action='store', dest='name', required=False,
                                   help = "List all attributes for this user")


      init_parser = argparse.ArgumentParser(description = 'Initialize the database.', add_help = False)

      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('init', parents=[init_parser])
      args.add_parser( 'user', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'user':
      if args.option == 'create':
        status, answer = self.create(args.name, args.email)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)


      elif args.option == 'delete':
        status, answer = self.delete(args.name)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'list':
        status, answer = self.list(args.name)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      elif args.option == 'init':
        status, answer = self.init()
        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)
        
      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def list( self, username ):

    t = PrettyTable([ Color.CGREEN2 + 'Username' + Color.CEND,
                      Color.CGREEN2 + 'email'  + Color.CEND])

    # Loop over all datasets inside of the username
    for user in self.__db.getAllUsers():
      t.add_row(  [user.username, user.email] )
    return (StatusCode.SUCCESS, t)


  #
  # Unregistry a dataset
  #
  def create( self, username, email ):

    if not self.__db.createUser( username, email ):
      return (StatusCode.FATAL, 'Failed to create the user into the database')
    
    return (StatusCode.SUCCESS, "Successfully created." )


  def delete( self, username ):

    MSG_WARNING(self, "Not implemented yet." )


  def init(self):

    from orchestra import getConfig

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    config = getConfig()
    engine = create_engine(config["postgres"])
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    session.commit()
    session.close()


    return (StatusCode.SUCCESS, "Successfully initialized." )



