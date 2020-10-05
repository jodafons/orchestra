
__all__ = ["NodeParser"]

from Gaugi.messenger import LoggingLevel, Logger
from Gaugi.messenger.macros import *
from Gaugi import StatusCode, Color, expandFolders

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




class NodeParser( Logger ):

  def __init__(self, db, args=None):

    Logger.__init__(self)
    self.__db = db
    if args:
      
      create_parser = argparse.ArgumentParser(description = 'Node create command lines.' , add_help = False)
      create_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                  help = "The name of the node.")
      create_parser.add_argument('-q','--queue', action='store', dest='queue', required=True,
                                  help = "The name of the queue.")
      create_parser.add_argument('-j','--jobs', action='store', dest='jobs', required=True,
                                  help = "The number of jobs (slots).")
      create_parser.add_argument('-m','--maxJobs', action='store', dest='maxJobs', required=True,
                                  help = "The total number of jobs (slots) for this node.")




      delete_parser = argparse.ArgumentParser(description = 'Node remove command lines.', add_help = False)
      delete_parser.add_argument('-n', '--name', action='store', dest='name', required=True,
                                   help = "The dataset name to be removed")
      delete_parser.add_argument('-q','--queue', action='store', dest='queue', required=True,
                                  help = "The name of the queue.")
 

      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'List all users command lines.', add_help = False)
      list_parser.add_argument('-q','--queue', action='store', dest='queue', required=False, default=None, 
                                  help = "The name of the queue.")
 


      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list', parents=[list_parser])
      args.add_parser( 'node', parents=[parent] )



  def compile( self, args ):
    # Dataset CLI
    if args.mode == 'node':
      if args.option == 'create':
        status, answer = self.create(args.name, args.queue, args.jobs, args.maxJobs)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)


      elif args.option == 'delete':
        status, answer = self.delete(args.name, args.queue)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          MSG_INFO(self, answer)

      elif args.option == 'list':
        status, answer = self.list(args.queue)

        if status.isFailure():
          MSG_FATAL(self, answer)
        else:
          print(answer)

      else:
        MSG_FATAL(self, "Not valid option.")




  #
  # List datasets
  #
  def list( self, queuename=None ):

    t = PrettyTable([ Color.CGREEN2 + 'Queue'     + Color.CEND,
                      Color.CGREEN2 + 'Node'      + Color.CEND,
                      Color.CGREEN2 + 'Slots'     + Color.CEND,
                      Color.CGREEN2 + 'Max slots' + Color.CEND,
                      ])

    # Loop over all datasets inside of the username
    for node in self.__db.getAllNodes(queuename):
      t.add_row(  [node.queueName, node.name, node.jobs, node.maxJobs] )
    return (StatusCode.SUCCESS, t)


  #
  # create a node
  #
  def create( self, nodename, queuename, jobs, maxJobs ): 

    if not self.__db.createNode( nodename, queuename, jobs, maxJobs ):
      return (StatusCode.FATAL, 'Failed to create the node into the database')
    
    return (StatusCode.SUCCESS, "Successfully created." )


  def delete( self, username ):

    MSG_WARNING(self, "Not implemented yet." )




